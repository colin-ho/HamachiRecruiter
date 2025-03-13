import instructor
from pydantic import BaseModel
from fireworks.client import AsyncFireworks
import daft
import os
from dotenv import load_dotenv
from typing import Literal

load_dotenv()

FIREWORKS_API_KEY = os.environ.get("FIREWORKS_API_KEY", "")
if not FIREWORKS_API_KEY:
    raise ValueError(
        "Fireworks API key not found. Please set FIREWORKS_API_KEY in your .env file."
    )


class ProjectType(BaseModel):
    project_type: Literal[
        "web_development",
        "data_processing",
        "dev_ops",
        "mobile_development",
        "machine_learning",
        "crypto",
        "artificial_intelligence",
        "game_development",
        "cloud_computing",
        "security",
        "developer_tools",
    ]
    reason: str


@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            project_type=daft.DataType.string(),
            reason=daft.DataType.string(),
        )
    ),
)
def analyze_repo_readme_and_description(repo_name, readme, description):
    client = AsyncFireworks(
        api_key=FIREWORKS_API_KEY,
        timeout=60
    )
    client = instructor.from_fireworks(client)

    async def analyze_single_readme_and_description(
        client, repo_name, readme, description
    ):
        print(f"Analyzing repo: {repo_name}")
        prompt = f"""You are an expert at categorizing software repositories. Your task is to analyze this repository and categorize it into exactly one of these categories:

        - web_development
        - data_processing
        - dev_ops
        - mobile_development
        - machine_learning
        - crypto
        - artificial_intelligence
        - game_development
        - cloud_computing
        - security
        - developer_tools

        Repository details:
        Name: {repo_name}
        Description: {description}
        README: {readme}

        Based on the repository details above, determine the single most appropriate category. Consider:
        1. The main purpose and core functionality
        2. Primary use cases and target users
        3. Key features and dependencies
        4. Common usage patterns

        Provide your category choice and a detailed explanation for why this category best fits the repository.

        Keep your reason explanation brief - maximum 2 sentences.
        """

        try:
            # result = await client.chat.completions.create(
            #     model="gpt-4o-mini",
            #     response_model=ProjectType,
            #     messages=[{"role": "user", "content": prompt}],
            #     max_tokens=128,
            #     max_retries=3,
            # )
            result = await client.chat.completions.create(
                model="accounts/fireworks/models/llama-v3p2-3b-instruct#accounts/sammy-b656e2/deployments/61bd1cb6",
                response_model=ProjectType,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                max_tokens=128,
                max_retries=3,
            )

            print(
                f"Analyzed repo: {repo_name}, project type: {result.project_type}"
            )
            return result.model_dump()
        except Exception as e:
            print(f"Got error when validating input from model {e}")
            return None

    import asyncio

    semaphore = asyncio.Semaphore(256)

    async def analyze_with_semaphore(*args):
        async with semaphore:
            return await analyze_single_readme_and_description(*args)

    tasks = [
        analyze_with_semaphore(client, repo_name, readme, description)
        for repo_name, readme, description in zip(
            repo_name.to_pylist(),
            readme.to_pylist(),
            description.to_pylist(),
        )
    ]

    # Run coroutines concurrently and gather results
    import asyncio

    results = []

    async def run_tasks():
        return await asyncio.gather(*tasks)

    results = asyncio.run(run_tasks())
    return results


if __name__ == "__main__":

    daft.set_execution_config(default_morsel_size=512)
    repo_data = daft.read_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/repos_with_readme"
    )

    # Analyze readme and description
    readme_and_description_analyzer = analyze_repo_readme_and_description.with_concurrency(1)
    repo_data_with_project_type = repo_data.with_column(
        "project_type",
        readme_and_description_analyzer(
            repo_data["name"], repo_data["readme"], repo_data["description"]
        ),
    )
    repo_data_with_project_type = repo_data_with_project_type.with_columns(
        {
            "project_type": daft.col("project_type").struct.get("project_type"),
            "reason": daft.col("project_type").struct.get("reason"),
        }
    )

    files = repo_data_with_project_type.write_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/repos_with_project_type",
        write_mode="append",
    )
    print(f"Wrote files to repo_data_files_with_project_type")
    print(files)
