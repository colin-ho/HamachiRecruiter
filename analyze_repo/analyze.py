import argparse
import instructor
from pydantic import BaseModel
from openai import OpenAI
import daft
import os
from dotenv import load_dotenv
from typing import Literal

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
if not GITHUB_TOKEN:
    raise ValueError(
        "GitHub token not found. Please set GITHUB_TOKEN in your .env file."
    )

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
if not OPENAI_API_KEY:
    raise ValueError(
        "OpenAI API key not found. Please set OPENAI_API_KEY in your .env file."
    )


class ProjectType(BaseModel):
    project_type: Literal[
        "data_processing",
        "web_framework",
        "developer_tools",
        "system_software",
        "scientific_computing",
        "security",
        "game_development",
        "mobile_development",
        "machine_learning",
        "networking",
    ]
    reason: str


@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            project_type=daft.DataType.string(),
            reason=daft.DataType.string(),
        )
    ),
    batch_size=1,
)
def analyze_repo_readme_and_description(repo_name, readme, description):
    client = instructor.from_openai(OpenAI(api_key=OPENAI_API_KEY))
    [repo_name] = repo_name.to_pylist()
    print(f"Analyzing repo: {repo_name}")
    [readme] = readme.to_pylist()
    [description] = description.to_pylist()

    prompt = f"""You are an expert at categorizing software repositories. Your task is to analyze this repository and categorize it into exactly one of these categories:

    - data_processing: Tools and libraries for ETL, data manipulation, analytics
    - web_framework: Frameworks and libraries for web development
    - developer_tools: IDEs, linters, build tools, package managers
    - system_software: Operating systems, drivers, system utilities
    - scientific_computing: Scientific simulations, numerical computing, research tools
    - security: Cryptography, authentication, security testing
    - game_development: Game engines, game frameworks, actual games
    - mobile_development: Mobile app development tools and frameworks
    - machine_learning: ML libraries, AI tools, deep learning
    - networking: Network protocols, APIs, communication tools

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
    """

    result = client.chat.completions.create(
        model="gpt-4o-mini",
        response_model=ProjectType,
        messages=[{"role": "user", "content": prompt}],
    )
    return [result.model_dump()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, default="repo_data_files")
    parser.add_argument("--runner", type=str, default="native")
    parser.add_argument("--write-to-file", type=bool, default=False)
    args = parser.parse_args()

    if args.runner == "native":
        daft.context.set_runner_native()
    elif args.runner == "ray":
        daft.context.set_runner_ray()
    else:
        raise ValueError(f"Invalid runner: {args.runner}")

    repo_data = daft.read_parquet(args.path)

    # Analyze readme and description
    readme_and_description_analyzer = (
        analyze_repo_readme_and_description.with_concurrency(4)
    )
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

    if args.write_to_file:
        files = repo_data_with_project_type.write_parquet(
            "repo_data_files_with_project_type"
        )
        print(f"Wrote files to repo_data_files_with_project_type")
        print(files)
    else:
        repo_data_with_project_type.show()
