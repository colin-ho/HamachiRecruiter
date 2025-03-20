import argparse
import instructor
from pydantic import BaseModel
from fireworks.client import AsyncFireworks
import daft
import os
from dotenv import load_dotenv
from typing import Literal
import pypandoc
import re

load_dotenv()

FIREWORKS_API_KEY = os.environ.get("FIREWORKS_API_KEY", "")
if not FIREWORKS_API_KEY:
    raise ValueError(
        "Fireworks API key not found. Please set FIREWORKS_API_KEY in your .env file."
    )

MODEL = os.environ.get("MODEL", "")
if not MODEL:
    raise ValueError(
        "Model not found. Please set MODEL in your .env file."
    )

def guess_format(text):
    if text is None:
        return "unknown"
    md_score = 0
    rst_score = 0
    
    # Very rough heuristic checks:
    # Markdown hallmark: [link text](http://...)
    if re.search(r'\[[^\]]+\]\(http', text):
        md_score += 1
        
    # Another Markdown hallmark: # heading
    if re.search(r'^\s*#{1,6}\s+\S', text, flags=re.MULTILINE):
        md_score += 1
    
    # reST hallmark: .. directive::
    if re.search(r'^\.\.\s+\w+::', text, flags=re.MULTILINE):
        rst_score += 1
    
    # reST hallmark: section underline
    if re.search(r'^[=\-`:\.' "'^~*+#]+(\r?\n)+", text, flags=re.MULTILINE):
        rst_score += 1
    
    if md_score > rst_score:
        return "markdown"
    elif rst_score > md_score:
        return "rst"
    else:
        return "unknown"  # Or fallback

class ProjectType(BaseModel):
    languages: list[str]
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
            languages=daft.DataType.list(daft.DataType.string()),
            project_type=daft.DataType.string(),
            reason=daft.DataType.string(),
        )
    ),
)
def analyze_repo_readme_and_description(repo_name, readme, description, max_concurrent_requests=256, max_tokens=256, max_retries=3, timeout=60):
    client = AsyncFireworks(api_key=FIREWORKS_API_KEY, timeout=timeout)
    client = instructor.from_fireworks(client)

    async def analyze_single_readme_and_description(
        client, repo_name, readme, description
    ):
        try:
            format = guess_format(readme)
            if format == "markdown":
                readme = pypandoc.convert_text(readme, to="plain", format="markdown")
            elif format == "rst":
                readme = pypandoc.convert_text(readme, to="plain", format="rst")
        except Exception as e:
            readme = readme

        prompt = f"""You are an expert at analyzing and categorizing github repositories. Your task is to analyze this github repository to:
        1. Determine the programming languages used in the repository
        2. Categorize it into exactly one of these categories:

        - web_development: Projects focused on building websites, web applications, and web services using technologies like HTML, CSS, JavaScript, and web frameworks
        - data_processing: Projects that handle data transformation, analysis, ETL pipelines, and data manipulation at scale
        - dev_ops: Projects related to deployment, infrastructure automation, CI/CD, monitoring, and other operational tooling
        - mobile_development: Projects for building mobile applications for iOS, Android or cross-platform mobile development
        - machine_learning: Projects implementing machine learning algorithms, model training, and ML pipelines
        - crypto: Projects related to blockchain, cryptocurrencies, smart contracts and decentralized applications
        - artificial_intelligence: Projects using AI techniques like natural language processing, computer vision, and other AI applications
        - game_development: Projects focused on creating video games, game engines, or gaming-related tools
        - cloud_computing: Projects built for cloud platforms, cloud-native applications, and cloud infrastructure management
        - security: Projects focused on cybersecurity, penetration testing, vulnerability scanning and security tooling
        - developer_tools: Projects that create libraries, frameworks, IDEs and other tools to help developers write code

        Repository details:
        Name: {repo_name}
        Description: {description}
        README: {readme}

        Based on the github repository details above:
        1. List the top 2 programming languages that appear to be used in this repository based on the README, description, and any code examples shown
        2. Determine the single most appropriate category from the list above. Consider:
           - The main purpose and core functionality
           - Primary use cases and target users
           - Key features and dependencies
           - Common usage patterns

        Provide:
        1. A list of the top 2 programming languages. Maximum 2 languages.
        2. The category choice and a brief explanation (maximum 2 sentences) for why this category best fits the repository.
        """

        try:
            result = await client.chat.completions.create(
                model=MODEL,
                response_model=ProjectType,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                max_retries=max_retries,
            )

            return result.model_dump()
        except Exception as e:
            return None

    import asyncio

    semaphore = asyncio.Semaphore(max_concurrent_requests)

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

    results = asyncio.run(asyncio.gather(*tasks))
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str, default="repos_with_readme")
    parser.add_argument("--runner", type=str, default="native")
    parser.add_argument("--write-to-file", type=str, default="categorized_repos")
    args = parser.parse_args()

    print(f"Reading repos from {args.input_path}, runner: {args.runner}, write-to-file: {args.write_to_file}")

    daft.set_execution_config(default_morsel_size=512)
    repo_data = daft.read_parquet(args.input_path)

    # Analyze readme and description
    readme_and_description_analyzer = (
        analyze_repo_readme_and_description.with_concurrency(1)
    )
    repo_data_with_project_type = repo_data.with_column(
        "project_analysis",
        readme_and_description_analyzer(
            repo_data["name"], repo_data["readme"], repo_data["description"]
        ),
    )
    repo_data_with_project_type = repo_data_with_project_type.with_columns(
        {
            "project_type": daft.col("project_analysis").struct.get("project_type"),
            "reason": daft.col("project_analysis").struct.get("reason"),
            "languages": daft.col("project_analysis").struct.get("languages"),
        }
    )
    repo_data_with_project_type = repo_data_with_project_type.exclude(
        "project_analysis"
    )

    if args.write_to_file:
        files = repo_data_with_project_type.write_parquet(
            args.write_to_file,
            write_mode="append",
        )
        print(f"Wrote files to {args.write_to_file}")
        print(files)
    else:
        repo_data_with_project_type.show()
