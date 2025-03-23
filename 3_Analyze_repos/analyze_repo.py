import argparse
import instructor
from pydantic import BaseModel
import daft
import os
from dotenv import load_dotenv
import pypandoc
import re
from openai import AsyncOpenAI
from fireworks.client import AsyncFireworks
import asyncio

load_dotenv()

def load_fireworks_client_and_model(timeout=60):
    FIREWORKS_API_KEY = os.environ.get("FIREWORKS_API_KEY")
    if not FIREWORKS_API_KEY:
        raise ValueError("FIREWORKS_API_KEY is not set")
    
    FIREWORKS_MODEL = os.environ.get("FIREWORKS_MODEL")
    if not FIREWORKS_MODEL:
        raise ValueError("FIREWORKS_MODEL is not set")
    
    return instructor.from_fireworks(AsyncFireworks(api_key=FIREWORKS_API_KEY, timeout=timeout)), FIREWORKS_MODEL

def load_openai_client_and_model(timeout=60):
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY is not set")
    
    OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    if not OPENAI_MODEL:
        raise ValueError("OPENAI_MODEL is not set")
    
    return instructor.from_openai(AsyncOpenAI(api_key=OPENAI_API_KEY, timeout=timeout)), OPENAI_MODEL

def guess_format(text):
    if text is None:
        return "unknown"
    md_score = 0
    rst_score = 0
    
    # Very rough heuristic checks:
    if re.search(r'\[[^\]]+\]\(http', text):
        md_score += 1
    if re.search(r'^\s*#{1,6}\s+\S', text, flags=re.MULTILINE):
        md_score += 1
    if re.search(r'^\.\.\s+\w+::', text, flags=re.MULTILINE):
        rst_score += 1
    if re.search(r'^[=\-`:\.' "'^~*+#]+(\r?\n)+", text, flags=re.MULTILINE):
        rst_score += 1
    
    if md_score > rst_score:
        return "markdown"
    elif rst_score > md_score:
        return "rst"
    else:
        return "unknown"

class ProjectAnalysis(BaseModel):
    languages: list[str]
    keywords: list[str]

@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            languages=daft.DataType.list(daft.DataType.string()),
            keywords=daft.DataType.list(daft.DataType.string()),
        )
    ),
)
def analyze_repo_readme_and_description(repo_name, readme, description, max_concurrent_requests=64, max_tokens=256, provider="OpenAI"):
    if provider == "OpenAI":
        client, model = load_openai_client_and_model()
    elif provider == "Fireworks":
        client, model = load_fireworks_client_and_model()
    else:
        raise ValueError(f"Invalid provider: {provider}")

    async def analyze_single_readme_and_description(client, repo_name, readme, description):
        print(f"Analyzing {repo_name}")
        try:
            format = guess_format(readme)
            if format == "markdown":
                readme = pypandoc.convert_text(readme, to="plain", format="markdown")
            elif format == "rst":
                readme = pypandoc.convert_text(readme, to="plain", format="rst")
        except Exception as e:
            readme = readme

        prompt = f"""You are an expert at analyzing GitHub repositories. Your task is to analyze this GitHub repository to:
        1. Determine the programming languages used in the repository.
        2. Generate a list of relevant keywords that describe the repository. 
        The keywords should contain the most important concepts and ideas in the repository, 
        such as the main problem it solves, the main features, the tools and frameworks used, etc.

        Repository details:
        Name: {repo_name}
        Description: {description}
        README: {readme}

        Based on the GitHub repository details above, provide:
        1. A list of the top 2 programming languages used in this repository. Make sure to not produce more than 2 languages.
        2. A list of the top 10 relevant keywords that describe the repository. Make sure to not produce more than 10 keywords. If the keywords are compound words, use a hyphen to join them.
        """

        from tenacity import retry, stop_after_attempt, wait_exponential

        @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60), reraise=True)
        async def fetch_completion(client, model, prompt, max_tokens):
            return await client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                response_model=ProjectAnalysis,
            )

        try:
            result = await fetch_completion(client, model, prompt, max_tokens)
            print(f"Analyzed {repo_name} with {model}")
            result_dict = result.model_dump()
            result_dict['languages'] = sorted(result_dict['languages'])
            result_dict['keywords'] = sorted([keyword.replace(" ", "-") for keyword in result_dict['keywords']])
            return result_dict
        except Exception as e:
            print(f"Error analyzing {repo_name}: {e}")
            return None

    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def analyze_with_semaphore(*args):
        async with semaphore:
            return await analyze_single_readme_and_description(*args)

    tasks = [
        analyze_with_semaphore(client, repo_name, readme, description)
        for repo_name, readme, description in zip(
            repo_name,
            readme,
            description,
        )
    ]

    async def run_tasks():
        return await asyncio.gather(*tasks)

    results = asyncio.run(run_tasks())
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str, default="repos_with_readme")
    parser.add_argument("--runner", type=str, default="native")
    parser.add_argument("--write-to-file", action="store_true")
    parser.add_argument("--output-path", type=str, default="analyzed_repos")
    args = parser.parse_args()

    print(f"Analyzing repos from {args.input_path}, runner: {args.runner}, write-to-file: {args.write_to_file}")

    repo_data = daft.read_parquet(args.input_path)

    # Analyze readme and description
    readme_and_description_analyzer = (
        analyze_repo_readme_and_description.with_concurrency(1)
    )
    repo_data_with_keywords = repo_data.with_column(
        "project_analysis",
        readme_and_description_analyzer(
            repo_data["name"], repo_data["readme"], repo_data["description"]
        ),
    )
    repo_data_with_keywords = repo_data_with_keywords.with_columns(
        {
            "languages": daft.col("project_analysis").struct.get("languages"),
            "keywords": daft.col("project_analysis").struct.get("keywords"),
        }
    )
    repo_data_with_keywords = repo_data_with_keywords.exclude(
        "project_analysis"
    )

    if args.write_to_file:
        files = repo_data_with_keywords.write_parquet(
            args.output_path,
            write_mode="append",
        )
        print(f"Wrote files to {args.output_path}")
        print(files)
    else:
        repo_data_with_keywords.show()
