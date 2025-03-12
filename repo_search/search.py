import argparse
from typing import Literal
from dotenv import load_dotenv
import os
import daft
from github import Auth, Github
import instructor
from pydantic import BaseModel
from openai import OpenAI

load_dotenv()

GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
if not GITHUB_TOKEN:
    raise ValueError("GitHub token not found. Please set GITHUB_TOKEN in your .env file.")

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
if not OPENAI_API_KEY:
    raise ValueError("OpenAI API key not found. Please set OPENAI_API_KEY in your .env file.")

auth = Auth.Token(GITHUB_TOKEN)
github = Github(auth=auth)

@daft.udf(
    return_dtype=daft.DataType.python(),
    batch_size=1
)
def search_for_repos(query: daft.Series, sort: Literal["stars", "forks", "updated"], order: Literal["asc", "desc"], limit: int | None = None):
    [query] = query.to_pylist()
    repos = github.search_repositories(query=query, sort=sort, order=order)
    total_count = repos.totalCount
    if total_count == 0:
        return [None]
    
    res = []
    for repo in repos[:min(limit or total_count, total_count)]:
        res.append(repo)
    return res

@daft.udf(
    return_dtype=daft.DataType.struct({
        "name": daft.DataType.string(),
        "url": daft.DataType.string(),
        "readme": daft.DataType.string(),
        "description": daft.DataType.string(),
        "stars": daft.DataType.int64(),
        "forks": daft.DataType.int64(),
        "updated": daft.DataType.string(),
        "created_at": daft.DataType.string(),
        "pushed_at": daft.DataType.string(),
        "open_issues_count": daft.DataType.int64(),
        "watchers_count": daft.DataType.int64(),
        "languages": daft.DataType.list(daft.DataType.string()),
        "topics": daft.DataType.list(daft.DataType.string()),
        "contributors_count": daft.DataType.int64(),
        "size": daft.DataType.int64(),
    }),
    batch_size=1
)
def get_repo_data(repo: daft.Series):
    [repo] = repo.to_pylist()
    try:
        readme = repo.get_readme()
        readme_content = readme.decoded_content.decode('utf-8')
    except Exception as e:
        readme_content = None

    try:
        languages = repo.get_languages()
        languages = list(languages.keys())
    except Exception as e:
        languages = None

    try:
        contributors_count = repo.get_contributors().totalCount
    except Exception as e:
        contributors_count = None

    try:
        topics = repo.get_topics()
    except Exception as e:
        topics = []

    return [{
        # Existing fields
        "name": repo.name,
        "url": repo.clone_url,
        "readme": readme_content,
        "description": repo.description,
        "stars": repo.stargazers_count,
        "forks": repo.forks_count,
        "updated": str(repo.updated_at),
        "languages": languages,
        "created_at": str(repo.created_at),
        "pushed_at": str(repo.pushed_at),
        "open_issues_count": repo.open_issues_count,
        "watchers_count": repo.subscribers_count,
        "topics": topics,
        "contributors_count": contributors_count,
        "size": repo.size,
    }]

class ProjectType(BaseModel):
    project_type: Literal["data_processing", "web_framework", "developer_tools", "system_software", "scientific_computing", "security", "game_development", "mobile_development", "machine_learning", "networking"] 
    reason: str


@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            project_type=daft.DataType.string(),
            reason=daft.DataType.string(),
        )
    ),
    batch_size=1
)
def analyze_repo_readme_and_description(repo_name, readme, description):
    client = instructor.from_openai(OpenAI(api_key=OPENAI_API_KEY))
    [repo_name] = repo_name.to_pylist()
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
        messages=[
        {"role": "user", "content": prompt}
    ]
    )
    return [result.model_dump()]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--keywords", type=str, default="data")
    parser.add_argument("--languages", type=str, default="rust")
    parser.add_argument("--sort", type=str, default="stars")
    parser.add_argument("--order", type=str, default="desc")
    parser.add_argument("--limit", type=str, default="10")
    parser.add_argument("--write_to_file", type=bool, default=False)
    args = parser.parse_args()

    # Parse args and create queries
    limit = int(args.limit) or None
    keyword_clause = " OR ".join(args.keywords.split(","))
    queries = [f"language:{language} {keyword_clause}" for language in args.languages.split(",")]

    print(f"Running queries: {queries}")
    df = daft.from_pydict({"query": queries})

    # Search for repos
    repo_searcher = search_for_repos.with_concurrency(4)
    repos = df.with_column(
        "repos",
        repo_searcher(df["query"], args.sort, args.order, limit)
    )

    # Get repo data
    repo_parser = get_repo_data.with_concurrency(4)
    repo_data = repos.with_column(
        "repo_data",
        repo_parser(repos["repos"])
    )
    repo_data = repo_data.select(repo_data["repo_data"].struct.get("*"))

    # Analyze readme and description
    readme_and_description_analyzer = analyze_repo_readme_and_description.with_concurrency(4)
    repo_data_with_project_type = repo_data.with_column(
        "project_type",
        readme_and_description_analyzer(repo_data["name"], repo_data["readme"], repo_data["description"])
    )
    repo_data_with_project_type = repo_data_with_project_type.with_columns({
        "project_type": daft.col("project_type").struct.get("project_type"),
        "reason": daft.col("project_type").struct.get("reason"),
    })

    if args.write_to_file:
        files = repo_data.write_parquet("repo_data_files")
        print(f"Wrote files to repo_data_files")
        print(files)
    else:
        repo_data_with_project_type.show(30)
