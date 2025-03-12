import argparse
from typing import Literal
from dotenv import load_dotenv
import os
import daft
from github import Auth, Github

load_dotenv()

GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
if not GITHUB_TOKEN:
    raise ValueError("GitHub token not found. Please set GITHUB_TOKEN in your .env file.")

auth = Auth.Token(GITHUB_TOKEN)
github = Github(auth=auth)

@daft.udf(return_dtype=daft.DataType.struct({"name": daft.DataType.string(), "url": daft.DataType.string()}), batch_size=1)
def search_for_repos(query: daft.Series, sort: Literal["stars", "forks", "updated"], order: Literal["asc", "desc"], limit: int):
    [query] = query.to_pylist()
    repos = github.search_repositories(query=query, sort=sort, order=order)
    res = []
    for i, repo in enumerate(repos[:limit]):
        res.append({"name": repo.name, "url": repo.clone_url})
    return res

if __name__ == "__main__":
    # todo add arg for query, sort, order, limit
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, default="language:rust")
    parser.add_argument("--sort", type=str, default="stars")
    parser.add_argument("--order", type=str, default="desc")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--write_to_file", type=bool, default=False)
    args = parser.parse_args()

    df = daft.from_pydict({"query": [args.query]})

    repo_searcher = search_for_repos.with_concurrency(10)
    df = df.with_column(
        "repo_data",
        repo_searcher(df["query"], args.sort, args.order, args.limit)
    )

    repo_data = df.select(df["repo_data"].struct.get("*"))

    repo_data.show()

    if args.write_to_file:
        files = repo_data.write_parquet("repo_data_files")
        print(f"Wrote files to repo_data_files")
        print(files)
