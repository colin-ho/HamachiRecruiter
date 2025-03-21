import argparse
from dotenv import load_dotenv
import os
import daft
from github import Auth, Github

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
if not GITHUB_TOKEN:
    raise ValueError(
        "GitHub token not found. Please set GITHUB_TOKEN in your .env file."
    )

auth = Auth.Token(GITHUB_TOKEN)
github = Github(auth=auth, per_page=100)


@daft.udf(
    return_dtype=daft.DataType.struct(
        {
            "name": daft.DataType.string(),
            "owner": daft.DataType.string(),
            "url": daft.DataType.string(),
            "description": daft.DataType.string(),
            "stars": daft.DataType.int64(),
            "updated": daft.DataType.string(),
            "created_at": daft.DataType.string(),
        }
    ),
    batch_size=1,
)
def get_repo_data(
    query: daft.Series,
    limit: int | None = None,
):
    [query] = query.to_pylist()
    print(f"Searching for repos: {query}, limit: {limit}")

    repos = github.search_repositories(query=query, sort="stars", order="desc")
    total_count = repos.totalCount
    print(f"Found {total_count} repos for query: {query}")

    if total_count == 0:
        return [None]

    res = []
    for repo in repos[: min(limit or total_count, total_count)]:
        print("Getting repo data for", repo.name)
        res.append(
            {
                "name": repo.name,
                "owner": repo.owner.login,
                "url": repo.clone_url,
                "description": repo.description,
                "stars": repo.stargazers_count,
                "updated": str(repo.updated_at),
                "created_at": str(repo.created_at),
            }
        )
    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", type=str, default="native")
    parser.add_argument("--keywords", type=str, default="language:Rust")
    parser.add_argument("--limit", type=str, default="None")
    parser.add_argument("--write-to-file", action="store_true")
    parser.add_argument("--output-path", type=str, default="repo_data_files")
    args = parser.parse_args()

    if args.runner == "native":
        daft.context.set_runner_native()
    elif args.runner == "ray":
        daft.context.set_runner_ray()
    else:
        raise ValueError(f"Invalid runner: {args.runner}")

    # Parse args and create queries
    if args.limit == "None":
        limit = None
    else:
        limit = int(args.limit)

    queries = args.keywords.split(",")

    print(
        f"Finding repos for: {queries}, limit: {limit}, runner: {args.runner}, write-to-file: {args.write_to_file}"
    )
    df = daft.from_pydict({"query": queries})

    # Get repo data
    get_repo_data = get_repo_data.with_concurrency(1)
    repo_data = df.with_column(
        "repo_data", get_repo_data(df["query"], limit)
    )
    repo_data = repo_data.select(repo_data["repo_data"].struct.get("*"))

    # write to file
    if args.write_to_file:
        path = args.output_path
        files = repo_data.write_parquet(path)
        print(f"Wrote files to {path}")
        print(files)
    else:
        repo_data.show()
