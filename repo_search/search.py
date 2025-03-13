import argparse
from typing import Literal
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
    sort: Literal["stars", "forks", "updated"],
    order: Literal["asc", "desc"],
    limit: int | None = None,
):
    [query] = query.to_pylist()
    print(f"Searching for repos: {query}, sort: {sort}, order: {order}, limit: {limit}")
    repos = github.search_repositories(query=query, sort=sort, order=order)
    total_count = repos.totalCount
    print(f"Found {total_count} repos for query: {query}")
    if total_count == 0:
        return [None]

    res = []
    for repo in repos[: min(limit or total_count, total_count)]:
        print("Getting repo data for", repo.name)
        res.append(
            {
                # Existing fields
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
    parser.add_argument("--keywords", type=str, default="language:Java")
    parser.add_argument("--sort", type=str, default="stars")
    parser.add_argument("--order", type=str, default="desc")
    parser.add_argument("--limit", type=str, default="None")
    parser.add_argument("--write-to-file", type=bool, default=False)
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
        f"Running queries: {queries}, limit: {limit}, sort: {args.sort}, order: {args.order}, runner: {args.runner}, write-to-file: {args.write_to_file}"
    )
    df = daft.from_pydict({"query": queries})

    # Get repo data
    get_repo_data = get_repo_data.with_concurrency(1)
    repo_data = df.with_column(
        "repo_data", get_repo_data(df["query"], args.sort, args.order, limit)
    )
    repo_data = repo_data.select(repo_data["repo_data"].struct.get("*"))

    # deduplicate
    repo_data = repo_data.groupby("url").any_value()

    if args.write_to_file:
        path = f"repo_data_files_{args.keywords}_{args.sort}_{args.order}_{args.limit}".replace(
            " ", "_"
        )
        files = repo_data.write_parquet(path)
        print(f"Wrote files to {path}")
        print(files)
    else:
        repo_data.show()
