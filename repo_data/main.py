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
def search_for_repos(query, sort, order, limit):
    query, sort, order, limit = query.to_pylist()[0], sort.to_pylist()[0], order.to_pylist()[0], limit.to_pylist()[0]
    repos = github.search_repositories(query=query, sort=sort, order=order)
    res = []
    for repo in repos[:limit]:
        res.append({"name": repo.name, "url": repo.clone_url})
    return res

df = daft.from_pydict({"query": ["language:rust"], "sort": ["stars"], "order": ["desc"], "limit": [10]})

df = df.with_column(
    "repo_data",
    search_for_repos(df["query"], df["sort"], df["order"], df["limit"])
)

df.show()