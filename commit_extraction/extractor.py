# import pandas as pd
from git import Repo
from datetime import datetime, timezone
import daft
import tempfile


@daft.udf(return_dtype=daft.DataType.struct(dict(
    repo_name=daft.DataType.string(),
    repo_owner=daft.DataType.string(),
    commit_hash=daft.DataType.string(),
    author_name=daft.DataType.string(),
    author_email=daft.DataType.string(), 
    committed_date=daft.DataType.timestamp(daft.TimeUnit.from_str("ms")),
    message=daft.DataType.string(),
    changed_files=daft.DataType.list(daft.DataType.string())
)))
def extract_commits_to_dataframe(remote_url):
    """
    Extract commit information from a local Git repository and return it as a pandas DataFrame.
    
    :param repo_path: Local path to the repository (string).
    :return: pandas DataFrame containing the commits.
    """

    commits_list = []
    
    # Create a temporary directory that will be automatically cleaned up
    for url in remote_url.to_pylist():
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Repo.clone_from(url, to_path=temp_dir)
            remote_url = repo.remotes.origin.url
            # Extract owner and repo name from remote URL
            # Handle both HTTPS and SSH URLs
            if remote_url.startswith('https://'):
                parts = remote_url.split('/')
                owner = parts[-2]
                repo_name = parts[-1].replace('.git', '')
            else:  # SSH format
                parts = remote_url.split(':')[1].split('/')
                owner = parts[0]
                repo_name = parts[1].replace('.git', '')


            for commit,i in zip(repo.iter_commits(), range(10)):
                # Convert committed_date (Unix timestamp) to datetime
                commit_info = {
                    'repo_name': repo_name,
                    'repo_owner': owner,
                    'commit_hash': commit.hexsha,
                    'author_name': commit.author.name,
                    'author_email': commit.author.email,
                    'committed_date': datetime.fromtimestamp(commit.committed_date, tz=timezone.utc),
                    'message': commit.message.strip(),
                    'changed_files': list(commit.stats.files.keys())
                }
                commits_list.append(commit_info)

    return commits_list

if __name__ == "__main__":
    df = daft.from_pydict(dict(remote_url=["https://github.com/Eventual-Inc/Daft.git"]))
    df = df.select(extract_commits_to_dataframe(df["remote_url"]).alias("commit"))
    df = df.select(daft.col("commit").struct.get('*'))
    df.show()