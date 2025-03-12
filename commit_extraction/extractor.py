# import pandas as pd
from git import Repo
from datetime import datetime, timezone
import daft
import tempfile

from pprint import pprint

def parse_logs(logs):
    commits = []
    current_commit = None
    files = []
    iterator = iter(logs.split('\n'))
    while True:
        line = next(iterator)
        if len(commits) > 10:
            break
        if line == '---COMMIT START---':
            current_commit = {}
            files = []
            continue
        elif line == '---COMMIT END---':
            # Calculate file stats which is after commit
            lines_added = 0
            lines_deleted = 0
            lines_modified = 0
            files_changed = []
            while True:
                line = next(iterator)
                if line == "":
                    break
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    files.append(parts)


                for file_stats in files:
                    if len(file_stats) >= 3:
                        try:
                            added = int(file_stats[0]) if file_stats[0] != '-' else 0
                            deleted = int(file_stats[1]) if file_stats[1] != '-' else 0
                            lines_added += added
                            lines_deleted += deleted
                            lines_modified += added + deleted
                        except ValueError:
                            continue
                        files_changed.append(file_stats[2])
            current_commit['lines_added'] = lines_added
            current_commit['lines_deleted'] = lines_deleted 
            current_commit['lines_modified'] = lines_modified
            current_commit['files_changed'] = files_changed
            
            commits.append(current_commit)
            continue
            
        if current_commit is not None:
            # First line after START is commit hash
            if 'hash' not in current_commit:
                current_commit['hash'] = line.strip()
            # Second line is author name
            elif 'author_name' not in current_commit:
                current_commit['author_name'] = line.strip()
            # Third line is author email
            elif 'author_email' not in current_commit:
                current_commit['author_email'] = line.strip()
            # Fourth line is date
            elif 'date' not in current_commit:
                current_commit['date'] = line.strip()
            # Message comes next until we hit file stats
            elif 'message' not in current_commit:
                if line.strip() and len(line.split('\t')) < 3:
                    current_commit['message'] = line.strip()
    pprint(commits)               
    return commits

    

@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            repo_name=daft.DataType.string(),
            repo_owner=daft.DataType.string(),
            hash=daft.DataType.string(),
            author_name=daft.DataType.string(),
            author_email=daft.DataType.string(),
            date=daft.DataType.timestamp(daft.TimeUnit.from_str("ms")),
            message=daft.DataType.string(),
            files_changed=daft.DataType.list(daft.DataType.string()),
            lines_added=daft.DataType.uint64(),
            lines_deleted=daft.DataType.uint64(),
            lines_modified=daft.DataType.uint64()
        )
    )
)
def extract_commits_to_dataframe(remote_url):
    """
    Extract commit information from a local Git repository and return it as a pandas DataFrame.

    :param repo_path: Local path to the repository (string).
    :return: struct of commit metadata
    """

    commits_list = []

    # Create a temporary directory that will be automatically cleaned up
    for url in remote_url.to_pylist():
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Repo.clone_from(url, to_path=temp_dir)
            remote_url = repo.remotes.origin.url
            # Extract owner and repo name from remote URL
            # Handle both HTTPS and SSH URLs
            if remote_url.startswith("https://"):
                parts = remote_url.split("/")
                owner = parts[-2]
                repo_name = parts[-1].replace(".git", "")
            else:  # SSH format
                parts = remote_url.split(":")[1].split("/")
                owner = parts[0]
                repo_name = parts[1].replace(".git", "")

            logs = repo.git.log('--pretty=format:---COMMIT START---%n%H%n%an%n%ae%n%ai%n%B%n---COMMIT END---', '--date=iso', '--numstat')

            parsed_logs = parse_logs(logs)
            for log in parsed_logs:
                log['repo_name'] = repo_name
                log['repo_owner'] = owner
            commits_list.extend(parsed_logs)
    return commits_list


if __name__ == "__main__":
    df = daft.from_pydict(dict(remote_url=["https://github.com/Eventual-Inc/Daft.git"]))
    df = df.select(extract_commits_to_dataframe(df["remote_url"]).alias("commit"))
    df = df.select(daft.col("commit").struct.get("*"))
    df.show()
