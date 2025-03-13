from git import Repo
from datetime import datetime
import daft
import tempfile


def parse_logs(logs):
    commits = []
    current_commit = None
    files = []
    iterator = iter(logs.split("\n"))
    is_active = True
    at_commit_end = False
    while is_active:
        try:
            line = next(iterator)
        except StopIteration:
            is_active = False
            continue
        if line == "---COMMIT START---":
            at_commit_end = False
            current_commit = {}
            files = []
            continue
        elif line == "---COMMIT END---":
            at_commit_end = True
            continue

        if current_commit is not None:
            # First line after START is commit hash
            if "hash" not in current_commit:
                current_commit["hash"] = line.strip()
            # Second line is author name
            elif "author_name" not in current_commit:
                current_commit["author_name"] = line.strip()
            # Third line is author email
            elif "author_email" not in current_commit:
                current_commit["author_email"] = line.strip()
            # Fourth line is date
            elif "date" not in current_commit:
                dt = datetime.strptime(line.strip(), "%Y-%m-%d %H:%M:%S %z")
                current_commit["date"] = dt
            # Message comes next until we hit file stats
            elif "message" not in current_commit:
                message = [line]
                while True:
                    try:
                        line = next(iterator)
                    except StopIteration:
                        is_active = False
                        break
                    if line == "---COMMIT END---":
                        at_commit_end = True
                        break
                    message.append(line)
                current_commit["message"] = "".join(
                    message[0] + "\n" + "\n".join(message[1:])
                    if len(message) > 1
                    else message[0]
                )

        if at_commit_end:
            # Calculate file stats which is after commit
            lines_added = 0
            lines_deleted = 0
            lines_modified = 0
            files_changed = []
            while True:
                try:
                    line = next(iterator)
                except StopIteration:
                    is_active = False
                    break
                if line == "":
                    break
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    files.append(parts)

            for file_stats in files:
                if len(file_stats) >= 3:
                    try:
                        added = int(file_stats[0]) if file_stats[0] != "-" else 0
                        deleted = int(file_stats[1]) if file_stats[1] != "-" else 0
                        lines_added += added
                        lines_deleted += deleted
                        lines_modified += added + deleted
                    except ValueError:
                        continue
                    files_changed.append(file_stats[2])
            current_commit["lines_added"] = lines_added
            current_commit["lines_deleted"] = lines_deleted
            current_commit["lines_modified"] = lines_modified
            current_commit["files_changed"] = files_changed

            # Replace with replacement character (�) if it is a string value
            current_commit = {
                k: (
                    v.encode("utf-8", "replace").decode("utf-8")
                    if isinstance(v, str)
                    else v
                )
                for k, v in current_commit.items()
            }
            commits.append(current_commit)
            at_commit_end = False
            continue

    return commits


@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            repo_name=daft.DataType.string(),
            url=daft.DataType.string(),
            repo_owner=daft.DataType.string(),
            hash=daft.DataType.string(),
            author_name=daft.DataType.string(),
            author_email=daft.DataType.string(),
            date=daft.DataType.timestamp(daft.TimeUnit.from_str("ms")),
            message=daft.DataType.string(),
            files_changed=daft.DataType.list(daft.DataType.string()),
            lines_added=daft.DataType.uint64(),
            lines_deleted=daft.DataType.uint64(),
            lines_modified=daft.DataType.uint64(),
        )
    ),
)
def extract_commits_to_dataframe(remote_url):
    """
    Extract commit information from a local Git repository and return it as a pandas DataFrame.

    :param repo_path: Local path to the repository (string).
    :return: struct of commit metadata
    """

    commits_list = []
    remote_urls = remote_url.to_pylist()
    print(f"Extracting commits for {len(remote_urls)} repos")
    # Create a temporary directory that will be automatically cleaned up
    for url in remote_urls:
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                print(f"Cloning repository {url}")
                repo = Repo.clone_from(
                    url, to_path=temp_dir, multi_options=["--no-checkout"]
                )
                remote_url = repo.remotes.origin.url
                print(f"Cloned repository {url}")
            except Exception as e:
                print(f"Error cloning repository {url}: {e}")
                continue

            try:
                print(f"Extracting owner and repo name from {remote_url}")
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
                print(f"Extracted owner and repo name from {remote_url}")
            except Exception as e:
                print(f"Error extracting owner and repo name from {remote_url}: {e}")
                continue

            try:
                print(f"Parsing logs for {url}")
                logs = repo.git.log(
                    "--pretty=format:---COMMIT START---%n%H%n%an%n%ae%n%ai%n%B%n---COMMIT END---",
                    "--date=iso",
                    "--numstat",
                )

                parsed_logs = parse_logs(logs)
                for log in parsed_logs:
                    log["repo_name"] = repo_name
                    log["repo_owner"] = owner
                    log["url"] = url
                commits_list.extend(parsed_logs)
                print(f"Parsed logs for {url}")
            except Exception as e:
                print(f"Error parsing logs for {url}: {e}")
                continue

    return commits_list


if __name__ == "__main__":
    daft.context.set_runner_ray()
    df = (
        daft.read_parquet(
            "s3://eventual-data-test-bucket/HamachiRecruiterData/raw_repos"
        )
        .where(
            ~daft.col("name").is_in(
                ["chromium", "cdnjs", "leetcode", "aws-sdk-java", "languagetool"]
            )
        )
        .into_partitions(1024)
    )

    extractor = extract_commits_to_dataframe

    df = df.select(extractor(df["url"]).alias("commit"))
    df = df.select(daft.col("commit").struct.get("*"))
    files = df.write_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/raw_commits"
    )
    print(f"Wrote files to commit_data_files")
    print(files)
