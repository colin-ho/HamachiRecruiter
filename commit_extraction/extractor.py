from git import Repo
from datetime import datetime
import daft
import tempfile

test_logs = """
---COMMIT START---
10ff6172ef20e2c478bcbdf4ee7d73aef53d85c7
i3water
121024123@qq.com
2023-05-10 20:12:24 +0800
Merge pull request #600 from blinker-iot/dev_3.0

update codes, fix ESP32 support codes.
---COMMIT END---
---COMMIT START---
efde12b5070fd7d2c01efa4c224eff29a2e82ecf
i3water
121024123@qq.com
2023-05-10 20:09:23 +0800
update codes, fix ESP32 support codes.

---COMMIT END---
1	1	library.json
1	1	library.properties
1	1	src/Blinker/BlinkerConfig.h
10	10	src/modules/mqtt/Adafruit_MQTT.cpp
"""


from datetime import datetime


def safe_encode(text):
    """
    Safely encode string values to handle potential encoding issues.
    Replaces invalid characters with the Unicode replacement character.

    Args:
        text: The string to safely encode

    Returns:
        str: The safely encoded string
    """
    if isinstance(text, str):
        return text.encode("utf-8", "replace").decode("utf-8")
    return text


def parse_logs(logs):
    commits = []
    current_commit = None

    lines = logs.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]
        # Start of a new commit
        if line == "---COMMIT START---":
            current_commit = {}
            i += 1
            continue

        # End of current commit
        elif line == "---COMMIT END---":
            # Look ahead for file stats before next commit
            file_stats = []
            j = i + 1

            while (
                j < len(lines)
                and lines[j] != "---COMMIT START---"
                and lines[j] != "---COMMIT END---"
            ):
                parts = lines[j].strip().split("\t")
                if len(parts) >= 3:
                    file_stats.append(parts)
                j += 1

            # Process file stats for current commit
            if current_commit:
                lines_added = 0
                lines_deleted = 0
                lines_modified = 0
                files_changed = []

                for file_stat in file_stats:
                    try:
                        added = int(file_stat[0]) if file_stat[0] != "-" else 0
                        deleted = int(file_stat[1]) if file_stat[1] != "-" else 0
                        lines_added += added
                        lines_deleted += deleted
                        lines_modified += added + deleted
                        files_changed.append(file_stat[2])
                    except ValueError:
                        continue

                current_commit["lines_added"] = lines_added
                current_commit["lines_deleted"] = lines_deleted
                current_commit["lines_modified"] = lines_modified
                current_commit["files_changed"] = files_changed

                safe_encoded_commit = {
                    k: safe_encode(v) for k, v in current_commit.items()
                }
                commits.append(safe_encoded_commit)

            # Skip processed file stats
            i = j
            continue

        # Parse commit details
        elif current_commit is not None:
            # Hash
            if "hash" not in current_commit:
                current_commit["hash"] = line.strip()
            # Author name
            elif "author_name" not in current_commit:
                current_commit["author_name"] = line.strip()
            # Author email
            elif "author_email" not in current_commit:
                current_commit["author_email"] = line.strip()
            # Date
            elif "date" not in current_commit:
                try:
                    dt = datetime.strptime(line.strip(), "%Y-%m-%d %H:%M:%S %z")
                    current_commit["date"] = dt
                except ValueError:
                    # Handle potential date format issues
                    current_commit["date"] = line.strip()
            # Commit message
            elif "message" not in current_commit:
                message_lines = [line]
                i += 1

                while i < len(lines) and lines[i] != "---COMMIT END---":
                    message_lines.append(lines[i])
                    i += 1

                # Join message lines, handling empty messages
                if len(message_lines) > 1:
                    message = message_lines[0] + "\n" + "\n".join(message_lines[1:])
                else:
                    message = message_lines[0]

                current_commit["message"] = message

                i -= 1  # Adjust index since we'll increment at the end of the loop

        i += 1

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
        "s3://eventual-data-test-bucket/HamachiRecruiterData/raw_commits4"
    )
    print(f"Wrote files to commit_data_files")
    print(files)
