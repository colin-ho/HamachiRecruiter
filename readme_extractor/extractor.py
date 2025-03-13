from git import Repo
import daft
import tempfile
import os

readme_patterns = [
    # Most common patterns (80%+ of repositories)
    "README.md",
    "readme.md",
    "Readme.md",
    # Standard variants still very common
    "README.markdown",
    "README",
    "README.txt",
    # Common case variations
    "ReadMe.md",
    "readme",
    "readme.txt",
    # Common locations in subdirectories
    "docs/README.md",
    "doc/README.md",
    # Less common but still regularly used formats
    "README.rst",
    "readme.rst",
    "README.adoc",
    # Mixed case variants
    "Readme",
    "ReadMe",
    "Readme.txt",
    "ReadMe.txt",
    # Uppercase extensions
    "README.MD",
    "readme.MD",
    # Underscore variants
    "READ_ME.md",
    "read_me.md",
    "READ_ME",
    "read_me",
    "READ_ME.txt",
    "read_me.txt",
    # Documentation directory variants
    "documentation/README.md",
    "docs/readme.md",
    "doc/readme.md",
    "documentation/readme.md",
    "docs/index.md",
    "doc/index.md",
    "documentation/index.md",
    # Less common formats but still found
    "readme.markdown",
    "Readme.markdown",
    "README.html",
    "readme.html",
    "Readme.html",
    "ReadMe.html",
    # Language-specific formats
    "README.rdoc",  # Ruby documentation
    "readme.rdoc",
    "README.pod",  # Perl documentation
    "readme.pod",
    # Specialized formats
    "README.org",  # Org-mode
    "readme.org",
    "README.asciidoc",
    "readme.adoc",
    "Readme.adoc",
    "ReadMe.adoc",
    "README.tex",  # LaTeX
    "readme.tex",
    # Less common uppercase extensions
    "Readme.MD",
    "ReadMe.MD",
    "README.TXT",
    "readme.TXT",
    "Readme.TXT",
    "ReadMe.TXT",
    "README.RST",
    "readme.RST",
    "Readme.RST",
    "README.ADOC",
    "readme.ADOC",
    "Readme.ADOC",
    "README.HTML",
    "readme.HTML",
    "Readme.HTML",
]


def extract_readme(url):

    print(f"Extracting readme for {url}")
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            print(f"Cloning repository {url}")
            repo = Repo.clone_from(url, to_path=temp_dir, multi_options=["--sparse"])
            print(f"Cloned repository {url}")
        except Exception as e:
            print(f"Error cloning repository {url}: {e}")
            return None

        # if the directory is empty, skip
        try:
            if not os.listdir(temp_dir):
                print(f"Repository {url} is empty, skipping")
                return None
        except Exception as e:
            print(f"Error checking if repository {url} is empty: {e}")
            return None

        # try to find a README file
        try:
            found_readme = False
            for pattern in readme_patterns:
                readme_path = os.path.join(temp_dir, pattern)
                if os.path.exists(readme_path):
                    with open(readme_path, "r") as f:
                        print(f"Found README file {readme_path} for {url}")
                        s = f.read()
                        s = s.encode("utf-8", "replace").decode("utf-8")
                        return s
        except Exception as e:
            print(f"Error finding README file for {url}: {e}")
            return None

    # if we get here, we didn't find a README file
    print(f"No README found for {url}, returning None")
    return None


@daft.udf(
    return_dtype=daft.DataType.string(),
)
def extract_readme_to_dataframe(remote_url):
    """
    Extract README information from a local Git repository and return it as a pandas DataFrame.

    :param repo_path: Local path to the repository (string).
    :return: README information
    """
    readme_list = []

    remote_urls = remote_url.to_pylist()
    print(f"Extracting readmes for {len(remote_urls)} repos")
    for url in remote_urls:
        readme = extract_readme(url)
        readme_list.append(readme)

    return readme_list



if __name__ == "__main__":
    # daft.context.set_runner_ray()
    df = daft.read_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/raw_repos"
    ).where(~daft.col("name").is_in(["chromium", "cdnjs", "leetcode"]))

    extractor = extract_readme_to_dataframe
    df = df.with_column("readme", extractor(df["url"]))
    files = df.write_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/repos_with_readme",
        write_mode="append",
    )
    print(f"Wrote files to commit_data_files")
    print(files)