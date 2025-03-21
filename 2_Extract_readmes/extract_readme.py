import argparse
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
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            Repo.clone_from(url, to_path=temp_dir, multi_options=["--sparse"])
        except Exception as e:
            return None

        # if the directory is empty, skip
        try:
            if not os.listdir(temp_dir):
                return None
        except Exception as e:
            return None

        # try to find a README file
        try:
            for pattern in readme_patterns:
                readme_path = os.path.join(temp_dir, pattern)
                if os.path.exists(readme_path):
                    with open(readme_path, "r") as f:
                        s = f.read()
                        s = s.encode("utf-8", "replace").decode("utf-8")
                        return s
        except Exception as e:
            return None

    return None


@daft.udf(
    return_dtype=daft.DataType.string(),
)
def extract_readme_to_dataframe(remote_url):
    readme_list = []

    remote_urls = remote_url.to_pylist()
    print(f"Extracting readmes for {len(remote_urls)} repos")
    for url in remote_urls:
        readme = extract_readme(url)
        readme_list.append(readme)

    return readme_list

uncloneable_repos = [
    "chromium",
    "cdnjs",
    "leetcode",
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str, default="repo_data_files")
    parser.add_argument("--runner", type=str, default="native")
    parser.add_argument("--write-to-file", action="store_true")
    parser.add_argument("--output-path", type=str, default="repos_with_readme")
    args = parser.parse_args()

    if args.runner == "native":
        daft.context.set_runner_native()
    elif args.runner == "ray":
        daft.context.set_runner_ray()
    else:
        raise ValueError(f"Invalid runner: {args.runner}")

    print(f"Reading repos from {args.input_path}, runner: {args.runner}, write-to-file: {args.write_to_file}")

    df = daft.read_parquet(args.input_path).where(
        ~daft.col("name").is_in(uncloneable_repos)
    )

    extractor = extract_readme_to_dataframe
    df = df.with_column("readme", extractor(df["url"]))

    if args.write_to_file:
        path = args.output_path
        files = df.write_parquet(path)
        print(f"Wrote files to {path}")
        print(files)
    else:
        df.show()
