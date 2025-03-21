import argparse
import daft

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str, default="raw_commits")
    parser.add_argument("--runner", type=str, default="native")
    parser.add_argument("--write-to-file", action="store_true")
    parser.add_argument("--output-path", type=str, default="raw_contributors")
    args = parser.parse_args()

    if args.runner == "native":
        daft.context.set_runner_native()
    elif args.runner == "ray":
        daft.context.set_runner_ray()
    else:
        raise ValueError(f"Invalid runner: {args.runner}")

    print(f"Reading commits from {args.input_path}, runner: {args.runner}, writing to {args.write_to_file}")

    df = daft.read_parquet(args.input_path)

    df = df.groupby("repo_owner", "repo_name", "author_email").agg(
        [
            daft.col("author_name").any_value(),
            daft.col("date").count().alias("commit_count"),
            daft.col("lines_added").sum(),
            daft.col("lines_deleted").sum(),
            daft.col("lines_modified").sum(),
            daft.col("files_changed").agg_concat(),
            daft.col("message").agg_concat(),
            daft.col("date").min().alias("first_commit"),
            daft.col("date").max().alias("last_commit"),
        ]
    )

    if args.write_to_file:
        path = args.output_path
        files = df.write_parquet(path)
        print(f"Wrote files to {path}")
        print(files)
    else:
        df.show()
