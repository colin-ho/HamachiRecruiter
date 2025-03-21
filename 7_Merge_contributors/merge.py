import argparse
import daft
from daft import col

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str, default="analyzed_contributors")
    parser.add_argument("--runner", type=str, default="native")
    parser.add_argument("--write-to-file", type=str, default="contributors_merged")
    args = parser.parse_args()

    if args.runner == "native":
        daft.context.set_runner_native()
    elif args.runner == "ray":
        daft.context.set_runner_ray()
    else:
        raise ValueError(f"Invalid runner: {args.runner}")
    
    print(f"Reading contributors from {args.input_path}, runner: {args.runner}, writing to {args.write_to_file}")

    contributors = daft.read_parquet(args.input_path).exclude('message', 'files_changed')

    contributors = contributors.with_columns(dict(
        author_name=col('author_name').str.normalize(remove_punct=True, lowercase=True, white_space=True),
        reason=col('repo_owner') + '/' + col('repo_name') + ': ' + col('reason')
    ))
    
    name_dedupped = contributors.groupby('repo_owner', 'repo_name', col('author_name')).agg(
        col('author_email').agg_list(),
        col('author_email').count().alias('email_count'),
        col('commit_count').sum(),
        col('lines_added').sum(),
        col('lines_deleted').sum(),
        col('lines_modified').sum(),
        col('first_commit').min(),
        col('last_commit').max(),
        (col('impact_to_project') * col('commit_count')).sum().alias('impact_to_project_sum'),
        (col('technical_ability') * col('commit_count')).sum().alias('technical_ability_sum'),
        col('reason').agg_list()
    ).with_columns(dict(
        author_email=col('author_email').list.join(delimiter='|'),
        impact_to_project=col('impact_to_project_sum') / col('commit_count'),
        technical_ability=col('technical_ability_sum') / col('commit_count'),
        reason=col('reason').list.join(delimiter='\n'),
    )).exclude('impact_to_project_sum', 'technical_ability_sum')

    # we want to filter out any contributors with more than 16 emails. this is typically a bot.
    name_dedupped_filtered = name_dedupped.where('email_count <= 15')
    
    if args.write_to_file:
        outfiles = name_dedupped_filtered.write_parquet(
            args.write_to_file,
            write_mode="overwrite"
        )
        print(f"Wrote files to {args.write_to_file}")
        print(outfiles)
    else:
        name_dedupped_filtered.show()