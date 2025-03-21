import argparse
import daft
from daft import col

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-contributors-path", type=str, default="analyzed_contributors", help="Path to the analyzed contributors data")
    parser.add_argument("--input-repos-path", type=str, default="analyzed_repos", help="Path to the analyzed repos data")
    parser.add_argument("--runner", type=str, default="native", help="Runner to use (native or ray)")
    parser.add_argument("--write-to-file", action="store_true", help="Write to file")
    parser.add_argument("--output-path", type=str, default="contributors_and_repos", help="Path for the final output")
    args = parser.parse_args()

    if args.runner == "native":
        daft.context.set_runner_native()
    elif args.runner == "ray":
        daft.context.set_runner_ray()
    else:
        raise ValueError(f"Invalid runner: {args.runner}")
    
    print(f"Starting merge contributors and repos with runner: {args.runner}")
    print(f"Reading contributors from {args.input_contributors_path} and repos from {args.input_repos_path}")
    
    # Step 1: Process contributors data
    contributors = daft.read_parquet(args.input_contributors_path).exclude('message', 'files_changed')

    contributors = contributors.with_columns(dict(
        author_name=col('author_name').str.normalize(remove_punct=True, lowercase=True, white_space=True),
        reason=col('repo_owner') + '/' + col('repo_name') + ': ' + col('reason')
    ))
    
    # Deduplicate and aggregate contributor data by name
    contributors_dedupped = contributors.groupby('repo_owner', 'repo_name', col('author_name')).agg(
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

    # Filter out any contributors with more than 15 emails (typically bots)
    contributors_dedupped = contributors_dedupped.where('email_count <= 15')
    
    # Step 2: Process repos data
    repos = daft.read_parquet(args.input_repos_path).exclude('readme', 'reason').with_columns(dict(
        languages=col('languages').list.join(delimiter='|').str.normalize(remove_punct=False, lowercase=True, white_space=True).str.split('|'),
    ))
    
    # Join contributors with repos on repo_owner and repo_name
    merged = contributors_dedupped.join(
        repos,
        left_on=["repo_owner", "repo_name"],
        right_on=["owner", "name"],
        how="inner"
    ).exclude('owner', 'name')

    # Find languages that a contributor has worked with
    languages = merged.groupby('author_email').agg(
        col('languages').agg_concat(),
        col('project_type').agg_set()
    )
    unique_languages_and_project_types = languages.with_column('languages', col('languages').list.distinct())

    # Join back to contributors
    contributors_with_languages_and_project_types = contributors_dedupped.join(
        unique_languages_and_project_types, 
        on='author_email', 
        how='left'
    ).with_column(
        'repo', col('repo_owner') + '/' + col('repo_name')
    ).exclude('repo_owner', 'repo_name')

    # Convert lists to strings for output
    final_contributors = contributors_with_languages_and_project_types.with_columns(dict(
        languages=col('languages').list.join(delimiter='|'),
        project_type=col('project_type').list.join(delimiter='|')
    ))
    
    # Write the final output
    if args.write_to_file:
        outfiles = final_contributors.write_parquet(
            args.output_path,
            write_mode="overwrite"
        )
        print(f"Wrote files to {args.output_path}")
        print(outfiles)
    else:
        final_contributors.show() 