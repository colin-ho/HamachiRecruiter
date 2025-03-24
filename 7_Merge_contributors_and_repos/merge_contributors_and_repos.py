import argparse
from collections import defaultdict
import daft
from daft import col

# map all emails to all emails they are related to
def generate_email_mapping(df: daft.DataFrame) -> dict[str, set[str]]:
    email_mapping = defaultdict(set)

    for row in df.iter_rows():
        emails = row['author_email'].split("|")
        for i in range(len(emails)):
            for j in range(len(emails)):
                email_mapping[emails[i]].add(emails[j])

    for k, v in email_mapping.items():
        v2 = set(v)
        for email in v:
            for email2 in email_mapping[email]:
                v2.add(email2)
        email_mapping[k] = v2
    return email_mapping

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

    email_mapping = generate_email_mapping(contributors_dedupped)

    @daft.udf(return_dtype=daft.DataType.list(daft.DataType.string()))
    def extract_email_mapping(emails_series) -> list[str]:
        res = []
        for emails in emails_series:
            emails = emails.split("|")
            all_emails = []
            for email in emails:
                all_emails.extend(email_mapping[email])
            res.append(all_emails)
        return res
    
    contributors_consolidated_emails = contributors_dedupped.with_column(
        'author_email', extract_email_mapping(daft.col('author_email')).list.distinct().list.sort()
    ).with_columns(dict(
        email_count=daft.col('author_email').list.len(),
        author_email=daft.col('author_email').list.join(delimiter='|'),
    ))

    # Filter out any contributors with more than 15 emails (typically bots)
    contributors_dedupped = contributors_consolidated_emails.where('email_count <= 15')
    
    # Step 2: Process repos data
    repos = daft.read_parquet(args.input_repos_path).exclude('readme', 'reason').with_columns(dict(
        languages=col('languages').list.join(delimiter='|').str.normalize(remove_punct=False, lowercase=True, white_space=True).str.split('|'),
        keywords=col('keywords').list.join(delimiter='|').str.normalize(remove_punct=False, lowercase=True, white_space=True).str.split('|'),
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
        col('keywords').agg_concat()
    )
    unique_languages_and_keywords = languages.with_columns(dict(
        languages=col('languages').list.distinct(),
        keywords=col('keywords').list.distinct()
    ))

    # Join back to contributors
    contributors_with_languages_and_keywords = contributors_dedupped.join(
        unique_languages_and_keywords, 
        on='author_email', 
        how='left'
    ).with_column(
        'repo', col('repo_owner') + '/' + col('repo_name')
    ).exclude('repo_owner', 'repo_name')

    # Convert lists to strings for output
    final_contributors = contributors_with_languages_and_keywords.with_columns(dict(
        languages=col('languages').list.join(delimiter='|'),
        keywords=col('keywords').list.join(delimiter='|')
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