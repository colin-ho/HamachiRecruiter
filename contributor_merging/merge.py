import daft
from daft import col
def main():
    contributors = daft.read_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_data_enriched"
    ).exclude('message', 'files_changed')

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
    outfiles = name_dedupped_filtered.write_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_data_enriched_dedupped",
        write_mode="overwrite"
    )

    print(outfiles)


if __name__ == "__main__":
    main()