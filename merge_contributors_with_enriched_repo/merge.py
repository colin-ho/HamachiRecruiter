import daft
from daft import col

def main():
    # Read the deduplicated contributor data
    contributors = daft.read_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_data_enriched_dedupped"
    )

    # Read the repo data
    repos = daft.read_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/repos_with_project_type_and_owner_and_languages_and_readme"
    ).exclude('readme', 'reason').with_columns(dict(
        languages=col('languages').list.join(delimiter='|').str.normalize(remove_punct=False, lowercase=True, white_space=True).str.split('|'),
    ))
    # Join contributors with repos on repo_owner and repo_name
    merged = contributors.join(
        repos,
        left_on=["repo_owner", "repo_name"],
        right_on=["owner", "name"],
        how="inner"
    ).exclude('owner', 'name')
    
    merged = merged

    # find lanagues that a contributor has worked with
    languages = merged.groupby('author_email').agg(
        col('languages').agg_concat(),
        col('project_type').agg_set()
    )
    unique_languages_and_project_types = languages.with_column('languages', col('languages').list.distinct())
    unique_languages_and_project_types.show()


    contributors_with_languages_and_project_types = contributors.join(unique_languages_and_project_types, on='author_email', how='left').with_column(
        'repo', col('repo_owner') + '/' + col('repo_name')
    ).exclude('repo_owner', 'repo_name')
    contributors_with_languages_and_project_types.show()


    contributors_with_languages_and_project_types = contributors_with_languages_and_project_types.with_columns(dict(
        languages=col('languages').list.join(delimiter='|'),
        project_type=col('project_type').list.join(delimiter='|')
    ))
    print(contributors_with_languages_and_project_types.schema())

    contributors_with_languages_and_project_types.write_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/contributors_with_languages_and_project_types",
        write_mode="overwrite"
    )

if __name__ == "__main__":
    main()
