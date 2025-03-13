import daft

import instructor
from pydantic import BaseModel, Field

from anthropic import AnthropicBedrock
from fireworks.client import Fireworks, AsyncFireworks
import instructor
from pydantic import BaseModel
import os

if __name__ == "__main__":
    # df = daft.read_parquet("data/commit_data.parquet")
    df = daft.read_parquet("s3://eventual-data-test-bucket/HamachiRecruiterData/raw_commits2/")
    df = df.groupby('repo_owner', 'repo_name', 'author_email').agg([
        daft.col('author_name').any_value(),
        daft.col('date').count().alias('commit_count'),
        daft.col('lines_added').sum(),
        daft.col('lines_deleted').sum(),
        daft.col('lines_modified').sum(),
        daft.col('files_changed').agg_concat(),
        daft.col('message').agg_concat(),
        daft.col('date').min().alias('first_commit'),
        daft.col('date').max().alias('last_commit')
    ])
    # we only care about folks who have contributed at least 100 lines of code and 3 commits
    # df = df.where("lines_modified > 100 AND commit_count >= 3")
    # df = df.limit(100)

    # df = df.with_column('commit_analysis', analyze_commit_message(df['repo_name'], df['commit_count'], df['lines_added'], df['lines_deleted'], df['lines_modified'], df['files_changed'], df['message']))
    # df = df.with_columns({
    #     "impact_to_project":  df['commit_analysis'].struct.get('impact_to_project'),
    #     "technical_ability":  df['commit_analysis'].struct.get('technical_ability'),
    #     "reason":  df['commit_analysis'].struct.get('reason'),
    # })
    # df = df.exclude('commit_analysis')
    df.write_parquet("s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_raw/")
