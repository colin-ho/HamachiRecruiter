import daft

import instructor
from pydantic import BaseModel, Field

from anthropic import AnthropicBedrock
from fireworks.client import Fireworks, AsyncFireworks
import instructor
from pydantic import BaseModel
import os

if __name__ == "__main__":
    df = daft.read_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/raw_commits_hash_dedupped"
    )

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

    df.write_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_raw2",
        write_mode="overwrite"
    )
