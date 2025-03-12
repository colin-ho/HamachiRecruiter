import daft


from typing import Literal

import instructor
from pydantic import BaseModel, Field
from openai import OpenAI

class CommitQuality(BaseModel):
    impact_to_project: int = Field(ge=1, le=10, description="Score from 1-10 indicating impact to project where 10 is someone who the project revolves around and 1 is tiny contributions")
    technical_ability: int = Field(ge=1, le=10, description="Score from 1-10 indicating technical ability where 10 is a god tier expert and 1 is a beginner")
    reason: str


@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            impact_to_project=daft.DataType.int64(),
            technical_ability=daft.DataType.int64(),
            reason=daft.DataType.string(),
        )
    )
)
def analyze_commit_message(repo_name, commit_count, lines_added, lines_deleted, lines_modified, files_changed, message):

    client = instructor.from_openai(OpenAI())
    results = []
    for repo, c, la, ld, lm, f, msg in zip(repo_name.to_pylist(), commit_count.to_pylist(), lines_added.to_pylist(), lines_deleted.to_pylist(), lines_modified.to_pylist(), files_changed.to_pylist(), message.to_pylist()):
        prompt = f"""Analyze this series of git commit messages and how impactful is this user to the project.

        repo name: {repo}
        commit count: {c}
        total lines added: {la}
        total lines deleted: {ld}
        total lines modified: {lm}
         
        files touched: {f}

        commit_messages:
        {msg}
        
        """
        result = client.chat.completions.create(
            model="gpt-4o-mini",
            response_model=CommitQuality,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        results.append(result.model_dump())
    return results


if __name__ == "__main__":
    df = daft.read_parquet("data/commit_data.parquet")
    df = df.groupby('repo_owner', 'repo_name', 'author_name').agg([
        daft.col('date').count().alias('commit_count'),
        daft.col('lines_added').sum(),
        daft.col('lines_deleted').sum(),
        daft.col('lines_modified').sum(),
        daft.col('files_changed').agg_concat(),
        daft.col('message').agg_concat(),
        daft.col('date').min().alias('first_commit'),
        daft.col('date').max().alias('last_commit')
    ])
    df = df.limit(10)

    df = df.with_column('commit_analysis', analyze_commit_message(df['repo_name'], df['commit_count'], df['lines_added'], df['lines_deleted'], df['lines_modified'], df['files_changed'], df['message']))
    df = df.with_columns({
        "impact_to_project":  df['commit_analysis'].struct.get('impact_to_project'),
        "technical_ability":  df['commit_analysis'].struct.get('technical_ability'),
        "reason":  df['commit_analysis'].struct.get('reason'),
    })
    df = df.exclude('commit_analysis')
    df.show()
