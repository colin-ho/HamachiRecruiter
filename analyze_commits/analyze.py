import daft

import instructor
from pydantic import BaseModel, Field

from anthropic import AnthropicBedrock
from fireworks.client import Fireworks, AsyncFireworks
import instructor
from pydantic import BaseModel
import os


class CommitQuality(BaseModel):
    impact_to_project: int = Field(
        ge=1,
        le=10,
        description="Score from 1-10 indicating impact to project where 10 is someone who the project revolves around and 1 is tiny contributions",
    )
    technical_ability: int = Field(
        ge=1,
        le=10,
        description="Score from 1-10 indicating technical ability where 10 is a god tier expert and 1 is a beginner",
    )
    reason: str


@daft.udf(
    return_dtype=daft.DataType.struct(
        dict(
            impact_to_project=daft.DataType.int64(),
            technical_ability=daft.DataType.int64(),
            reason=daft.DataType.string(),
        )
    ),
)
def analyze_commit_message(
    repo_name,
    commit_count,
    lines_added,
    lines_deleted,
    lines_modified,
    files_changed,
    message,
):
    client = AsyncFireworks(api_key=os.environ.get("FIREWORKS_API_KEY"), timeout=60)
    client = instructor.from_fireworks(client)

    results = []

    async def analyze_single_commit(client, repo, c, la, ld, lm, f, msg):
        # Limit message length to first 500 lines or 10000 words
        msg_lines = msg.split("\n")[:500]
        msg_text = "\n".join(msg_lines)

        msg_words = msg_text.split()[:10000]
        msg_text = " ".join(msg_words)
        msg = msg_text

        f = list(set(f))[:100]

        prompt = f"""You are an expert at analyzing GitHub contributions and determining developer impact and technical ability.

        Analyze the following GitHub contribution data to assess:
        1. The contributor's impact to the project (score 1-10):
           - 10: Core maintainer/architect whose work is foundational
           - 7-9: Major feature owner or frequent substantial contributor
           - 4-6: Regular contributor with meaningful additions
           - 1-3: Minor/occasional contributor

        2. Their technical ability (score 1-10):
           - 10: Expert system architect/developer
           - 7-9: Very strong technical skills
           - 4-6: Competent developer
           - 1-3: Beginning developer

        Consider:
        - Repository: {repo}
        - Contribution volume: {c} commits
        - Code changes: {la} lines added, {ld} lines deleted, {lm} lines modified
        - Scope of changes: Files modified: {f}

        Based on these commit messages:
        {msg}

        Keep your reason explanation brief - maximum 4 sentences.
        """
        try:
            result = await client.chat.completions.create(
                model="accounts/fireworks/models/llama-v3p2-3b-instruct#accounts/sammy-b656e2/deployments/61bd1cb6",
                # model="accounts/fireworks/models/llama-v3p2-3b-instruct",
                response_model=CommitQuality,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=128,
                max_retries=3,
            )

            print(result.reason)
            return result.model_dump()
        except Exception as e:
            print(f"Got error when validating input from model {e}")
            return None

    # Limit concurrent requests to 5 (or adjust as needed)
    import asyncio

    semaphore = asyncio.Semaphore(64)

    async def analyze_with_semaphore(*args):
        async with semaphore:
            return await analyze_single_commit(*args)

    tasks = [
        analyze_with_semaphore(client, repo, c, la, ld, lm, f, msg)
        for repo, c, la, ld, lm, f, msg in zip(
            repo_name.to_pylist(),
            commit_count.to_pylist(),
            lines_added.to_pylist(),
            lines_deleted.to_pylist(),
            lines_modified.to_pylist(),
            files_changed.to_pylist(),
            message.to_pylist(),
        )
    ]

    # Run coroutines concurrently and gather results
    import asyncio

    results = []

    async def run_tasks():
        return await asyncio.gather(*tasks)

    results = asyncio.run(run_tasks())
    return results


if __name__ == "__main__":
    # df = daft.read_parquet("data/commit_data.parquet")
    df = daft.read_parquet(
        "s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_raw/"
    )
    # we only care about folks who have contributed at least 100 lines of code and 3 commits
    df = df.where("lines_modified > 100 AND commit_count >= 3")
    df = df.limit(100)

    df = df.with_column(
        "commit_analysis",
        analyze_commit_message(
            df["repo_name"],
            df["commit_count"],
            df["lines_added"],
            df["lines_deleted"],
            df["lines_modified"],
            df["files_changed"],
            df["message"],
        ),
    )
    df = df.with_columns(
        {
            "impact_to_project": df["commit_analysis"].struct.get("impact_to_project"),
            "technical_ability": df["commit_analysis"].struct.get("technical_ability"),
            "reason": df["commit_analysis"].struct.get("reason"),
        }
    )
    df = df.exclude("commit_analysis")
    # df.write_parquet("s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_data/")
    df.write_parquet("data/contributer_data_100")
