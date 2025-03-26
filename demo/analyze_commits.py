# /// script
# [tools.ev]
# env_vars = { OPENAI_API_KEY = ""}
# ///

import argparse
import daft
import instructor
from openai import AsyncOpenAI
from pydantic import BaseModel, Field
import os
import asyncio

class CommitQuality(BaseModel):
    impact_to_project: int = Field(
        ge=1, le=10,
        description="Score from 1-10 indicating impact to project.",
    )
    technical_ability: int = Field(
        ge=1, le=10,
        description="Score from 1-10 indicating technical ability.",
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
def analyze_commit(
    repo_name, commit_count, lines_added, lines_deleted, lines_modified, files_changed, message,
):
    client = instructor.from_openai(AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY")))

    async def analyze_single_commit(client, repo, c, la, ld, lm, f, msg):
        msg = "\n".join(msg.split("\n")[:500])
        msg = " ".join(msg.split()[:10000])
        f = list(set(f))[:100]

        prompt = f"""Analyze the following GitHub contribution data to assess:
        1. The contributor's impact to the project (score 1-10)
        2. Their technical ability (score 1-10)

        Consider:
        - Repo {repo}
        - Commits: {c}
        - Lines added: {la}
        - Lines deleted: {ld}
        - Lines modified: {lm}
        - Files changed: {f}
        - Commit messages: {msg}

        Maximum 4 sentences.
        """
        result = await client.chat.completions.create(
            model="gpt-4o-mini",
            response_model=CommitQuality,
            messages=[{"role": "user", "content": prompt}],
        )
        return result.model_dump()

    
    async def run_tasks():
        tasks = []
        for repo, c, la, ld, lm, f, msg in zip(
            repo_name, commit_count, lines_added, lines_deleted, 
            lines_modified, files_changed, message,
        ):
            async with asyncio.Semaphore(64):
                tasks.append(analyze_single_commit(client, repo, c, la, ld, lm, f, msg))
        return await asyncio.gather(*tasks)
    
    return asyncio.run(run_tasks())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str, default="s3://eventual-data-test-bucket/HamachiRecruiterData/contributer_raw2")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--write-to-file", action="store_true")
    parser.add_argument("--output-path", type=str, default="analyzed_contributors")
    args = parser.parse_args()
    
    df = daft.read_parquet(args.input_path)
    df = df.where("lines_modified > 100 AND commit_count >= 3").limit(args.limit)
    df = df.with_column(
        "commit_analysis",
        analyze_commit(
            df["repo_name"], df["commit_count"], df["lines_added"], 
            df["lines_deleted"], df["lines_modified"], df["files_changed"], df["message"],
        ),
    )
    df = df.with_columns({
        "impact_to_project": df["commit_analysis"].struct.get("impact_to_project"),
        "technical_ability": df["commit_analysis"].struct.get("technical_ability"),
        "reason": df["commit_analysis"].struct.get("reason"),
    }).exclude("commit_analysis")
    
    if args.write_to_file:
        files = df.write_parquet(args.output_path)
        print(f"Wrote files to {args.output_path}")
    else:
        df.show()
