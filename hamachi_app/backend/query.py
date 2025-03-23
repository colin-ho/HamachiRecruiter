import daft
import os
from dotenv import load_dotenv
from openai import OpenAI
from daft import col
import json
from typing import Dict, List, Any, Union
load_dotenv()

class QueryAnalyzer:
    def __init__(self):
        key = os.environ.get("OPENAI_API_KEY")
        self.client = OpenAI(api_key=key)
        
        data_dir = os.environ.get("HAMACHI_DATA_DIR", "contributors_and_repos")   

        df = daft.read_parquet(data_dir, io_config=daft.io.IOConfig(s3=daft.io.S3Config(anonymous=True, region_name="us-west-2")))
        df = df.where(~daft.col('author_email').str.contains('[bot]') & ~daft.col('author_email').str.contains('@github.com') & daft.col('author_email').__ne__('')).collect()
        
        self.sess = daft.Session()
        self.sess.create_temp_table("contributions", df)

        # self.conn = duckdb.connect()
        # self.conn.execute("CREATE TABLE contributions AS SELECT * FROM read_parquet('data/demo-analyzed-data-10k-v2/716ae28b-bfbb-4fcb-ba34-76daa2777df5-0.parquet')")

    def natural_language_query(self, query: str) -> Union[List[Dict[str, Any]], List[Dict[str, str]]]:
        # Define the agent's tools
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "execute_sql_query",
                    "description": "Execute a SQL query on the contributions database",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql_query": {
                                "type": "string",
                                "description": "The SQL query to execute"
                            }
                        },
                        "required": ["sql_query"]
                    }
                }
            }
        ]
        
        # Define the agent's system prompt
        system_prompt = """You are an AI assistant that helps users find information about open source contributors.
        You have access to a database with a table called 'contributions' that contains the following columns:
        - author_email: string (lowercase). The email address of the contributor.
        - author_name: string (lowercase). The name of the contributor.
        - email_count: int. The number of emails the contributor has.
        - commit_count: int. The number of commits the contributor has made.
        - lines_added: int. The number of lines added by the contributor.
        - lines_deleted: int. The number of lines deleted by the contributor.
        - lines_modified: int. The number of lines modified by the contributor.
        - first_commit: datetime. The date of the contributor's first commit.
        - last_commit: datetime. The date of the contributor's last commit.
        - reason: string. The reasoning behind the impact_to_project and technical_ability scores.
        - impact_to_project: float (1-10 score). A score between 1 and 10 indicating the impact the contributor has on the project.
        - technical_ability: float (1-10 score). A score between 1 and 10 indicating the technical ability of the contributor.
        - languages: string (multiple values separated by '|', all values are lowercase and normalized). The programming languages the contributor has worked with on this project.
        - keywords: string (multiple values separated by '|', all values are lowercase and normalized, hyphenated if keywords are compound words). The keywords associated with the project, such as the domain of the project, the purpose of the project, frameworks and libraries used, etc.
        - repo: string (case sensitive, in the format of owner/repo). The repository the contributor has worked on.

        When creating SQL queries:
        - The output schema should always be in this order: [author_name, author_email, commit_count, impact_to_project, technical_ability, languages, keywords, repo, reason, first_commit, last_commit, lines_modified]
        - Keep the SQL query as simple as possible and avoid complex syntax
        - Avoid use of the `ANY` operator
        - When comparing dates, cast string literals to timestamps using CAST('2024-01-01' AS TIMESTAMP) format
        - Use >= for "after" or "since" comparisons and <= for "before" comparisons
        - For date ranges, use BETWEEN CAST('2024-01-01' AS TIMESTAMP) AND CAST('2024-12-31' AS TIMESTAMP)
        - Unless specified otherwise, order results by technical_ability DESC, impact_to_project DESC, commit_count DESC
        - For pipe-separated fields, use appropriate pattern matching techniques
        - When searching for keywords, take note that keywords are hyphenated if keywords are compound words.
        - Note that the repo is in the format of owner/repo. This means that you can use the LIKE operator to search for repos by owner or repo name.
        - When adding filters on string columns i.e. author_name, author_email, repo, make sure to lowercase the column.
        
        You must use the execute_sql_query function to answer user questions.
        """

        # Create the agent
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            tools=tools,
            tool_choice={"type": "function", "function": {"name": "execute_sql_query"}}
        )

        # Extract the SQL query from the agent's response
        tool_calls = response.choices[0].message.tool_calls
        if not tool_calls:
            return [{"error": "Unable to generate a SQL query. Please try rephrasing your question."}]
        
        sql_query = json.loads(tool_calls[0].function.arguments).get("sql_query")
        print(f"SQL Query:\n{sql_query}")
        
        # Validate the SQL query
        if not sql_query or not sql_query.lower().strip().startswith("select"):
            return [{"error": "Unable to answer question. Please ask only questions about open source project contributors. If we made a mistake, please file an issue at https://github.com/colin-ho/HamachiRecruiter/issues"}]

        # Execute the SQL query
        try:
            result = self.sess.sql(sql_query)
            result_with_struct = result.with_column(
                "repo", daft.struct(
                    col("commit_count"),
                    col("impact_to_project"),
                    col("technical_ability"), 
                    col("repo"),
                    col("first_commit").cast(daft.DataType.string()),
                    col("last_commit").cast(daft.DataType.string()),
                    col("lines_modified")
                )
            )
            
            result_dedupped = result_with_struct.groupby('author_email').agg(
                daft.col('author_name').any_value(),
                daft.col('languages').any_value(),
                daft.col('keywords').any_value(),
                daft.col('commit_count').sum(),
                daft.col('impact_to_project').mean(),
                daft.col('technical_ability').mean(),
                daft.col('reason').agg_list(),
                daft.col('repo').agg_list(),
            ).sort(by=['technical_ability', 'impact_to_project', 'commit_count'], desc=[True, True, True]).limit(100)
            return result_dedupped.to_pylist()
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return [{"error": str(e)}]

    def close(self):
        del self.sess


if __name__ == "__main__":
    analyzer = QueryAnalyzer()

    # Example usage
    # query = "Who are the top 10 contributors by technical ability and have contributed in 2024?"
    while True:
        query = input("\nEnter your query (or 'quit' to exit): ")
        if query.lower() == 'quit':
            break
            
        result = analyzer.natural_language_query(query)
        print("\nResult:")
        print(result)

    analyzer.close()
