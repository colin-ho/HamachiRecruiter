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
        df = df.where(~daft.col('author_email').str.contains('[bot]') & ~daft.col('author_email').str.contains('@github.com')).collect()
        
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
        - author_email: string
        - author_name: string 
        - email_count: int
        - commit_count: int
        - lines_added: int
        - lines_deleted: int
        - lines_modified: int
        - first_commit: datetime
        - last_commit: datetime
        - reason: string
        - impact_to_project: float (1-10 score)
        - technical_ability: float (1-10 score)
        - languages: string (multiple values separated by '|', all values are lowercase and normalized)
        - project_type: string (multiple values separated by '|', possible values are lowercase and normalized: [web_development, data_processing, dev_ops, mobile_development, machine_learning, crypto, artificial_intelligence, game_development, cloud_computing, security, developer_tools])
        - repo: string (case sensitive, in the format of owner/repo)

        When creating SQL queries:
        - The output schema should always be in this order: [author_name, author_email, commit_count, impact_to_project, technical_ability, languages, project_type, repo, reason, first_commit, last_commit, lines_modified]
        - Keep the SQL query as simple as possible and avoid complex syntax
        - Avoid use of the `ANY` operator
        - When comparing dates, cast string literals to timestamps using CAST('2024-01-01' AS TIMESTAMP) format
        - Use >= for "after" or "since" comparisons and <= for "before" comparisons
        - For date ranges, use BETWEEN CAST('2024-01-01' AS TIMESTAMP) AND CAST('2024-12-31' AS TIMESTAMP)
        - Unless specified otherwise, limit results to top 100 candidates ordered by technical_ability DESC, impact_to_project DESC, commit_count DESC
        - Project_type must be one of the values listed above, but users may use broader or adjacent terms
        
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
                daft.col('project_type').any_value(),
                daft.col('reason').any_value(),
                daft.col('repo').agg_list(),
            )
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
