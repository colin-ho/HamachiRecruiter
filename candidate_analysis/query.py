import daft
import os
from openai import OpenAI


class QueryAnalyzer:
    def __init__(self):
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

        df = daft.read_parquet(
            "data/demo-analyzed-data-10k-v2/716ae28b-bfbb-4fcb-ba34-76daa2777df5-0.parquet"
        ).collect()
        self.sess = daft.Session()
        self.sess.create_temp_table("contributions", df)

        # self.conn = duckdb.connect()
        # self.conn.execute("CREATE TABLE contributions AS SELECT * FROM read_parquet('data/demo-analyzed-data-10k-v2/716ae28b-bfbb-4fcb-ba34-76daa2777df5-0.parquet')")

    def natural_language_query(self, query: str) -> str:
        # First ask OpenAI to convert natural language to SQL
        system_prompt = """You are an expert at converting natural language questions into SQL queries.
        The database has a table called 'contributions' with these columns:
        - repo_owner: string
        - repo_name: string
        - author_name: string
        - author_email: string
        - commit_count: int
        - lines_added: int
        - lines_deleted: int
        - lines_modified: int
        - impact_to_project: int (1-10 score)
        - technical_ability: int (1-10 score)
        - reason: string
        - first_commit: datetime
        - last_commit: datetime



        Convert the following question into a SQL query:

        - Try to show other metadata such as what repo they contribute to and the reason when it is possible.
        - the output schema should always be in this order: [author_name, author_email, commit_count, impact_to_project,technical_ability, reason]
        """

        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query},
            ],
        )

        # Strip any markdown code block formatting from the response
        sql_query = (
            response.choices[0].message.content.strip("`").replace("sql", "").strip()
        )
        print(f"SQL Query:\n{sql_query}")
        # Execute the SQL query
        try:
            result = self.sess.sql(sql_query).collect()
            return result
        except Exception as e:
            return f"Error executing query: {str(e)}"

    def close(self):
        del self.sess


if __name__ == "__main__":
    analyzer = QueryAnalyzer()

    # Example usage
    # query = "Who are the top 10 contributors by technical ability and have contributed in 2024?"
    query = "who are the most cracked engineers that we should hire at a series a startup in san francisco?"
    result = analyzer.natural_language_query(query)
    print(f"\nQuery: {query}")
    print("\nResult:")
    print(result)

    analyzer.close()
