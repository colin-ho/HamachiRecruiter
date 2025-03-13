import daft
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

class QueryAnalyzer:
    def __init__(self):
        key = os.environ.get("OPENAI_API_KEY")
        self.client = OpenAI(api_key=key)

        df = daft.read_parquet("s3://eventual-data-test-bucket/HamachiRecruiterData/contributors_with_languages_and_project_types")
        df = df.where(~daft.col('author_email').str.contains('[bot]') & ~daft.col('author_email').str.contains('@github.com')).collect()
        
        self.sess = daft.Session()
        self.sess.create_temp_table("contributions", df)

        # self.conn = duckdb.connect()
        # self.conn.execute("CREATE TABLE contributions AS SELECT * FROM read_parquet('data/demo-analyzed-data-10k-v2/716ae28b-bfbb-4fcb-ba34-76daa2777df5-0.parquet')")

    def natural_language_query(self, query: str) -> str:
        # First ask OpenAI to convert natural language to SQL
        system_prompt = """You are an expert at converting natural language questions into SQL queries.
        The database has a table called 'contributions' with these columns:
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
        - repo: string

        Convert the following question into a SQL query:

        - Try to show other metadata such as what repo they contribute to and the reason when it is possible.
        - the output schema should always be in this order: [author_name, author_email, commit_count, impact_to_project, technical_ability, languages, project_type, repo, reason]
        - we have a very simple SQL parser so avoid complex SQL syntax
        - avoid use of the `ANY` operator
        - when comparing dates, make sure to cast string literals to timestamps using CAST('2024-01-01' AS TIMESTAMP) format
        - use >= for "after" or "since" comparisons and <= for "before" comparisons
        - for date ranges, use BETWEEN CAST('2024-01-01' AS TIMESTAMP) AND CAST('2024-12-31' AS TIMESTAMP)
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
    while True:
        query = input("\nEnter your query (or 'quit' to exit): ")
        if query.lower() == 'quit':
            break
            
        result = analyzer.natural_language_query(query)
        print("\nResult:")
        print(result)

    analyzer.close()
