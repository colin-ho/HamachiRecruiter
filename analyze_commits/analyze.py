import daft




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
    df.sort('lines_modified', desc=True).show()
