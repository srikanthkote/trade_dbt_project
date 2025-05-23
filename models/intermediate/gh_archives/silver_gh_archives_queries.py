import duckdb
from time import perf_counter_ns


# model that runs many queries in order to check latencies
def model(dbt, session):

    queries = [
        "select count(1) from silver_gh_archives_deltascan_polars",
        "select actor_id, type, created_at, repo_id, repo_name from silver_gh_archives_deltascan_polars where actor_id = '49699333' and type = 'PullRequestEvent' and created_at between '2025-01-01 00:00:00.000' and '2025-01-01 01:00:00.000'",
        "select a.actor_id, a.type, a.created_at, a.repo_id, a.repo_name from silver_gh_archives_pullrequests a, silver_gh_archives_forks b	where a.actor_id = b.actor_id and a.actor_id = '163356328' and a.created_at between '2025-01-01 00:00:00.000' and '2025-01-02 01:00:00.000'",
        "select actor_id, type, created_at, repo_id, repo_name from '/Users/srikanthkotekar/Downloads/gh_archives/raw/*.parquet' where actor_id = '49699333' and created_at between '2025-01-01 00:00:00.000' and '2025-01-02 01:00:00.000'",
    ]

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_gh_archives_deltascan_polars").df()
    con = duckdb.connect("/Users/srikanthkotekar/code/analytics-workspace/trade-statistics.duckdb")
    con.sql("CREATE INDEX idx_actor_id ON silver_gh_archives_deltascan_polars(actor_id);")
    con.sql("CREATE INDEX idx_type ON silver_gh_archives_deltascan_polars(type);")
    con.sql("CREATE INDEX idx_created_at ON silver_gh_archives_deltascan_polars(created_at);")
    con.sql("CREATE INDEX idx_pr_actor_id ON silver_gh_archives_pullrequests(actor_id);")
    con.sql("CREATE INDEX idx_pr_type ON silver_gh_archives_pullrequests(type);")
    con.sql("CREATE INDEX idx_pr_created_at ON silver_gh_archives_pullrequests(created_at);")
    con.sql("CREATE INDEX idx_fr_actor_id ON silver_gh_archives_forks(actor_id);")
    con.sql("CREATE INDEX idx_fr_type ON silver_gh_archives_forks(type);")
    con.sql("CREATE INDEX idx_fr_created_at ON silver_gh_archives_forks(created_at);")
    
    for query in queries:
        t1_start = perf_counter_ns()

        # Execute the query
        result = con.sql(query).df()      
        t1_stop = perf_counter_ns()
        elapsed = round((t1_stop - t1_start)/1000000, 3)
        print(f"{query} - {elapsed}ms")
  
        # Print the result
        print(result)

    # explicitly drop the indices to support regeneration
    con.sql("DROP INDEX idx_actor_id")
    con.sql("DROP INDEX idx_type")
    con.sql("DROP INDEX idx_created_at")
    con.sql("DROP INDEX idx_pr_actor_id")
    con.sql("DROP INDEX idx_pr_type")
    con.sql("DROP INDEX idx_pr_created_at")
    con.sql("DROP INDEX idx_fr_actor_id")
    con.sql("DROP INDEX idx_fr_type")
    con.sql("DROP INDEX idx_fr_created_at")

    # explicitly close the connection
    con.close()

    return result