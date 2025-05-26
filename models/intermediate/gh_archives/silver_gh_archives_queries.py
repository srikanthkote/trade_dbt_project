import duckdb
from time import perf_counter_ns
from tabulate import tabulate


# model that runs many queries in order to check latencies
def model(dbt, session):

    # DataFrame representing an upstream model
    silver_alldata_df = dbt.ref("silver_gh_archives_deltascan_polars").df()

    queries = [
        # query to run against a Python Dataframe representing the upstream model
        # illustrates the seamless integration and querying capabilities of DuckDB inside the
        # Python ecosystem; it does not matter whether the relation has been created as a table,
        # sourced from a file, or represented by a pandas DataFrame.
        "select actor_id, type, created_at, repo_id, repo_name from silver_alldata_df where actor_id = '49699333' and type = 'PullRequestEvent' and created_at between '2025-01-01 00:00:00.000' and '2025-01-01 01:00:00.000'",
        # queries to run against materialized Duckdb tables
        "select count(1) from silver_gh_archives_deltascan_polars",
        "select actor_id, type, created_at, repo_id, repo_name from silver_gh_archives_deltascan_polars where actor_id = '49699333' and type = 'PullRequestEvent' and created_at between '2025-01-01 00:00:00.000' and '2025-01-01 01:00:00.000'",
        "select a.actor_id, a.type, a.created_at, a.repo_id, a.repo_name from silver_gh_archives_pullrequests a, silver_gh_archives_forks b	where a.actor_id = b.actor_id and a.actor_id = '163356328' and a.created_at between '2025-01-01 00:00:00.000' and '2025-01-02 01:00:00.000'",
        # query that run against parquet files
        "select actor_id, type, created_at, repo_id, repo_name from '/Users/srikanthkotekar/Downloads/gh_archives/raw/*.parquet' where actor_id = '49699333' and created_at between '2025-01-01 00:00:00.000' and '2025-01-02 01:00:00.000'",
    ]

    con = duckdb.connect(
        "/Users/srikanthkotekar/code/analytics-workspace/trade-statistics.duckdb"
    )
    con.sql(
        "CREATE INDEX idx_actor_id ON silver_gh_archives_deltascan_polars(actor_id);"
    )
    con.sql("CREATE INDEX idx_type ON silver_gh_archives_deltascan_polars(type);")
    con.sql(
        "CREATE INDEX idx_created_at ON silver_gh_archives_deltascan_polars(created_at);"
    )
    con.sql("CREATE INDEX idx_repo_id ON silver_gh_archives_deltascan_polars(repo_id);")
    con.sql(
        "CREATE INDEX idx_repo_name ON silver_gh_archives_deltascan_polars(repo_name);"
    )

    con.sql(
        "CREATE INDEX idx_pr_actor_id ON silver_gh_archives_pullrequests(actor_id);"
    )
    con.sql("CREATE INDEX idx_pr_type ON silver_gh_archives_pullrequests(type);")
    con.sql(
        "CREATE INDEX idx_pr_created_at ON silver_gh_archives_pullrequests(created_at);"
    )
    con.sql("CREATE INDEX idx_pr_repo_id ON silver_gh_archives_pullrequests(repo_id);")
    con.sql(
        "CREATE INDEX idx_pr_repo_name ON silver_gh_archives_pullrequests(repo_name);"
    )

    con.sql("CREATE INDEX idx_fr_actor_id ON silver_gh_archives_forks(actor_id);")
    con.sql("CREATE INDEX idx_fr_type ON silver_gh_archives_forks(type);")
    con.sql("CREATE INDEX idx_fr_created_at ON silver_gh_archives_forks(created_at);")
    con.sql("CREATE INDEX idx_fr_repo_id ON silver_gh_archives_forks(repo_id);")
    con.sql("CREATE INDEX idx_fr_repo_name ON silver_gh_archives_forks(repo_name);")

    table_data = [
        ["Query", "Latency"],
    ]

    for query in queries:
        t1_start = perf_counter_ns()

        # Execute the query
        result = con.sql(query).df()
        t1_stop = perf_counter_ns()
        elapsed = round((t1_stop - t1_start) / 1000000, 3)
        table_data.append([query, elapsed])

    # explicitly drop the indices to support regeneration
    con.sql("DROP INDEX idx_actor_id")
    con.sql("DROP INDEX idx_type")
    con.sql("DROP INDEX idx_created_at")
    con.sql("DROP INDEX idx_repo_id")
    con.sql("DROP INDEX idx_repo_name")

    con.sql("DROP INDEX idx_pr_actor_id")
    con.sql("DROP INDEX idx_pr_type")
    con.sql("DROP INDEX idx_pr_created_at")
    con.sql("DROP INDEX idx_pr_repo_id")
    con.sql("DROP INDEX idx_pr_repo_name")

    con.sql("DROP INDEX idx_fr_actor_id")
    con.sql("DROP INDEX idx_fr_type")
    con.sql("DROP INDEX idx_fr_created_at")
    con.sql("DROP INDEX idx_fr_repo_id")
    con.sql("DROP INDEX idx_fr_repo_name")

    # explicitly close the connection
    con.close()

    print(tabulate(table_data, headers="firstrow", tablefmt="grid"))

    return result
