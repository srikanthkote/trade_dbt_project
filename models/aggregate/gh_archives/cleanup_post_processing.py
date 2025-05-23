import duckdb

def model(dbt, session):
    dbt.config(materialized="table")
    # DataFrame representing an upstream model
    upstream_model = dbt.ref("gold_gh_archives_top_pullrequests_by_user").df()

    queries = [
        "DROP TABLE IF EXISTS gold_gh_archives_daily",        
        "DROP TABLE IF EXISTS gold_gh_archives_top_forks_by_repo",
        "DROP TABLE IF EXISTS gold_gh_archives_top_pullrequests_by_repo",
        "DROP TABLE IF EXISTS gold_gh_archives_top_pullrequests_by_user",
        "DROP TABLE IF EXISTS silver_gh_archives_arrowwrite",
        "DROP VIEW IF EXISTS silver_gh_archives_dailyload",
        "DROP TABLE IF EXISTS silver_gh_archives_deltascan_polars",
        "DROP TABLE IF EXISTS silver_gh_archives_deltascan",
        "DROP VIEW IF EXISTS silver_gh_archives_deltawrite",
        "DROP TABLE IF EXISTS silver_gh_archives_forks",
        "DROP TABLE IF EXISTS silver_gh_archives_groupby_timewindow",
        "DROP TABLE IF EXISTS silver_gh_archives_pullrequests",
        "DROP TABLE IF EXISTS silver_gh_archives_pushevents",
        "DROP TABLE IF EXISTS silver_gh_archives_queries",
    ]

    con = duckdb.connect("/Users/srikanthkotekar/code/analytics-workspace/trade-statistics.duckdb")

    for query in queries:
        # Execute the query
        con.sql(query)     

    return upstream_model