import streamlit as st
import pandas as pd
import duckdb

# App title
st.title("WELCOME TO STREAMLIT APPS")

# Header
st.subheader("silver_gh_archives_deltascan_polars")
st.code("select actor_id, type, created_at, repo_id, repo_name from silver_gh_archives_deltascan_polars where actor_id = '49699333' and type = 'PullRequestEvent' and created_at between '2025-01-01 00:00:00.000' and '2025-01-01 01:00:00.000'")

con = duckdb.connect("/Users/srikanthkotekar/code/analytics-workspace/trade-statistics.duckdb", read_only=True)

query = f"""
    select actor_id, type, created_at, repo_id, repo_name from silver_gh_archives_deltascan_polars where actor_id = '49699333' and type = 'PullRequestEvent' and created_at between '2025-01-01 00:00:00.000' and '2025-01-01 01:00:00.000'"""

df = con.sql(query).df()
st.dataframe(df)

query = f"""
    SELECT * FROM gold_gh_archives_top_pullrequests_by_repo"""

# Header
st.subheader("gold_gh_archives_top_pullrequests_by_repo")
st.code("SELECT * FROM gold_gh_archives_top_pullrequests_by_repo")
df = con.sql(query).df()
st.dataframe(df)
st.bar_chart(df, x="repo_name", y="counts", horizontal=True)

st.scatter_chart(df, x="repo_id", y="counts",color=["#FF0000"])

con.close()