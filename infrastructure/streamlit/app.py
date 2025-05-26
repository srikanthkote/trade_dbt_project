import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

# App title
st.title("WELCOME TO STREAMLIT APPS")

# Header
st.subheader("silver_gh_archives_deltascan_polars")
st.code(
    "select actor_id, type, created_at, repo_id, repo_name from silver_gh_archives_deltascan_polars where actor_id = '49699333' and type = 'PullRequestEvent' and created_at between '2025-01-01 00:00:00.000' and '2025-01-01 01:00:00.000'"
)

con = duckdb.connect(
    "/Users/srikanthkotekar/code/analytics-workspace/trade-statistics.duckdb",
    read_only=True,
)

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

# Header
st.subheader("silver_gh_archives_groupby_timewindow")
st.code("SELECT * FROM silver_gh_archives_groupby_timewindow")
query = f"""
    SELECT * FROM silver_gh_archives_groupby_timewindow"""
df = con.sql(query).df()
st.dataframe(df)
fig = px.scatter(
    df,
    x="created_at",
    y="len",
    color="type",
    color_discrete_sequence=px.colors.qualitative.Plotly,
)
st.plotly_chart(fig, use_container_width=True)

## closing the DB connection
con.close()
