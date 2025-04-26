### Using the starter project
To run the project: dbt run, dbt test

## Resources:
- Learn about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Learn about DuckDB [in the docs](https://duckdb.org/docs/stable/index)

```mermaid
---
config:
  theme: neutral
---
timeline
    title Data stack
    section Data orchestration
        SQL-centric : dbt (Data build tool)
    section Data lake
        Storage layer : Google cloud storage
        File layer : Parquet
        Metadata layer : Delta lake 
        Compute layer : DuckDB
    section Data Warehouse
        Cloud : Snowflake (WIP)
```

```mermaid
---
config:
  theme: neo
---
sequenceDiagram
  participant Alice as raw layer<br>(gcs)
  participant John as silver layer<br>(gcs)
  participant Bob as gold layer<br>(gcs)

  autonumber
  Alice ->> John: read:json, <br>convert to parquet, <br> convert to deltalake format 
  loop Processing
    John ->> John: silver_gh_archives_dailyload.sql <br> silver_gh_archives_deltawrite.py <br> silver_gh_archives_deltascan.sql <br> silver_gh_archives_deltascan_polars.py <br> silver_gh_archives_groupby_timewindow.py <br> silver_gh_archives_forks.py <br> silver_gh_archives_pullrequests.py <br> silver_gh_archives_pushevents.py
  end
  Note right of John: Now aggregate the data!
  John ->> Bob: aggregate forks, PRs and push events
  loop Processing
    Bob ->> Bob: gold_gh_archives_daily.sql <br> gold_gh_archives_top_pullrequests_by_repo.py <br> gold_gh_archives_top_pullrequests_by_user.py
  end  
```
