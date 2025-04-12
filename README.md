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
    title Modern Data stack
    section Data orchestration
        SQL-centric : dbt (Data build tool)
    section Data lake
        Storage layer : Google cloud storage
        File layer : Parquet
        Metadata layer : Hive Metastore 
        Compute layer : DuckDB : Trino (WIP)
    section Data Warehouse
        Cloud : Snowflake (WIP)
```

```mermaid
---
config:
  theme: neo
  look: neo
---
sequenceDiagram
  participant Alice as raw bucket<br>(gcs)
  participant John as silver bucket<br>(gcs)
  participant Bob as gold bucket<br>(gcs)

  autonumber
  Alice ->> John: read:json, <br>convert to parquet
  loop Processing
    John ->> John: dbt model:silver_gh_archives_daily.sql <br> materialized='external' <br> partition by year,month,week
  end
  Note right of John: Rational thoughts!
  John ->> Bob: read:parquet, <br>aggregate data, <br>write:parquet
  loop Processing
    Bob ->> Bob: dbt model:gold_gh_archives_daily.sql <br>materialized='external' <br> partition by year,month,week
  end  
```
