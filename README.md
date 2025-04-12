This is the place for me to experiment with modern data stack. 
Currently I have used DBT to ingest and process files using medallion architecture with DuckDB as the compute layer.

### Using the starter project
To run the project: dbt run, dbt test

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)

## What is DuckDB
- An Embeddable & Portable Database
- A Columnar OLAP Database
- Interoperable SQL-Powered DataFrame
- A Federated Query Engine
- A Single-Node Compute Engine

```mermaid
---
config:
  theme: neo
  look: handDrawn
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
        File layer : Parquet, JSON
        Metadata layer : Hive metastore 
        Compute layer : In-process: DuckDB
```
