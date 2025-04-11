This is the place for me to experiment with modern data stack. 
Currently I have used DBT to ingest and process files using medallion architecture with DuckDB as the compute layer.

### Using the starter project
To run the project:
- dbt run
- dbt test

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Learn more about DuckDB (https://www.pracdata.io/p/duckdb-beyond-the-hype)

```mermaid
sequenceDiagram
  participant Alice as raw(gcs)
  participant John as silver(gcs)
  participant Bob as gold(gcs)

  autonumber
  Alice ->> John: model:silver_gh_archives_daily.sql <br>read:json, <br>convert to parquet
  loop Processing
    John ->> John: materialized='external' <br> partition by year,month,week
  end
  Note right of John: Rational thoughts!
  John ->> Bob: model:gold_gh_archives_daily.sql <br>read:parquet, <br>aggregate data, <br>write:parquet
```
