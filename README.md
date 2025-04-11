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
    participant Alice
    participant Bob
    Alice->>John: Hello John, how are you?
    loop HealthCheck
        John->>John: Fight against hypochondria
    end
    Note right of John: Rational thoughts<br/>prevail...
    John-->>Alice: Great!
    John->>Bob: How about you?
    Bob-->>John: Jolly good!
```
