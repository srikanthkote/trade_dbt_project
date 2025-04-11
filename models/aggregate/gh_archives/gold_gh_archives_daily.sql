-- depends_on: {{ ref('silver_gh_archives_daily') }}
{{
    config(
        materialized='external',
        location="gs://gh_archives/gold",
        options={
            "partition_by": "created_year, created_month, created_week"
        }
    )
}}

WITH aggregate_data AS (
    SELECT type, repo_name, repo_url, count(*) as count, date_trunc('day', created_at) as event_date
        FROM read_parquet({{ source('external_source', 'silver_gh_archives_daily') }}) GROUP BY ALL
)

SELECT 
    year(event_date) AS created_year,
    month(event_date) AS created_month,
    week(event_date) AS created_week,
    * 
    from aggregate_data




