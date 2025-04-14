{{
    config(
        materialized='table'
    )
}}

-- This section begins the aggregation of forks, pull requests, and push events
with aggr_forks_pulls_push as (
    -- The macro will add nulls for missing columns in each relation.
    {{ dbt_utils.union_relations(
        relations=[ref('silver_gh_archives_forks'), ref('silver_gh_archives_pullrequests'), ref('silver_gh_archives_pushevents')]
    ) }}
)

-- The following select statement will create a table with the aggregated data
SELECT 
    year(created_at) AS created_year,
    month(created_at) AS created_month,
    day(created_at) AS created_day,
    * 
        from 
    aggr_forks_pulls_push 
    ORDER BY id




