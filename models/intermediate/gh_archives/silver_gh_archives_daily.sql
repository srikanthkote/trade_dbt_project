--        location="gs://gh_archives/silver",

{{
    config(
        materialized='external',
        location="{{ var('storage_location') }}/silver",
        options={
            "partition_by": "created_year, created_month, created_day",
            "Overwrite": "true"
        },
    )
}}

WITH bronze_data AS (
    SELECT 
        id, 
        type, 
        actor.id as actor_id, 
        actor.login as actor_login, 
        actor.display_login as actor_display_login, 
        actor.gravatar_id as actor_gravatar_id, 
        actor.url as actor_url, 
        actor.avatar_url as actor_avatar_url, 
        repo.id as repo_id, 
        repo.name as repo_name, 
        repo.url as repo_url, 
        payload, 
        public, 
        created_at, 
        strftime(created_at, '%Y-%m-%d') as created_at_dt,
        org.id as org_id,
        org.login as org_login,
        org.gravatar_id as org_gravatar_id,
        org.url as org_url,
        org.avatar_url as org_avatar_url,
        FROM read_json(
            {{ source('external_source', 'bronze_gh_archives_daily') }},
            ignore_errors = 'true',
            columns = { id: 'UBIGINT', 
                        type: 'VARCHAR', 
                        actor: 'JSON',
                        repo: 'JSON',
                        payload: 'JSON',
                        public: 'BOOLEAN',
                        created_at: 'TIMESTAMP',
                        org: 'JSON'
                    }
        )
)

SELECT 
    year(created_at) AS created_year,
    month(created_at) AS created_month,
    day(created_at) AS created_day,
    * 
    from bronze_data