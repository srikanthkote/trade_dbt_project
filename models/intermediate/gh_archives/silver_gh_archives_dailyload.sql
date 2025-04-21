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
        payload.forkee.id as payload_forkee_id,
        payload.forkee.name as payload_forkee_name,
        payload.forkee.full_name as payload_forkee_full_name,
        payload.forkee.owner.login as payload_forkee_owner_login,
        payload.forkee.created_at as payload_forkee_created_at,
        payload.pull_request.id as payload_pull_request_id,
        payload.pull_request.url as payload_pull_request_url,
        payload.pull_request.number as payload_pull_request_number,
        payload.pull_request.title as payload_pull_request_title,
        payload.pull_request.user.id as payload_pull_request_user_id,
        payload.pull_request.user.login as payload_pull_request_user_login,
        payload.repository_id as payload_repository_id,
        payload.push_id as payload_push_id,
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
                        payload_forkee_id: 'VARCHAR',
                        payload_forkee_name: 'VARCHAR',
                        payload_forkee_full_name: 'VARCHAR',
                        payload_forkee_owner_login: 'VARCHAR',
                        payload_forkee_created_at: 'VARCHAR',
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