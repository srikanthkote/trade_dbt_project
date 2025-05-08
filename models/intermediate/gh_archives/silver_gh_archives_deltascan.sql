{{
    config(
        materialized = 'table'
    )
}}
-- depends_on: {{ ref('silver_gh_archives_deltawrite') }}
SELECT  id,
        type,
        actor_id, 
        actor_login, 
        actor_display_login, 
        actor_gravatar_id, 
        actor_url, 
        actor_avatar_url, 
        repo_id, 
        repo_name, 
        repo_url, 
        payload_forkee_id,
        payload_forkee_name,
        payload_forkee_full_name,
        payload_forkee_owner_login,
        payload_forkee_created_at,
        payload_pull_request_id,
        payload_pull_request_url,
        payload_pull_request_number,
        payload_pull_request_title,
        payload_pull_request_user_id,
        payload_pull_request_user_login,
        payload_repository_id,
        payload_push_id,        
        created_at,
        public, 
        org_id,
        org_login,
        org_gravatar_id,
        org_url,
        org_avatar_url,
        now() AS ingest_timestamp
    FROM 
        delta_scan('{{ var('storage_location') }}/silver/delta_table')

