{{
    config(
        materialized = 'table'
    )
}}

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
        payload.forkee.id as payload_forkee_id,
        payload.forkee.name as payload_forkee_name,
        payload.forkee.full_name as payload_forkee_full_name,
        payload.forkee.owner.login as payload_forkee_owner_login,
        payload.forkee.created_at as payload_forkee_created_at,
        created_at,
        public, 
        org_id,
        org_login,
        org_gravatar_id,
        org_url,
        org_avatar_url,
        now() AS ingest_timestamp
    FROM 
        {{ ref('silver_gh_archives_daily') }}
    WHERE 
        type = 'ForkEvent'
