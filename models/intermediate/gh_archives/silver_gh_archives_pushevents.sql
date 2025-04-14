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
        payload.repository_id as payload_repository_id,
        payload.push_id as payload_push_id,
        payload.size as payload_size,
        payload.distinct_size as payload_distinct_size,
        payload.ref as payload_ref,
        payload.ref_full as payload_ref_full,
        payload.head as payload_head,
        payload.before as payload_before,
        UNNEST(from_json(payload.commits, '["JSON"]')) as payload_commits,
        public, 
        created_at, 
        org_id,
        org_login,
        org_gravatar_id,
        org_url,
        org_avatar_url,
        now() AS ingest_timestamp
    FROM 
        {{ ref('silver_gh_archives_daily') }}
    WHERE 
        type = 'PushEvent'