{{
    config(
        materialized = 'view'
    )
}}

SELECT {{ dbt_utils.generate_surrogate_key(['name', 'pid', 'UNNEST(tracks).track_uri', 'UNNEST(tracks).duration_ms']) }} AS surr_key,
        pid, 
        UNNEST(tracks, recursive := true), 
        now() AS ingest_timestamp
    FROM {{ ref('bronze_spotify_playlists') }}