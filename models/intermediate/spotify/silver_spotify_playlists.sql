{{
    config(
        materialized = 'view'
    )
}}

SELECT * Exclude('tracks'), now() AS ingest_timestamp
    FROM {{ ref('bronze_spotify_playlists') }}
