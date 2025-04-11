{{
    config(
        materialized = 'table'
    )
}}

WITH playlist_counts AS (
    SELECT COUNT(*) AS total_playlists
    FROM {{ ref('silver_spotify_playlists') }}
),

taylor_swift_playlists AS (
    SELECT COUNT(DISTINCT t.pid) AS taylor_swift_playlists
    FROM  {{ ref('silver_spotify_tracks') }} t
    WHERE t.artist_name ILIKE '%Taylor Swift%'
)

SELECT 
    *,   
    taylor_swift_playlists / total_playlists as taylor_swift_playlist_ratio
FROM playlist_counts, taylor_swift_playlists