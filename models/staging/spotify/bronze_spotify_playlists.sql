{{
    config(
        materialized = 'table'
    )
}}

SELECT * , now() AS ingest_timestamp
    FROM read_parquet({{ source('external_source', 'bronze_spotify_playlists') }})