{% macro bronze_spotify_playlists_table() %}

CREATE TABLE IF NOT EXISTS "trade-statistics".main.bronze_spotify_playlists
(
  "name" VARCHAR, 
  collaborative VARCHAR, 
  pid BIGINT, 
  modified_at BIGINT, 
  num_tracks BIGINT, 
  num_albums BIGINT, 
  num_followers BIGINT, 
  tracks STRUCT(album_name VARCHAR, album_uri VARCHAR, artist_name VARCHAR, artist_uri VARCHAR, duration_ms BIGINT, pos BIGINT, track_name VARCHAR, track_uri VARCHAR)[], 
  num_edits BIGINT, 
  duration_ms BIGINT, 
  num_artists BIGINT, 
  description VARCHAR, 
  ingest_timestamp TIMESTAMP WITH TIME ZONE
)  

{% endmacro %}
