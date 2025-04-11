def model(dbt, session):

    dbt.config(materialized="table")

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_spotify_tracks").df()

    order_payments_df = upstream_model.groupby(['artist_name','album_name', 'track_name']).count().sort_values(['pid'], ascending=False).head(20)
    order_payments_df = order_payments_df.reset_index()[["artist_name", "album_name", "track_name", "pid"]]

    return order_payments_df