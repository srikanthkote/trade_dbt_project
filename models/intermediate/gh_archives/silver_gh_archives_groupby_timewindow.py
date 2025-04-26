import polars as pl

def model(dbt, session):

    # DataFrame representing an upstream model
    df = dbt.ref("silver_gh_archives_deltascan_polars").df()

    out = pl.from_pandas(df).sort("created_at", descending=False).group_by_dynamic(
        "created_at",
        every="1h",
        closed="both",
        group_by="type",
        include_boundaries=True,
    ).agg(pl.len())

    return out