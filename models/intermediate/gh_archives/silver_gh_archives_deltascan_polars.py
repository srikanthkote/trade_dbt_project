import polars as pl


def model(dbt, session):
    dbt.ref("silver_gh_archives_deltawrite")
    storage_location = str(dbt.config.get("storage_location")) + "/silver/delta_table"

    # read_delta by defualt reads latest version, but we can specify a particular version
    # efficiently time travel to different versions of an existing Delta Table
    df = pl.read_delta(storage_location, version=0)

    return df
