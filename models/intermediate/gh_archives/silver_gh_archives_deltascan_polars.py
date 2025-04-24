import polars as pl

def model(dbt, session):
    dbt.ref('silver_gh_archives_deltawrite')
    storage_location = str(dbt.config.get("storage_location")) + 'silver/delta_table'
    df = pl.read_delta(storage_location)
    return df