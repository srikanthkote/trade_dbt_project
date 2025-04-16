import duckdb
from deltalake import DeltaTable, write_deltalake
from datetime import datetime
from dateutil import parser

def model(dbt, session):

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_gh_archives_daily").df()

    con = duckdb.connect()
    storage_location = str(dbt.config.get("storage_location")) + 'silver/delta_table'

    delta_model = write_deltalake(
        storage_location,
        upstream_model,
        mode="overwrite",
        partition_by=["created_at_dt"]
    )
    return upstream_model