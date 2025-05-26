import pyarrow as pa
import duckdb as duck


def model(dbt, session):

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_gh_archives_dailyload").df()

    # Convert to Arrow Table
    table = pa.Table.from_pandas(upstream_model)
    return table
