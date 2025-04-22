from deltalake import DeltaTable, write_deltalake
from datetime import datetime
from dateutil import parser

def model(dbt, session):

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_gh_archives_dailyload").df()
    
    forksdf = upstream_model.query('type == "ForkEvent"')
    prdf = upstream_model.query('type == "PullRequestEvent"')
    pushdf = upstream_model.query('type == "PushEvent"')

    print("#ForkEvents[" + str(len(forksdf)) + "], #PullRequestEvents[" + str(len(prdf)) + "], #PushEvents[" + str(len(pushdf)) + "], #TOTAL[" + str(len(upstream_model)) + "]")

    storage_location = str(dbt.config.get("storage_location")) + 'silver/delta_table'

    write_deltalake(
        storage_location,
        upstream_model,
        mode="overwrite",
        partition_by=["created_at_dt"]
    )
    return upstream_model