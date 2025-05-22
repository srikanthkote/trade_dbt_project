from deltalake import DeltaTable, write_deltalake
from datetime import datetime
from dateutil import parser
from time import perf_counter_ns

def model(dbt, session):
    model = "silver_gh_archives_pullrequests"

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_gh_archives_deltascan_polars").df()

    t1_start = perf_counter_ns()

    filtered_df = upstream_model.query('type == "PullRequestEvent"')
    print("#PullRequestEvents[" + str(len(filtered_df)) + "]")
    filtered_df = filtered_df.filter(items=["id", "type", "actor_id", "actor_display_login", "repo_id", "repo_name", "payload_pull_request_id", "payload_pull_request_name", "payload_pull_request_number", "payload_pull_request_title", "payload_pull_request_user_id", "payload_pull_request_user_login", "created_at", "org_id", "org_login", "org_gravatar_id", "org_url", "org_avatar_url", "ingest_timestamp"])

    t1_stop = perf_counter_ns()
    elapsed = round((t1_stop - t1_start)/1000000, 3)
    print(f"{model} - {elapsed}ms")

    return filtered_df