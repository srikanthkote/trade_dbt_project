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

    storage_location = str(dbt.config.get("storage_location")) + '/silver/delta_table'
    write_deltalake(
        storage_location,
        upstream_model,
        mode="overwrite",
        partition_by=["created_at_dt"]
    )

    new_df = upstream_model[['id', 'type', 'actor_id', 'actor_login', 'actor_display_login',
       'actor_gravatar_id', 'actor_url', 'actor_avatar_url', 'repo_id',
       'repo_name', 'repo_url', 'payload_forkee_id', 'payload_forkee_name',
       'payload_forkee_full_name', 'payload_forkee_owner_login',
       'payload_forkee_created_at', 'payload_pull_request_id',
       'payload_pull_request_url', 'payload_pull_request_number',
       'payload_pull_request_title', 'payload_pull_request_user_id',
       'payload_pull_request_user_login', 'payload_repository_id',
       'payload_push_id', 'public', 'created_at', 'created_at_dt', 'org_id',
       'org_login', 'org_gravatar_id', 'org_url', 'org_avatar_url',
       'created_day', 'created_month', 'created_year']] 

    # writing new dataframe to create new version, will use to test time travel    
    write_deltalake(
        storage_location,
        new_df,
        mode="append",
        partition_by=["created_at_dt"]
    )

    return upstream_model