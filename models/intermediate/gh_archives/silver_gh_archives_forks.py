from deltalake import DeltaTable, write_deltalake
from datetime import datetime
from dateutil import parser


def model(dbt, session):

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_gh_archives_deltascan_polars").df()
    filtered_df = upstream_model.query('type == "ForkEvent"')
    print("#ForkEvents[" + str(len(filtered_df)) + "]")
    filtered_df = filtered_df.filter(
        items=[
            "id",
            "type",
            "actor_id",
            "actor_display_login",
            "repo_id",
            "repo_name",
            "payload_forkee_id",
            "payload_forkee_name",
            "payload_forkee_full_name",
            "payload_forkee_owner_login",
            "payload_forkee_created_at",
            "created_at",
            "org_id",
            "org_login",
            "org_gravatar_id",
            "org_url",
            "org_avatar_url",
            "ingest_timestamp",
        ]
    )

    return filtered_df
