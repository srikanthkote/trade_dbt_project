from deltalake import DeltaTable, write_deltalake
from datetime import datetime
from dateutil import parser


def model(dbt, session):

    # DataFrame representing an upstream model
    upstream_model = dbt.ref("silver_gh_archives_deltascan_polars").df()
    filtered_df = upstream_model.query('type == "PushEvent"')

    filtered_df = filtered_df.filter(
        items=[
            "id",
            "type",
            "actor_id",
            "actor_display_login",
            "repo_id",
            "repo_name",
            "payload_repository_id",
            "payload_push_id",
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
