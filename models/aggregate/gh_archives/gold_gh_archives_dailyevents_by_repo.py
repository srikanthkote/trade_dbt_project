def model(dbt, session):

    dbt.config(materialized="table")
    
    # DataFrame aggregate events by repo, materialise to table
    upstream_model = dbt.ref("gold_gh_archives_daily").df()

    # sort by repo and count
    upstream_model = upstream_model.sort_values(['count'], ascending=False).head(20)
    return upstream_model
