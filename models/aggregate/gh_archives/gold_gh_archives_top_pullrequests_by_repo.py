# This model processes data from the "gold_gh_archives_daily" upstream model to identify the top 20 repos with highest PRs
def model(dbt, session):
    dbt.config(materialized="table")
    # DataFrame representing an upstream model
    upstream_model = dbt.ref("gold_gh_archives_daily").df()
    # filter out pull requests
    pull_requests_model = upstream_model.query("type == 'PullRequestEvent'")

    # group by actor_id and actor_display_login and count the number of events
    pull_requests_model = pull_requests_model.groupby(['type','repo_id', 'repo_name']).size().reset_index(name='counts').sort_values(['counts'], ascending=False).head(20)
    
    pull_requests_model = pull_requests_model.reset_index()[["type", "repo_id", "repo_name", "counts"]]

    return pull_requests_model