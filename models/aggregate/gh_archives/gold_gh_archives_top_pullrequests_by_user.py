from time import perf_counter_ns


# This model processes data from the "gold_gh_archives_daily" upstream model to identify the top 20 users
def model(dbt, session):
    model = "gold_gh_archives_top_pullrequests_by_user"

    dbt.config(materialized="table")
    # DataFrame representing an upstream model
    upstream_model = dbt.ref("gold_gh_archives_daily").df()
    print("#TOTAL[" + str(len(upstream_model)) + "]")

    t1_start = perf_counter_ns()

    # filter out pull requests
    pull_requests_model = upstream_model.query("type == 'PullRequestEvent'")

    # group by actor_id and actor_display_login and count the number of events
    pull_requests_model = (
        pull_requests_model.groupby(["type", "actor_display_login", "actor_id"])
        .size()
        .reset_index(name="counts")
        .sort_values(["counts"], ascending=False)
        .head(20)
    )

    pull_requests_model = pull_requests_model.reset_index()[
        ["type", "actor_id", "actor_display_login", "counts"]
    ]

    t1_stop = perf_counter_ns()
    elapsed = round((t1_stop - t1_start) / 1000000, 3)
    print(f"{model} - {elapsed}ms")

    return pull_requests_model
