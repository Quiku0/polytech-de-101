from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)

from . import (
    github_gists,
    github_gists_commits,
    github_gists_aggregation,
    github_repositories,
    github_repositories_commits
)

github_gists_assets = load_assets_from_modules([github_gists])
github_gists_commits_assets = load_assets_from_modules([github_gists_commits])
github_gists_aggregation_assets = load_assets_from_modules([github_gists_aggregation])
github_repositories_assets = load_assets_from_modules([github_repositories])
github_repositories_commits_assets = load_assets_from_modules([github_repositories_commits])



github_gists_job = define_asset_job("github_gists_job", selection=github_gists_assets)
github_gists_commits_job = define_asset_job("github_gists_commits_job", selection=github_gists_commits_assets)
github_gists_aggregation_job = define_asset_job("github_gists_aggregation_job", selection=github_gists_aggregation_assets)
github_repositories_job = define_asset_job("github_repositories_job", selection=github_repositories_assets)
github_repositories_commits_job = define_asset_job("github_repositories_commits_job", selection=github_repositories_commits_assets)


github_gists_schedule = ScheduleDefinition(
    job=github_gists_job,
    cron_schedule="0 12 * * *"
)

github_gists_commits_schedule = ScheduleDefinition(
    job=github_gists_commits_job,
    cron_schedule="0 12 * * *"
)

github_gists_aggregation_schedule = ScheduleDefinition(
    job=github_gists_aggregation_job,
    cron_schedule="0 12 * * *"
)

github_repositories_schedule = ScheduleDefinition(
    job=github_repositories_job,
    cron_schedule="0 12 * * *"
)

github_repositories_commits_schedule = ScheduleDefinition(
    job=github_repositories_commits_job,
    cron_schedule="0 12 * * *"
)

defs = Definitions(
    assets=github_gists_assets + github_gists_commits_assets + github_gists_aggregation_assets+github_repositories_assets+github_repositories_commits_assets,
    schedules=[
        github_gists_schedule,
        github_gists_commits_schedule,
        github_gists_aggregation_schedule,
        github_repositories_schedule,
        github_repositories_commits_schedule,
    ]
)
