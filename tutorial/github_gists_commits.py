import json
from datetime import datetime

import pandas as pd
from dagster import asset, get_dagster_logger

from .utils import (
    run_ready_github_rest_query,
    push_data_to_datalake,
    pull_data_from_datalake,
    insert_df_in_warehouse,
    select_data_from_query
)

@asset()
def acquisition_gists_commits(**kwargs):
    
    logger = get_dagster_logger("acquisition_gists_commits")

    warehouse_sql_query = """
        SELECT id, commits_url 
        FROM consolidate.consolidate_gists
        LIMIT 500;
    """

    warehouse_data = select_data_from_query(sql_query=warehouse_sql_query)

    data = []
    for row in warehouse_data:
        logger.info(f"Trying to fetch commits data for gist {row.id}")
        response = run_ready_github_rest_query(url=row.commits_url, params={"per_page": 100})
        if response.status_code != 200:
            print(f"Error for url {row.commits_url}")
        else:
            intermediate_data = response.json()
            for item in intermediate_data:
                item["id_gist"] = row.id
            data = data + intermediate_data
    
    now = datetime.today()
    push_data_to_datalake(
        data=json.dumps(data),
        bucket_name="datalake-polytech-de-101",
        file_key=f"kevinl/acquisition/gists_commits/gist_commits_{now.year}_{now.month}_{now.day}.json"
    )


@asset(deps=[acquisition_gists_commits])
def consolidate_gists_commits_data(**kwargs):
    now = datetime.today()

    raw_data = pull_data_from_datalake(
        bucket_name="datalake-polytech-de-101",
        file_key=f"kevinl/acquisition/gists_commits/gist_commits_{now.year}_{now.month}_{now.day}.json"
    )

    gists_commits_data = json.loads(raw_data)
    for item in gists_commits_data:
        item.pop("user")
    
    df = pd.json_normalize(gists_commits_data)
    df.drop_duplicates(subset=["id_gist", "version"], inplace=True)

    columns_name = ["version", "id_gist", "committed_at", "url", '"change_status.deletions"', '"change_status.additions"', '"change_status.total"']
    
    insert_df_in_warehouse(
        df,
        "consolidate_gist_commits",
        "consolidate",
        columns=columns_name,
        on_conflict_key=["version", "id_gist"], 
        on_conflict_update=["change_status.deletions","change_status.additions", "change_status.total"]
    )
