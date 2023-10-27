import json

import pandas as pd
from dagster import asset, get_dagster_logger

from .utils import (
    run_github_rest_query,
    push_data_to_datalake,
    pull_data_from_datalake,
    insert_df_in_warehouse
)

@asset()
def acquisition_gists(**kwargs):
    
    data = []
    logger = get_dagster_logger("acquisition_gists")

    for i in range(1, 31):
        logger.info(f"Trying to fetch page {i}")
        response = run_github_rest_query(endpoint="gists/public", params={"page": i, "per_page": 100})
        data = data + response.json()
    
    push_data_to_datalake(
        data=json.dumps(data),
        bucket_name="datalake-polytech-de-101",
        file_key="kevinl/acquisition/gists/gists_2023_10_10.json"
    )


@asset(deps=[acquisition_gists])
def consolidate_gists_files_data(**kwargs):
    raw_data = pull_data_from_datalake(
        bucket_name="datalake-polytech-de-101",
        file_key="kevinl/acquisition/gists/gists_2023_10_10.json"
    )

    gists_data = json.loads(raw_data)

    result = []
    for item in gists_data: 
        for _, file in item["files"].items():
            file["id_gist"] = item["id"]
            result = result + [file]
    
    df = pd.DataFrame(result)
    df.drop_duplicates(subset=["id_gist", "filename"], inplace=True)

    insert_df_in_warehouse(
        df, 
        "consolidate_gist_files", 
        "consolidate",
        columns=["id_gist", "filename", "type","language", "raw_url", "size"],
        on_conflict_key=["id_gist", "filename"], 
        on_conflict_update=["type","language", "raw_url", "size"]
    )



@asset(deps=[acquisition_gists])
def consolidate_gists_data(**kwargs):
    raw_data = pull_data_from_datalake(
        bucket_name="datalake-polytech-de-101",
        file_key="kevinl/acquisition/gists/gists_2023_10_10.json"
    )
    
    gists_data = json.loads(raw_data)

    df = pd.json_normalize(gists_data)

    df = df[[
        "id", 
        "commits_url",
        "comments_url",
        "url",
        "public",
        "created_at",
        "updated_at", 
        "description", 
        "comments", 
        "owner.login", 
        "owner.id"
    ]]  
    df.drop_duplicates(subset=["id"], inplace=True)

    insert_df_in_warehouse(
        df, 
        "consolidate_gists", 
        "consolidate",
        columns=[
            "id", 
            "commits_url",
            "comments_url",
            "url",
            "public",
            "created_at",
            "updated_at", 
            "description", 
            "comments", 
            '"owner.login"', 
            '"owner.id"'
        ],
        on_conflict_key=["id"], 
        on_conflict_update=["description","comments"]
    )
