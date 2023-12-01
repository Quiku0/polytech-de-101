import json
from datetime import datetime

import pandas as pd
from dagster import asset, get_dagster_logger

from .utils import (
    run_github_rest_query,
    push_data_to_datalake,
    pull_data_from_datalake,
    insert_df_in_warehouse
)

@asset()
def acquisition_repositories(**kwargs):
    
    data = []
    logger = get_dagster_logger("acquisition_repositories")
    github_search_query = "stars:>1000"

    for i in range(1, 11):

        logger.info(f"Trying to fetch page {i}")
        response = run_github_rest_query(endpoint="search/repositories", params={"q": github_search_query, "sort": "stars", "order": "desc","page": i, "per_page": 100})
        data = data + response.json()["items"]


    now = datetime.today()
    push_data_to_datalake(
        data=json.dumps(data),
        bucket_name="datalake-polytech-de-101",
        file_key=f"maxenceb/acquisition/repositories/repositories_{now.year}_{now.month}_{now.day}.json"
    )





@asset(deps=[acquisition_repositories])
def consolidate_repositories_data(**kwargs):
    now = datetime.today()
    
    raw_data = pull_data_from_datalake(
        bucket_name="datalake-polytech-de-101",
        file_key=f"maxenceb/acquisition/repositories/repositories_{now.year}_{now.month}_{now.day}.json"
    )
    
    repositories_data = json.loads(raw_data)

    df = pd.json_normalize(repositories_data)

    df = df[[
        "id", 
        "commits_url",
        "contributors_url",
        "issues_url",
        "score",
        "pulls_url",
        "forks_url",
        "url",
        "created_at",
        "updated_at", 
        "description", 
        "comments_url", 
        "owner.login", 
        "owner.id"
    ]]  
    df.drop_duplicates(subset=["id"], inplace=True)

    insert_df_in_warehouse(
        df, 
        "consolidate_repositories", 
        "consolidate",
        columns=[
            "id", 
            "commits_url",
            "contributors_url",
            "issues_url",
            "score",
            "pulls_url",
            "forks_url",
            "url",
            "created_at",
            "updated_at", 
            "description", 
            "comments_url", 
            '"owner.login"', 
            '"owner.id"'
        ],
        on_conflict_key=["id"], 
        on_conflict_update=["description"]
    )
