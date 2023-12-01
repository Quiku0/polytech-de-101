#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 17:57:45 2023

@author: mbuisson
"""

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
def acquisition_repositories_commits(**kwargs):
    
    logger = get_dagster_logger("acquisition_repositories_commits")

    warehouse_sql_query = """
        SELECT id, commits_url 
        FROM consolidate.consolidate_repositories
        LIMIT 500;
    """

    warehouse_data = select_data_from_query(sql_query=warehouse_sql_query)

    data = []
    for row in warehouse_data:
        logger.info(f"Trying to fetch commits data for gist {row.id}")
        response = run_ready_github_rest_query(url=row.commits_url.replace("{/sha}",""), params={"per_page": 100})
        #logger.info(f"{response}")
        if response.status_code != 200:
            print(f"Error for url {row.commits_url}")
        else:
            #logger.info(f"{response.json()}")
            intermediate_data = response.json()
            for item in intermediate_data:
                item["id_repos"] = row.id
            data = data + intermediate_data
    
    now = datetime.today()
    #logger.info(f"{data}")
    push_data_to_datalake(
        data=json.dumps(data),
        bucket_name="datalake-polytech-de-101",
        file_key=f"maxenceb/acquisition/repositories_commits/repositories_commits_{now.year}_{now.month}_{now.day}.json"
    )


@asset(deps=[acquisition_repositories_commits])
def consolidate_repositories_commits_data(**kwargs):
    now = datetime.today()
   #logger = get_dagster_logger("acquisition_repositories_commits")
    raw_data = pull_data_from_datalake(
        bucket_name="datalake-polytech-de-101",
        file_key=f"maxenceb/acquisition/repositories_commits/repositories_commits_{now.year}_{now.month}_{now.day}.json"
    )

    repositories_commits_data = json.loads(raw_data)
    for item in repositories_commits_data:
        #logger.info(f"{item}")
        item.pop("author")
    
    df = pd.json_normalize(repositories_commits_data)
    df.drop_duplicates(subset=["id_repos", "node_id"], inplace=True)

    columns_name = ["node_id", "id_repos", '"commit.committer.date"', "url"]
    
    insert_df_in_warehouse(
        df,
        "consolidate_repositories_commits",
        "consolidate",
        columns=columns_name,
        on_conflict_key=["version", "id_repos"], 
        on_conflict_update=["change_status.deletions","change_status.additions", "change_status.total"]
    )
