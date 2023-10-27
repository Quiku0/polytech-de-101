import os
import logging


def run_github_graphql_query(data, graphql_input_variables={}):
    import requests

    headers = {"Authorization": f"token {os.getenv('GITHUB_TOKEN')}"}

    url = 'https://api.github.com/graphql'

    request_data = {
        "query": data,
        "variables": graphql_input_variables
    }

    try:
        response = requests.post(url=url, data=request_data, headers=headers)
    except Exception as e:
        logging.error("An error occured during the acquisition.")
        raise e
        #doing more stuff is necessary
    finally:
        return response


def run_github_rest_query(endpoint, params={}):
    import requests

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    url = f"https://api.github.com/{endpoint}"

    try:
        response = requests.get(url=url, params=params, headers=headers)
    except Exception as e:
        raise e
        #doing more stuff is necessary
    finally:
        return response
    

def run_ready_github_rest_query(url, params={}):
    import requests

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    try:
        response = requests.get(url=url, params=params, headers=headers)
    except Exception as e:
        raise e
        #doing more stuff is necessary
    finally:
        return response


def push_data_to_datalake(data: str, bucket_name: str, file_key: str) -> None:
    import boto3
    import botocore

    session = boto3.session.Session()
    client = session.client(
        's3',
        endpoint_url=os.getenv("DATALAKE_ENDPOINT"),
        config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
        region_name='nyc3',
        aws_access_key_id=os.getenv("DATALAKE_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("DATALAKE_SECRET_TOKEN")
    )

    client.put_object(Bucket=bucket_name,
        Key=file_key,
        Body=data.encode('utf-8'),
        ACL='public-read',
    )

def pull_data_from_datalake(bucket_name: str, file_key: str) -> str:
    import boto3
    import botocore

    session = boto3.session.Session()
    client = session.client(
        's3',
        endpoint_url=os.getenv("DATALAKE_ENDPOINT"),
        config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
        region_name='nyc3',
        aws_access_key_id=os.getenv("DATALAKE_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("DATALAKE_SECRET_TOKEN")
    )

    response = client.get_object(Bucket=bucket_name, Key=file_key)
    return response["Body"].read().decode("utf-8")


def execute_query_on_warehouse(sql_query: str):

    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine

    url = URL.create(
        "postgresql",
        username=os.getenv("WAREHOUSE_USERNAME"),
        password=os.getenv("WAREHOUSE_PASSWORD"),
        host=os.getenv("WAREHOUSE_HOST"),
        database=os.getenv("WAREHOUSE_DATABASE"),
        port=os.getenv("WAREHOUSE_PORT")
    )

    engine = create_engine(url)

    with engine.connect() as conn:
        conn.execute(sql_query)


def insert_df_in_warehouse(
        df, 
        table_name: str, 
        schema_name: str, 
        columns: list = [], 
        on_conflict_key: list = [],
        on_conflict_update: list = []
    ):

    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine, text

    url = URL.create(
        "postgresql",
        username=os.getenv("WAREHOUSE_USERNAME"),
        password=os.getenv("WAREHOUSE_PASSWORD"),
        host=os.getenv("WAREHOUSE_HOST"),
        database=os.getenv("WAREHOUSE_DATABASE"),
        port=os.getenv("WAREHOUSE_PORT")
    )

    engine = create_engine(url)

    with engine.connect() as conn:
        table_name_tmp = f"{table_name}_tmp"
        df.to_sql(table_name_tmp, conn, schema=schema_name, if_exists="replace", index=False)
        insert_statement = f"""
            INSERT INTO {schema_name}.{table_name} ({','.join(columns)})
            SELECT {','.join(columns)}
            FROM {schema_name}.{table_name_tmp}
            ON CONFLICT ({','.join(on_conflict_key)}) DO UPDATE SET
        """
        for i in range(len(on_conflict_update)):
            insert_statement = insert_statement + f'"{on_conflict_update[i]}" = EXCLUDED."{on_conflict_update[i]}"{"," if i != len(on_conflict_update)-1 else ""}\n'
        print(insert_statement)
        conn.execute(text(insert_statement))
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name_tmp};"))
        conn.execute(text("COMMIT;"))


def select_df_from_sql_query(sql_query: str):

    import pandas as pd
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine

    url = URL.create(
        "postgresql",
        username=os.getenv("WAREHOUSE_USERNAME"),
        password=os.getenv("WAREHOUSE_PASSWORD"),
        host=os.getenv("WAREHOUSE_HOST"),
        database=os.getenv("WAREHOUSE_DATABASE"),
        port=os.getenv("WAREHOUSE_PORT")
    )

    engine = create_engine(url)

    with engine.connect() as conn:
        return pd.read_sql_query(sql=sql_query, con=conn)
    

def select_data_from_query(sql_query: str):

    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine, text

    url = URL.create(
        "postgresql",
        username=os.getenv("WAREHOUSE_USERNAME"),
        password=os.getenv("WAREHOUSE_PASSWORD"),
        host=os.getenv("WAREHOUSE_HOST"),
        database=os.getenv("WAREHOUSE_DATABASE"),
        port=os.getenv("WAREHOUSE_PORT")
    )

    engine = create_engine(url)

    with engine.connect() as conn:
        return conn.execute(text(sql_query))
    

def execute_query_in_warehouse(sql_query: str):

    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine, text

    url = URL.create(
        "postgresql",
        username=os.getenv("WAREHOUSE_USERNAME"),
        password=os.getenv("WAREHOUSE_PASSWORD"),
        host=os.getenv("WAREHOUSE_HOST"),
        database=os.getenv("WAREHOUSE_DATABASE"),
        port=os.getenv("WAREHOUSE_PORT")
    )

    engine = create_engine(url)

    with engine.connect() as conn:
        conn.execute(text(sql_query))