import streamlit as st
import boto3
import pandas as pd
import time

ATHENA_DB = "nbanalytics"
S3_OUTPUT = "s3://nba-de/athena-results/"

def run_query(sql: str) -> pd.DataFrame:
    client = boto3.client("athena", region_name="ap-southeast-1")
    
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    execution_id = response["QueryExecutionId"]

    # Poll until done
    while True:
        status = client.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query {state}")

    # Paginate results
    paginator = client.get_paginator("get_query_results")
    rows, headers = [], None
    for page in paginator.paginate(QueryExecutionId=execution_id):
        result_rows = page["ResultSet"]["Rows"]
        if headers is None:
            headers = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([c.get("VarCharValue", "") for c in row["Data"]])

    return pd.DataFrame(rows, columns=headers)


@st.cache_data(ttl=3600)   # cache for 1 hour
def get_season_overview(season_id: str, season_type: str) -> pd.DataFrame:
    sql = f"""
        SELECT *
        FROM nbanalytics_gold.fct_season_overview
        WHERE season_id = '{season_id}'
          AND season_type = '{season_type}'
        ORDER BY rnk
    """
    df = run_query(sql)
    # Cast numeric columns
    # numeric_cols = ["wins", "losses", "pts_per_game", "opp_pts_per_game",
    #                 "net_rating", "pace", "ts_pct"]
    # for col in numeric_cols:
    #     if col in df.columns:
    #         df[col] = pd.to_numeric(df[col], errors="coerce")
    return df