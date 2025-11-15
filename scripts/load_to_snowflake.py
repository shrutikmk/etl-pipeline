import os
import uuid
import time
import csv
import re
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector as sf

load_dotenv()

ACCOUNT=os.getenv("SNOWFLAKE_ACCOUNT")
USER=os.getenv("SNOWFLAKE_USER")
PASSWORD=os.getenv("SNOWFLAKE_PASSWORD")
ROLE=os.getenv("SNOWFLAKE_ROLE")
WAREHOUSE=os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE=os.getenv("SNOWFLAKE_DATABASE")
SCHEMA_ANALYTICS=os.getenv("SNOWFLAKE_SCHEMA_ANALYTICS","ANALYTICS").upper()

PROCESSED_DIR=os.path.join("data","processed")
LOGS_DIR=os.path.join("logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOAD_LOG_PATH=os.path.join(LOGS_DIR,"load_metrics.csv")

TABLE_FILES={
    "DIM_CUSTOMERS":"dim_customers.csv",
    "DIM_ACCOUNTS":"dim_accounts.csv",
    "DIM_SECURITIES":"dim_securities.csv",
    "FACT_TRANSACTIONS":"fact_transactions.csv",
    "ACCOUNT_DAILY_VALUE":"account_daily_value.csv",
    "CUSTOMER_DAILY_VALUE":"customer_daily_value.csv"
}

DATE_NAME_HINTS={"date","transaction_date","trade_date","as_of_date","effective_date","posted_date","settlement_date","valuation_date"}
TS_NAME_HINTS={"timestamp","created_at","updated_at","ingested_at","txn_ts"}
DATE_REGEX=re.compile(r"^\d{4}-\d{2}-\d{2}$")
TS_REGEX=re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}")
SCHEMA_HINTS={
    "FACT_TRANSACTIONS":{"TRANSACTION_DATE":"DATE"},
    "ACCOUNT_DAILY_VALUE":{"DATE":"DATE"},
    "CUSTOMER_DAILY_VALUE":{"DATE":"DATE"}
}

def map_dtype_to_snowflake(series: pd.Series) -> str:
    name=(series.name or "").lower()
    non_null=series.dropna().astype(str).head(50)
    if name in DATE_NAME_HINTS and len(non_null) and all(bool(DATE_REGEX.match(v)) for v in non_null):
        return "DATE"
    if name in TS_NAME_HINTS or (len(non_null) and any(bool(TS_REGEX.match(v)) for v in non_null)):
        return "TIMESTAMP_NTZ"
    if pd.api.types.is_integer_dtype(series):
        return "NUMBER(38,0)"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if len(non_null) and all(bool(DATE_REGEX.match(v)) for v in non_null):
        return "DATE"
    if len(non_null) and any(bool(TS_REGEX.match(v)) for v in non_null):
        return "TIMESTAMP_NTZ"
    return "VARCHAR"

def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def build_ddl(df: pd.DataFrame, full_name: str) -> str:
    table_name=full_name.split(".")[-1].upper()
    hints=SCHEMA_HINTS.get(table_name,{})
    cols=[]
    for c in df.columns:
        col_upper=c.upper()
        dtype=hints.get(col_upper) or map_dtype_to_snowflake(df[c])
        cols.append(f'"{col_upper}" {dtype}')
    cols_sql=", ".join(cols)
    return f'CREATE OR REPLACE TABLE {full_name} ({cols_sql});'

def open_conn():
    kwargs={"account":ACCOUNT,"user":USER,"password":PASSWORD,"warehouse":WAREHOUSE,"database":DATABASE}
    if ROLE and ROLE.strip():
        kwargs["role"]=ROLE
    return sf.connect(**kwargs)

def ensure_stage_and_format(cur, database, schema):
    cur.execute(f'USE DATABASE {database}')
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    cur.execute(f'USE SCHEMA {schema}')
    cur.execute('CREATE STAGE IF NOT EXISTS LOAD_STAGE')

def put_file(cur, local_path: str, stage_prefix: str):
    cur.execute(f"PUT file://{os.path.abspath(local_path)} @LOAD_STAGE/{stage_prefix} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

def truncate_table(cur, full_name: str):
    cur.execute(f"TRUNCATE TABLE {full_name}")

def copy_into(cur, full_name: str, stage_prefix: str):
    cur.execute(
        f"""COPY INTO {full_name}
            FROM @LOAD_STAGE/{stage_prefix}
            FILE_FORMAT=(
              TYPE=CSV
              PARSE_HEADER=TRUE
              FIELD_OPTIONALLY_ENCLOSED_BY='\"'
              NULL_IF=('','NULL')
              EMPTY_FIELD_AS_NULL=TRUE
              TRIM_SPACE=TRUE
            )
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            ON_ERROR='ABORT_STATEMENT'
            FORCE=TRUE"""
    )

def count_rows(cur, full_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {full_name}")
    return cur.fetchone()[0]

def append_log(row: dict):
    write_header=not os.path.exists(LOAD_LOG_PATH)
    with open(LOAD_LOG_PATH,"a",newline="") as f:
        w=csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            w.writeheader()
        w.writerow(row)

def run():
    run_id=str(uuid.uuid4())
    started=dt.datetime.now(dt.timezone.utc)
    conn=open_conn()
    cur=conn.cursor()
    ensure_stage_and_format(cur, DATABASE, SCHEMA_ANALYTICS)
    for tname, filename in TABLE_FILES.items():
        local_path=os.path.join(PROCESSED_DIR, filename)
        if not os.path.exists(local_path):
            continue
        df=read_csv(local_path)
        full_name=f'{DATABASE}.{SCHEMA_ANALYTICS}.{tname}'
        ddl=build_ddl(df, full_name)
        cur.execute(ddl)
        stage_prefix=f'{run_id}/{tname}'
        src_rows=len(df)
        t0=time.time()
        status="success"
        error=""
        tgt_rows=-1
        try:
            put_file(cur, local_path, stage_prefix)
            truncate_table(cur, full_name)
            copy_into(cur, full_name, stage_prefix)
            tgt_rows=count_rows(cur, full_name)
            if tgt_rows!=src_rows:
                status="row_mismatch"
        except Exception as e:
            status="failed"
            error=str(e)
        duration=time.time()-t0
        ended=dt.datetime.now(dt.timezone.utc)
        log_row={
            "run_id":run_id,
            "table_name":tname,
            "file_name":filename,
            "source_rows":src_rows,
            "target_rows":tgt_rows,
            "status":status,
            "error":error,
            "started_at_utc":started.isoformat(),
            "ended_at_utc":ended.isoformat(),
            "duration_seconds":round(duration,3)
        }
        append_log(log_row)
    cur.close()
    conn.close()

if __name__=="__main__":
    run()
    print('All done!')