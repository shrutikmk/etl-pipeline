import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(ROOT, "data", "processed")
LOGS_DIR = os.path.join(ROOT, "logs")
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

def read_csv(path, parse_dates=None):
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    if parse_dates:
        for c in parse_dates:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df

def to_float(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def upper(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()
    return df

def lower(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower()
    return df

def strip_all(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def write_csv(df, path):
    df.to_csv(path, index=False)

def add_surrogate_keys(df, id_col, key_name):
    unique_ids = df[[id_col]].drop_duplicates().reset_index(drop=True)
    unique_ids[key_name] = np.arange(1, len(unique_ids) + 1, dtype=int)
    return df.merge(unique_ids, on=id_col, how="left"), unique_ids

def ensure_cols(df, cols):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))

metrics = []
dq_issues = []

customers = read_csv(os.path.join(RAW_DIR, "customers.csv"), parse_dates=["created_at"])
accounts = read_csv(os.path.join(RAW_DIR, "accounts.csv"), parse_dates=["opened_at"])
securities = read_csv(os.path.join(RAW_DIR, "securities.csv"))
transactions = read_csv(os.path.join(RAW_DIR, "transactions.csv"), parse_dates=["trade_date","settle_date"])
positions = read_csv(os.path.join(RAW_DIR, "positions.csv"), parse_dates=["as_of_date"])
market_data_path = os.path.join(RAW_DIR, "market_data.csv")
market_data = read_csv(market_data_path, parse_dates=["as_of_date"]) if os.path.exists(market_data_path) else pd.DataFrame()

customers = strip_all(customers, customers.columns.tolist())
accounts = strip_all(accounts, accounts.columns.tolist())
securities = strip_all(securities, securities.columns.tolist())
transactions = strip_all(transactions, transactions.columns.tolist())
positions = strip_all(positions, positions.columns.tolist())
if not market_data.empty:
    market_data = strip_all(market_data, market_data.columns.tolist())

upper(securities, ["ticker","exchange"])
lower(customers, ["status"])
lower(accounts, ["status","account_type","currency"])
upper(transactions, ["currency"])
upper(positions, ["currency"])
if not market_data.empty:
    upper(market_data, ["ticker"])

to_float(transactions, ["quantity","price","amount"])
to_float(positions, ["quantity","avg_cost","market_price","market_value"])

ensure_cols(customers, ["customer_id","first_name","last_name","email","created_at","status"])
ensure_cols(accounts, ["account_id","customer_id","account_type","opened_at","status","currency"])
ensure_cols(securities, ["security_id","ticker","name","asset_class","cusip","exchange"])
ensure_cols(transactions, ["transaction_id","account_id","security_id","transaction_type","quantity","price","amount","trade_date","settle_date","currency"])
ensure_cols(positions, ["as_of_date","account_id","security_id","quantity","avg_cost","market_price","market_value","currency"])

valid_customer_status = {"active","inactive"}
valid_account_status = {"active","inactive"}
valid_account_type = {"brokerage","ira","roth","trust"}
valid_asset_class = {"equity","etf","bond","cash"}
valid_txn_type = {"buy","sell","dividend","interest","deposit","withdrawal","fee"}

pre_rows = len(securities)
securities = securities[securities["asset_class"].isin(valid_asset_class)]
dq_issues.append({"rule":"securities.asset_class_enum","dropped": pre_rows - len(securities)})

pre_rows = len(accounts)
accounts = accounts[accounts["account_type"].isin(valid_account_type) & accounts["status"].isin(valid_account_status)]
dq_issues.append({"rule":"accounts.enums","dropped": pre_rows - len(accounts)})

pre_rows = len(customers)
customers = customers[customers["status"].isin(valid_customer_status)]
dq_issues.append({"rule":"customers.status_enum","dropped": pre_rows - len(customers)})

pre_rows = len(transactions)
transactions = transactions[transactions["transaction_type"].isin(valid_txn_type)]
dq_issues.append({"rule":"transactions.transaction_type_enum","dropped": pre_rows - len(transactions)})

pre_rows = len(transactions)
transactions = transactions[(transactions["quantity"].isna()) | (transactions["quantity"] >= 0)]
dq_issues.append({"rule":"transactions.quantity_nonnegative","dropped": pre_rows - len(transactions)})

pre_rows = len(transactions)
transactions = transactions[(transactions["price"].isna()) | (transactions["price"] >= 0)]
dq_issues.append({"rule":"transactions.price_nonnegative","dropped": pre_rows - len(transactions)})

pre_rows = len(positions)
positions = positions[(positions["quantity"] >= 0) & (positions["market_price"] >= 0) & (positions["market_value"] >= 0)]
dq_issues.append({"rule":"positions.nonnegative","dropped": pre_rows - len(positions)})

customers = customers.drop_duplicates(subset=["customer_id"])
accounts = accounts.drop_duplicates(subset=["account_id"])
securities = securities.drop_duplicates(subset=["security_id"])
transactions = transactions.drop_duplicates(subset=["transaction_id"])
positions = positions.drop_duplicates(subset=["as_of_date","account_id","security_id"])

customers, cust_key_map = add_surrogate_keys(customers, "customer_id", "customer_key")
accounts, acct_key_map = add_surrogate_keys(accounts, "account_id", "account_key")
securities, sec_key_map = add_surrogate_keys(securities, "security_id", "security_key")

accounts = accounts.merge(cust_key_map, on="customer_id", how="left", suffixes=("",""))
accounts["account_key"] = accounts["account_key"].astype("Int64")
customers["customer_key"] = customers["customer_key"].astype("Int64")
securities["security_key"] = securities["security_key"].astype("Int64")

txn = transactions.merge(acct_key_map, on="account_id", how="left")
txn = txn.merge(sec_key_map, on="security_id", how="left")
fact_transactions = txn[[
    "transaction_id","account_key","security_key","transaction_type",
    "quantity","price","amount","trade_date","settle_date","currency"
]].copy()

dim_customers = customers[[
    "customer_key","customer_id","first_name","last_name","email","created_at","status"
]].copy()

dim_accounts = accounts[[
    "account_key","account_id","customer_key","customer_id","account_type","opened_at","status","currency"
]].copy()

dim_securities = securities[[
    "security_key","security_id","ticker","name","asset_class","cusip","exchange"
]].copy()

account_daily_value = positions.merge(acct_key_map, on="account_id", how="left")
account_daily_value = account_daily_value.groupby(["as_of_date","account_key"], dropna=False, as_index=False)["market_value"].sum()
account_daily_value = account_daily_value.rename(columns={"market_value":"total_market_value"})

acct_to_cust = dim_accounts[["account_key","customer_key"]].drop_duplicates()
customer_daily_value = account_daily_value.merge(acct_to_cust, on="account_key", how="left")
customer_daily_value = customer_daily_value.groupby(["as_of_date","customer_key"], dropna=False, as_index=False)["total_market_value"].sum()
customer_daily_value = customer_daily_value.rename(columns={"total_market_value":"total_market_value"})

write_csv(dim_customers, os.path.join(PROCESSED_DIR, "dim_customers.csv"))
write_csv(dim_accounts, os.path.join(PROCESSED_DIR, "dim_accounts.csv"))
write_csv(dim_securities, os.path.join(PROCESSED_DIR, "dim_securities.csv"))
write_csv(fact_transactions, os.path.join(PROCESSED_DIR, "fact_transactions.csv"))
write_csv(account_daily_value, os.path.join(PROCESSED_DIR, "account_daily_value.csv"))
write_csv(customer_daily_value, os.path.join(PROCESSED_DIR, "customer_daily_value.csv"))

metrics.append({"table":"dim_customers","rows": len(dim_customers)})
metrics.append({"table":"dim_accounts","rows": len(dim_accounts)})
metrics.append({"table":"dim_securities","rows": len(dim_securities)})
metrics.append({"table":"fact_transactions","rows": len(fact_transactions)})
metrics.append({"table":"account_daily_value","rows": len(account_daily_value)})
metrics.append({"table":"customer_daily_value","rows": len(customer_daily_value)})

pd.DataFrame(metrics).to_csv(os.path.join(LOGS_DIR, "transform_metrics.csv"), index=False)
pd.DataFrame(dq_issues).to_csv(os.path.join(LOGS_DIR, "data_quality_report.csv"), index=False)

print("Transform complete. Outputs written to data/processed/.")
