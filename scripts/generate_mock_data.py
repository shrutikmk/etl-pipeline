import os
import uuid
from datetime import datetime, timedelta
import random
import csv

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

def write_csv(path, rows, header):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def iso_days_ago(days):
    return (datetime.utcnow() - timedelta(days=days)).date().isoformat()

random.seed(7)

customers = []
for i in range(5):
    cid = f"CUST{i+1:03d}"
    customers.append({
        "customer_id": cid,
        "first_name": f"First{i+1}",
        "last_name": f"Last{i+1}",
        "email": f"user{i+1}@example.com",
        "created_at": iso_days_ago(400 - i * 20),
        "status": "active" if i % 4 != 0 else "inactive"
    })

accounts = []
acct_types = ["brokerage", "ira", "roth", "trust"]
for i, c in enumerate(customers, start=1):
    for j in range(1, 3):
        aid = f"ACCT{i:03d}{j:02d}"
        accounts.append({
            "account_id": aid,
            "customer_id": c["customer_id"],
            "account_type": random.choice(acct_types),
            "opened_at": iso_days_ago(365 - i * 10 - j),
            "status": "active",
            "currency": "USD"
        })

securities = []
tickers = [("AAPL","Apple Inc."),("MSFT","Microsoft Corp."),("AGG","iShares Core US Agg Bond ETF"),("VTI","Vanguard Total Stock Mkt"),("CASH","Cash")]
for i,(t,nm) in enumerate(tickers, start=1):
    sid = f"SEC{i:03d}"
    cls = "cash" if t=="CASH" else ("bond" if t=="AGG" else ("etf" if t in ["AGG","VTI"] else "equity"))
    securities.append({
        "security_id": sid,
        "ticker": t,
        "name": nm,
        "asset_class": cls,
        "cusip": f"000000{i:03d}",
        "exchange": "NASDAQ" if cls in ["equity","etf"] else "OTC"
    })

transactions = []
types = ["buy","sell","dividend","deposit","withdrawal","fee","interest"]
for a in accounts:
    for _ in range(random.randint(8, 15)):
        ttype = random.choice(types)
        sec = random.choice(securities)
        sec_id = None if ttype in ["deposit","withdrawal","fee","interest"] or sec["asset_class"]=="cash" else sec["security_id"]
        qty = 0.0 if sec_id is None else round(random.uniform(1, 50), 3)
        price = 0.0 if sec_id is None else round(random.uniform(10, 300), 2)
        amt = round(qty * price, 2) if sec_id is not None else round(random.uniform(10, 2000), 2) * (1 if ttype in ["deposit","interest","dividend"] else -1)
        trade = iso_days_ago(random.randint(1, 120))
        settle = trade
        transactions.append({
            "transaction_id": str(uuid.uuid4()),
            "account_id": a["account_id"],
            "security_id": sec_id,
            "transaction_type": ttype,
            "quantity": qty,
            "price": price,
            "amount": amt,
            "trade_date": trade,
            "settle_date": settle,
            "currency": "USD"
        })

positions = []
today = datetime.utcnow().date().isoformat()
for a in accounts:
    for s in [x for x in securities if x["asset_class"] != "cash"]:
        qty = round(random.uniform(0, 120), 3)
        price = round(random.uniform(10, 350), 2)
        mv = round(qty * price, 2)
        positions.append({
            "as_of_date": today,
            "account_id": a["account_id"],
            "security_id": s["security_id"],
            "quantity": qty,
            "avg_cost": round(price * random.uniform(0.7, 1.1), 2),
            "market_price": price,
            "market_value": mv,
            "currency": "USD"
        })

market_data = []
for t,_ in tickers:
    if t == "CASH":
        continue
    for d in range(30, -1, -1):
        market_data.append({
            "as_of_date": iso_days_ago(d),
            "ticker": t,
            "close": round(random.uniform(50, 350), 2),
            "volume": random.randint(1000000, 50000000)
        })

write_csv(os.path.join(RAW_DIR, "customers.csv"), customers, list(customers[0].keys()))
write_csv(os.path.join(RAW_DIR, "accounts.csv"), accounts, list(accounts[0].keys()))
write_csv(os.path.join(RAW_DIR, "securities.csv"), securities, list(securities[0].keys()))
write_csv(os.path.join(RAW_DIR, "transactions.csv"), transactions, list(transactions[0].keys()))
write_csv(os.path.join(RAW_DIR, "positions.csv"), positions, list(positions[0].keys()))
write_csv(os.path.join(RAW_DIR, "market_data.csv"), market_data, list(market_data[0].keys()))
print("Mock CSVs generated in data/raw/")
