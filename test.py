import snowflake.connector as sf
import os
from dotenv import load_dotenv

load_dotenv()

conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cur = conn.cursor()
cur.execute("SELECT CURRENT_VERSION()")
print("✅ Snowflake connected. Version:", cur.fetchone())
cur.close()
conn.close()
