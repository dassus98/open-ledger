import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

print("----------- DEBUGGING -----------")
print(f"User found: {'YES' if os.getenv('SNOWFLAKE_USER') else 'NO'}")
print(f"Password found: {'YES' if os.getenv('SNOWFLAKE_PASSWORD') else 'NO'}")
print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
print("---------------------------------")

try:
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='ANALYTICS'
    )
    print('Successfully connected to Snowflake.')
    conn.close()
except Exception as exception:
    print(f"Snowflake connection has failed: {exception}")