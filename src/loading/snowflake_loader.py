import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

def get_env_var(key):
    """Removing whitespace and quotes"""
    val = os.getenv(key)
    if val:
        return val.strip().strip("'").strip('"')
    return None

def get_snowflake_connection():
    user = get_env_var('SNOWFLAKE_USER')
    if not user:
        raise ValueError("ERROR: .env environment variables not loading.")
    
    return snowflake.connector.connect(
        user=get_env_var('SNOWFLAKE_USER'),
        password=get_env_var('SNOWFLAKE_PASSWORD'),
        account=get_env_var('SNOWFLAKE_ACCOUNT'),
        warehouse=get_env_var('SNOWFLAKE_WAREHOUSE'),
        database=get_env_var('SNOWFLAKE_DATABASE'),
        schema=get_env_var('SNOWFLAKE_SOURCE_SCHEMA'),
        role=get_env_var('SNOWFLAKE_ROLE')
    )

def load_table(conn, table_name, file_path):
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        cursor.execute(f"""
            CREATE OR REPLACE FILE FORMAT my_csv_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'None', '')
            EMPTY_FIELD_AS_NULL = TRUE;
        """)
        
        local_file_path = file_path.replace('\\', '/')
        stage_path = f"@~/openledger_staged/{table_name}"
        
        put_query = f"PUT file://{local_file_path} {stage_path} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cursor.execute(put_query)
        
        copy_query = f"""
            COPY INTO {table_name}
            FROM {stage_path}
            FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
            ON_ERROR = 'CONTINUE' 
            PURGE = TRUE;
        """
        cursor.execute(copy_query)
        
        print(f"{table_name} has been loaded.")
        
    except Exception as exception:
        print(f"ERROR: {exception}")
    finally:
        cursor.close()

def main():
    try:
        conn = get_snowflake_connection()
        
        files_to_load = {
            "TRANSACTIONS": "data/raw/transactions.csv",
            "LEDGER_ENTRIES": "data/raw/ledger_entries.csv",
            "SETTLEMENTS": "data/raw/settlements.csv"
        }
        
        for table, path in files_to_load.items():
            if not os.path.exists(path):
                print(f"ERROR: Missing file path {path}")
                continue
            
            abs_path = os.path.abspath(path)
            load_table(conn, table, abs_path)
            
    except Exception as exception:
        print(f"ERROR: {exception}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()