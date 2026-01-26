import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

def load_table(conn, table_name, file_path):
    cursor = conn.cursor()
    try:
        print(f'Loading {table_name}...')
        
        # Clearing existing data (Idempotency)
        # Replacing data for now, incremental appending in production would be wiser
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Creating a temporary file format for parsing CSVs
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT my_csv_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'None', '')
            EMPTY_FIELD_AS_NULL = TRUE;
        """)
        
        # Uploading file to the table stage
        local_file_path = file_path.replace('\\', '/') # Windows fix
        put_query = f'PUT file://{local_file_path} @%{table_name} AUTO_COMPRESS=TRUE'
        cursor.execute(put_query)
        
        # Copying from stage to table
        # on_error = 'CONTINUE' allows load to finish despite Snowflake trying to block randomized data errors
        copy_query = f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
            ON_ERROR = 'CONTINUE' 
            PURGE = TRUE;
        """
        cursor.execute(copy_query)
        
        print(f'Successfully loaded {table_name}')
        
    except Exception as e:
        print(f'Error loading {table_name}: {e}')
    finally:
        cursor.close()

def main():
    conn = get_snowflake_connection()
    
    # Defining map of table name -> local file
    files_to_load = {
        'TRANSACTIONS': 'data/raw/transactions.csv',
        'LEDGER_ENTRIES': 'data/raw/ledger_entries.csv',
        'SETTLEMENTS': 'data/raw/settlements.csv'
    }
    
    try:
        for table, path in files_to_load.items():
            abs_path = os.path.abspath(path)
            load_table(conn, table, abs_path)
            
    finally:
        conn.close()
        print('Connection closed.')

if __name__ == '__main__':
    main()