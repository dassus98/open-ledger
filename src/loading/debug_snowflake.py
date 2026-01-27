import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    print("🕵️ BEGINNING DIAGNOSTIC CHECK...")
    
    # 1. Connect
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        print("✅ Connection Successful.")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    cursor = conn.cursor()
    
    try:
        # 2. Check Context
        cursor.execute("SELECT CURRENT_REGION(), CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"ℹ️  Snowflake Version: {version[1]} (Region: {version[0]})")

        # 3. List Schemas in the Database
        target_db = os.getenv('SNOWFLAKE_DATABASE')
        print(f"\n📂 Checking Database: {target_db}")
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {target_db}")
        schemas = [row[1] for row in cursor.fetchall()]
        print(f"   Found Schemas: {schemas}")
        
        if 'RAW' not in schemas:
            print("❌ CRITICAL ERROR: The 'RAW' schema does not exist!")
        
        # 4. List Tables in the RAW Schema
        target_schema = os.getenv('SNOWFLAKE_SCHEMA') # Should be RAW
        print(f"\n📄 Checking Tables in {target_db}.{target_schema}...")
        
        try:
            cursor.execute(f"SHOW TABLES IN SCHEMA {target_db}.{target_schema}")
            tables = cursor.fetchall()
            
            if not tables:
                print("❌ NO TABLES FOUND. The schema is empty.")
            else:
                print(f"   Found {len(tables)} tables:")
                for table in tables:
                    # table[1] is table name, table[6] is owner
                    print(f"   - Name: '{table[1]}' | Owner: {table[6]} | Kind: {table[0]}")
                    
        except Exception as e:
            print(f"❌ Could not list tables: {e}")

    finally:
        conn.close()
        print("\n🕵️ DIAGNOSTIC COMPLETE.")

if __name__ == "__main__":
    main()