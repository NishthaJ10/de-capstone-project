import requests
import json
import snowflake.connector
import os
from dotenv import load_dotenv

# Load variables from .env file into the environment
load_dotenv()

# --- 1. EXTRACT: Fetch data from the API ---
def fetch_data():
    """
    Fetches posts and comments from the JSONPlaceholder API.
    Returns two lists: one for posts and one for comments.
    """
    posts_url = "https://jsonplaceholder.typicode.com/posts"
    comments_url = "https://jsonplaceholder.typicode.com/comments"
    
    try:
        print("Fetching data from API...")
        posts_response = requests.get(posts_url)
        comments_response = requests.get(comments_url)
        
        # Raise an exception for bad status codes (4xx or 5xx)
        posts_response.raise_for_status()
        comments_response.raise_for_status()
        
        posts = posts_response.json()
        comments = comments_response.json()
        
        print(f"Successfully fetched {len(posts)} posts and {len(comments)} comments.")
        # Return the fetched data so it can be used by other functions
        return posts, comments
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
        return None, None

# --- 2. LOAD: Load data into Snowflake ---
def load_data(conn, data, db_name, schema_name, table_name):
    """
    Loads a list of JSON objects into a specified Snowflake table.
    The target table is created if it doesn't exist.
    """
    if not data:
        print(f"No data provided to load into {table_name}. Skipping.")
        return

    print(f"Preparing to load {len(data)} records into {db_name}.{schema_name}.{table_name}...")
    cur = conn.cursor()
    try:
        # Set the context to the correct database and schema
        cur.execute(f"USE DATABASE {db_name};")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        cur.execute(f"USE SCHEMA {schema_name};")
        
        # Create the table with a VARIANT column if it doesn't exist
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                RAW_JSON VARIANT
            );
        """)
        
        # Truncate the table to avoid duplicates on re-runs
        cur.execute(f"TRUNCATE TABLE {table_name};")

        # Prepare the insert statement
        insert_sql = f"INSERT INTO {table_name} (RAW_JSON) SELECT PARSE_JSON(%s);"
        
        # Execute the insert for each record
        for record in data:
            cur.execute(insert_sql, (json.dumps(record),))
            
        print(f"Successfully loaded {len(data)} records into {table_name}.")

    finally:
        # Always close the cursor after the operation is complete
        cur.close()

# --- 3. Main Orchestration Block ---
def main():
    """Main function to control the ETL process."""
    # Fetch posts and comments data first
    posts_data, comments_data = fetch_data()
    
    # If fetching failed, stop the script
    if posts_data is None or comments_data is None:
        print("Halting execution due to data fetching failure.")
        return

    conn = None
    try:
        # CORRECTED SECTION: Define connection parameters as a dictionary
        conn_params = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA")
        }
        
        # Establish the connection using the dictionary
        print("\nConnecting to Snowflake...")
        conn = snowflake.connector.connect(**conn_params)
        print("Connection successful!")

        # Use the variables from the .env file for consistency
        db = conn_params["database"]
        schema = conn_params["schema"]
        
        # Load both datasets into their respective tables
        load_data(conn, posts_data, db, schema, "POSTS")
        load_data(conn, comments_data, db, schema, "COMMENTS")

    except snowflake.connector.Error as e:
        print(f"A Snowflake error occurred: {e}")
    finally:
        # Ensure the connection is always closed
        if conn and not conn.is_closed():
            conn.close()
            print("Snowflake connection closed.")

# This makes the script runnable
if __name__ == "__main__":
    main()