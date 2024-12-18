import io
import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration from .env
CURRENT_URL = os.getenv("CURRENT_URL")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Build database connection string
DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def download_csv(url):
    """
    Downloads a CSV file from a URL and loads it into a pandas DataFrame.
    """
    response = requests.get(url)
    response.raise_for_status()
    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data)
    print("CSV data downloaded successfully.")
    return df

def get_existing_data(engine, table_name):
    """
    Reads existing data from the database table into a pandas DataFrame.
    """
    try:
        existing_df = pd.read_sql_table(table_name, con=engine)
        print("Existing data loaded from database.")
        return existing_df
    except Exception as e:
        print(f"No existing table found: {e}")
        return pd.DataFrame()

def update_database(engine, table_name, new_data):
    """
    Updates the database with new or updated records.
    """
    # Load existing data
    existing_df = get_existing_data(engine, table_name)
    
    # Merge new data and existing data
    combined_df = pd.concat([existing_df, new_data]).drop_duplicates(subset=["Halt Date", "Halt Time", "Symbol"], keep="last")
    
    # Overwrite the table with the updated data
    combined_df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print("Database updated successfully.")

def main():
    try:
        # Step 1: Establish database connection
        engine = create_engine(DB_CONNECTION_STRING)
        table_name = "halt_records"

        while True:
            # Step 2: Download the CSV
            new_data = download_csv(CURRENT_URL)
            
            # Step 3: Update the database
            update_database(engine, table_name, new_data)

            # Wait for 60 seconds before the next iteration
            time.sleep(60)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
