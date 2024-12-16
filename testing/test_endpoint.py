import io
import requests
import pandas as pd

def download_csv_old(url):
    response = requests.get(url)
    response.raise_for_status()
    
    # Print the response text to check its content
    print("Response Content:\n", response.text[:500])  # Print the first 500 characters for inspection
    
    # Convert the text content into a DataFrame
    if response.text.strip():  # Ensure the response isn't empty
        return pd.read_csv(io.StringIO(response.text))
    else:
        print("No data returned from the URL.")
        return pd.DataFrame()

import requests

def download_csv(url, file_name):
    """
    Downloads a CSV file from a URL and saves it to disk.
    
    Args:
        url (str): The URL to download the CSV from.
        file_name (str): The name of the file to save the CSV as.
    
    Returns:
        None
    """
    response = requests.get(url)
    response.raise_for_status()
    
    # Save the content as a .csv file
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write(response.text)
    
    print(f"CSV downloaded and saved as {file_name}")

# URLs for downloading data
CURRENT_URL = "https://www.nyse.com/api/trade-halts/current/download"
HISTORICAL_URL = "https://www.nyse.com/api/trade-halts/historical/download?symbol=&reason=&sourceExchange=&haltDateFrom=2023-12-16&haltDateTo=2024-12-16"

# Save the current halts data
download_csv(CURRENT_URL, "current_trade_halts.csv")

# Save the historical halts data
download_csv(HISTORICAL_URL, "historical_trade_halts.csv")
