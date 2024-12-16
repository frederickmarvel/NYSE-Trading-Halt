import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import schedule
import time
import io

CURRENT_URL = "https://www.nyse.com/api/trade-halts/current/download"
HISTORICAL_URL = "https://www.nyse.com/api/trade-halts/historical/download?symbol=&reason=&sourceExchange=&haltDateFrom=2023-12-16&haltDateTo=2024-12-16"

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    # Use io.StringIO to handle the response text as a file-like object
    return pd.read_csv(io.StringIO(response.text))

download_csv(CURRENT_URL)