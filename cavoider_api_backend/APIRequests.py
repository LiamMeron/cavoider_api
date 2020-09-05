import os
from pathlib import Path
import requests
import csv
from cavoider_api_backend import conf
import pandas
import tempfile
import io
from urllib.parse import urlparse
from datetime import datetime
from pathlib import Path

CACHE_PATH = Path("../res/cache/")

def get_data_from_endpoint(endpoint):
    today = datetime.now().date().isoformat()
    filename = f"{today}_{os.path.basename(urlparse(endpoint).path)}"
    file_extension = filename.split(".")[-1]
    file_cache_path = Path(CACHE_PATH) / filename
    if file_cache_path.exists():
        if file_extension == "json":
            df = pandas.read_json(file_cache_path)
        elif file_extension == "csv":
            df = pandas.read_csv(file_cache_path)
        else:
            raise ValueError(f"File Extension: {file_extension}")
        return df
    else:
        with open(file_cache_path, mode="wb") as f:
            response = requests.get(endpoint)
            f.write(response.content)
        return get_data_from_endpoint(endpoint)

def get_excess_deaths_from_cdc():
    endpoint = conf.CDC_EXCESS_DEATHS_ENDPOINT
    return get_data_from_endpoint(endpoint)

def get_nyt_historical_data():
    endpoint = conf.NYT_HISTORICAL_COUNTIES_ENDPOINT
    return get_data_from_endpoint(endpoint)

def get_nyt_current_data():
    endpoint = conf.NYT_CURRENT_COUNTIES_ENDPOINT
    return get_data_from_endpoint(endpoint)

def get_current_county_data():
    endpoint = conf.CURRENT_COUNTY_POP_ENDPOINT
    return get_data_from_endpoint(endpoint)

if __name__ == "__main__":
    get_nyt_current_data() #CSV
    get_excess_deaths_from_cdc() #JSON
    #with Path("../out/nytHistorical.xlsx").open("wb") as f:
    #    df.to_excel(f)
