import os

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas
import requests

from cavoider_api_backend import conf

CACHE_PATH = Path("../res/cache/")


def get_data_from_endpoint(endpoint, is_current):
    today = datetime.now().date().isoformat()
    if is_current:
        data_date = "current"
    else:
        data_date = "historical"
    filename = f"{today}_{data_date}_{os.path.basename(urlparse(endpoint).path)}"
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
        write_to_file(endpoint, file_cache_path)
        return get_data_from_endpoint(endpoint, data_date)


def write_to_file(endpoint, file_cache_path):
    with open(file_cache_path, mode="wb") as f:
        response = requests.get(endpoint)
        f.write(response.content)


def get_excess_deaths_from_cdc():
    endpoint = conf.CDC_EXCESS_DEATHS_ENDPOINT
    return get_data_from_endpoint(endpoint, True)


def get_nyt_historical_data():
    endpoint = conf.NYT_HISTORICAL_COUNTIES_ENDPOINT
    return get_data_from_endpoint(endpoint, False)


def get_nyt_current_data():
    endpoint = conf.NYT_CURRENT_COUNTIES_ENDPOINT
    return get_data_from_endpoint(endpoint, True)


def get_current_county_data():
    endpoint = conf.CURRENT_COUNTY_POP_ENDPOINT
    return get_data_from_endpoint(endpoint, True)


def get_current_state_data():
    endpoint = conf.CURRENT_STATE_ENDPOINT
    return get_data_from_endpoint(endpoint, True)


if __name__ == "__main__":
    pass
    # with Path("../out/nytHistorical.xlsx").open("wb") as f:
    #    df.to_excel(f)
