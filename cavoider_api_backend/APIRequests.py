from pathlib import Path
import requests
import csv
from cavoider_api_backend import conf
import pandas
import tempfile
import io


def get_data_from_endpoint(endpoint):
    file_extension = endpoint.split(".")[-1]
    if (file_extension == "json"):
        response = requests.get(endpoint).json()
        df = pandas.DataFrame.from_dict(response)
        return df
    else:
        response = requests.get(endpoint)
        df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')))
        return df

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
    df = get_current_county_data()
    print(df.head())
    print(df.tail())
    #with Path("../out/nytHistorical.xlsx").open("wb") as f:
    #    df.to_excel(f)
