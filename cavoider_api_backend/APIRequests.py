from pathlib import Path
import requests
from cavoider_api_backend import conf


def get_data_from_endpoint(endpoint):
    return requests.get(endpoint).json()


def get_excess_deaths_from_cdc():
    endpoint = conf.CDC_EXCESS_DEATHS_ENDPOINT
    return get_data_from_endpoint(endpoint)


if __name__ == "__main__":
    import pandas
    raw_response = get_excess_deaths_from_cdc()
    df = pandas.DataFrame.from_dict(raw_response)
    with Path("../out/excess_deaths.xlsx").open("wb") as f:
        df.to_excel(f)
