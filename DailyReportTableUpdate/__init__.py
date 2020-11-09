import datetime
import json
import logging

import azure.functions as func

from cavoider_api_backend import calculations as calc
from cavoider_api_backend.repository import AzureTableRepository, Partition


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    calc.update_repo_with_new_data()

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
