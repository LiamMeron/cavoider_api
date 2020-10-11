import datetime
import json
import logging

import azure.functions as func

import calculations
from repository import AzureTableRepository, Partition


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    calculations.updateRepoWithNewData()

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
