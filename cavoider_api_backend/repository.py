import abc
import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from pprint import pprint

from conf import DB_PASS  # , DB_USER, DB_PASSWORD, ,DB_HOST, DB_PORT, DB_DBNAME
from APIRequests import get_current_county_data

log = logging.getLogger("repository")


class Partition(Enum):
    LATEST_COUNTY_REPORT = "latest_county_report"
    HISTORICAL_COUNTY_REPORTS = "historical_county_reports"
    COUNTIES = "counties"


class AbstractRepository(abc.ABC):
    def add(self, partition: Partition, data: dict):
        pass

    def get(self, partition: Partition, row_key: str, default_val=None):
        pass


class FakeAzureTableRepository(AbstractRepository):
    def __init__(self):
        self.table = {
            Partition.HISTORICAL_COUNTY_REPORTS: [],
            Partition.LATEST_COUNTY_REPORT: [],
            Partition.COUNTIES: [],
        }

    def add(self, partition: Partition, data: dict):
        if partition == Partition.HISTORICAL_COUNTY_REPORTS:
            data["PartitionKey"] = Partition.HISTORICAL_COUNTY_REPORTS.value
            if is_valid_county_report(data):
                data["RowKey"] = f"{data['fips']}_{data['report_date']}"
                self.table[partition].append(data)
            else:
                raise ValueError(f"Did not pass county validity: \n{pprint(data)}")
        elif partition == Partition.LATEST_COUNTY_REPORT:
            data["PartitionKey"] = partition.LATEST_COUNTY_REPORT.value
            if is_valid_county_report(data):
                latest_version_in_table = self.get(
                    partition.LATEST_COUNTY_REPORT, data["fips"]
                )
                data["RowKey"] = f"{data['fips']}"
                if (
                    not latest_version_in_table
                    or latest_version_in_table["report_date"] < data["report_date"]
                ):
                    self.table[partition].append(data)
                elif latest_version_in_table["report_date"] == data["report_date"]:
                    log.warning(
                        f"Report already exists in table! Old version timestamp: {latest_version_in_table['timestamp']}"
                    )
                else:
                    log.warning(
                        f"Upload Failed: Attempting to upload old data to LatestCountyReport: \n{pprint(data)}"
                    )
            else:
                raise ValueError(f"Did not pass county validity: \n{pprint(data)}")
        elif partition == partition.COUNTIES:
            raise NotImplemented("Writing to `counties` partition is not supported!")

    def get(self, partition: Partition, row_key: str, default_val=None):
        try:
            return next(r for r in self.table[partition] if r["RowKey"] == row_key)
        except StopIteration:
            return default_val


def is_valid_county_report(data: dict) -> bool:
    try:
        return data["fips"] and data["report_date"]
    except IndexError:
        return False


if __name__ == "__main__":
    pass
