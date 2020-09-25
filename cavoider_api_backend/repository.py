import abc
import logging
import time
from copy import deepcopy
from enum import Enum
from pprint import pprint

import conf
from tests import test_repository
from utilities import get_table_connection

log = logging.getLogger("repository")


class Partition(Enum):
    latest_county_report = "latest_county_report"
    historical_county_reports = "historical_county_reports"
    counties = "counties"


class AbstractRepository(abc.ABC):
    def add(self, partition: Partition, data: dict):
        if partition == Partition.historical_county_reports:
            data["PartitionKey"] = Partition.historical_county_reports.value
            if is_valid_county_report(data):
                data["RowKey"] = f"{data['fips']}_{data['report_date']}"
                self._add(data)
            else:
                raise ValueError(f"Did not pass county validity: \n{pprint(data)}")
        elif partition == Partition.latest_county_report:
            data["PartitionKey"] = partition.latest_county_report.value
            if is_valid_county_report(data):
                latest_version_in_table = self.get(
                    partition.latest_county_report, data["fips"]
                )
                data["RowKey"] = f"{data['fips']}"
                if not latest_version_in_table:
                    self._add(data)

                elif latest_version_in_table["report_date"] < data["report_date"]:
                    self._update(data)

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
        elif partition == partition.counties:
            data["PartitionKey"] = partition.counties.value
            data["RowKey"] = data["fips"]
            self._update(data)

    def _add(self, data: dict):
        raise NotImplemented

    def _update(self, data: dict):
        raise NotImplemented

    def get(self, partition: Partition, row_key: str, default_val=None):
        raise NotImplemented

    def delete(self, partition: Partition, row_key: str):
        raise NotImplemented


class FakeAzureTableRepository(AbstractRepository):
    def __init__(self):
        self.table = {
            Partition.historical_county_reports: [],
            Partition.latest_county_report: [],
            Partition.counties: [],
        }

    def _add(
        self,
        data: dict,
    ):
        partition = Partition[data["PartitionKey"]]
        self.table[partition].append(data)

    def _update(self, data: dict):
        partition = Partition[data["PartitionKey"]]
        self.table[partition] = [
            i if i["RowKey"] != data["RowKey"] else deepcopy(data)
            for i in self.table[partition]
        ]

    def get(self, partition: Partition, row_key: str, default_val=None):
        try:
            return next(r for r in self.table[partition] if r["RowKey"] == row_key)
        except StopIteration:
            return default_val

    def delete(self, partition: Partition, row_key: str):
        self.table[partition] = [
            i if i["RowKey"] != row_key else None for i in self.table[partition]
        ]


class AzureTableRepository(AbstractRepository):
    def __init__(self, table_name="master"):
        self.table_svc = get_table_connection(conf.AZ_TABLE_CONN_STR)
        self.table_name = table_name
        if not self.table_svc.exists(table_name):
            self.table_svc.create_table(table_name)
            while not self.table_svc.exists(table_name):
                time.sleep(10)
            log.info(f"Created table {table_name}")

    def _add(self, data: dict):
        self.table_svc.insert_entity(self.table_name, data)

    def _update(self, data: dict):
        self.table_svc.merge_entity(self.table_name, data)

    def get(self, partition: Partition, row_key: str, default_val=None):
        return self.table_svc.get_entity(
            self.table_name, partition_key=partition.value, row_key=row_key
        )

    def delete(self, partition: Partition, row_key: str):
        return self.table_svc.delete_entity(self.table_name, partition.value, row_key)


def is_valid_county_report(data: dict) -> bool:
    """Verifies that a given data report contains the minimum necessary fields to create the
    rowKey and partitionKey fields...."""
    try:
        return data["fips"] and data["report_date"]
    except IndexError:
        return False


if __name__ == "__main__":
    report = {"fips": "001", "report_date": "2020-09-20", "score": "bad"}
    repository = AzureTableRepository()
    repository.add(Partition.historical_county_reports, report)
