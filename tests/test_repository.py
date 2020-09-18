import copy
from datetime import datetime, timedelta
from typing import Union

import pytest
from cavoider_api_backend.repository import (
    FakeAzureTableRepository,
    AbstractRepository,
    Partition as Pt,
)


@pytest.fixture()
def report():
    date = datetime.fromisoformat("2020-09-17").date()
    report = {"fips": "001", "report_date": date, "score": "bad"}
    return report


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_adds_new_record_to_historical_partition(
    repository: AbstractRepository, report
):
    report_as_stored = copy.deepcopy(report)
    report_as_stored["PartitionKey"] = Pt.HISTORICAL_COUNTY_REPORTS.value
    report_as_stored[
        "RowKey"
    ] = f'{report_as_stored["fips"]}_{report_as_stored["report_date"]}'

    repository.add(Pt.HISTORICAL_COUNTY_REPORTS, report)
    assert (
        repository.get(Pt.HISTORICAL_COUNTY_REPORTS, report_as_stored["RowKey"])
        == report_as_stored
    )


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_adds_new_record_to_latest_partition(
    repository: AbstractRepository, report
):
    report_as_stored = copy.deepcopy(report)
    report_as_stored["PartitionKey"] = Pt.LATEST_COUNTY_REPORT.value
    report_as_stored["RowKey"] = report_as_stored["fips"]
    repository.add(Pt.LATEST_COUNTY_REPORT, report)


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_prevents_duplicated_in_latest(
    repository: AbstractRepository, report
):
    report_as_stored = copy.deepcopy(report)
    report_as_stored["PartitionKey"] = Pt.LATEST_COUNTY_REPORT.value
    report_as_stored["RowKey"] = report_as_stored["fips"]

    old_date = report["report_date"] - timedelta(days=1)
    old_report = copy.deepcopy(report)
    old_report["report_date"] = old_date

    repository.add(Pt.LATEST_COUNTY_REPORT, report)
    repository.add(Pt.LATEST_COUNTY_REPORT, old_report)
    assert (
        repository.get(Pt.LATEST_COUNTY_REPORT, report["fips"])["report_date"]
        == report["report_date"]
    )
    assert (
        repository.get(Pt.LATEST_COUNTY_REPORT, report["fips"])["report_date"]
        != old_date
    )


if __name__ == "__main__":
    pass
