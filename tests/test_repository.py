from copy import deepcopy
from datetime import datetime, timedelta
from typing import Union

import pytest

from cavoider_api_backend import conf

from cavoider_api_backend.repository import (
    FakeAzureTableRepository,
    AbstractRepository,
    Partition as Pt,
    AzureTableRepository,
)
from cavoider_api_backend.utilities import get_table_connection


@pytest.fixture()
def report():
    date = datetime.fromisoformat("2020-09-17")
    report = {"fips": "001", "report_date": date, "score": "bad"}
    return report


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_historical_rowkey_format(repository: AbstractRepository, report):
    repository.add(Pt.historical_county_reports, report)
    row_key = f'{report["fips"]}_{report["report_date"]}'
    assert repository.get(Pt.historical_county_reports, row_key) is not None


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_latest_rowkey_format(repository: AbstractRepository, report):
    repository.add(Pt.latest_county_report, report)
    row_key = f'{report["fips"]}'
    get_val = repository.get(Pt.latest_county_report, row_key)
    repository.delete(Pt.latest_county_report, row_key)
    assert get_val is not None


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_adds_new_record_to_historical_partition(
    repository: AbstractRepository, report
):
    partition = Pt.historical_county_reports
    repository.add(partition, deepcopy(report))
    stored_result = repository.get(partition, get_row_key(partition, report))

    # Assert that all items in report are stored
    assert set(report.items()).issubset(stored_result.items())


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_adds_new_record_to_latest_partition(
    repository: AbstractRepository, report
):
    partition = Pt.latest_county_report
    repository.add(partition, deepcopy(report))
    stored_result = repository.get(partition, get_row_key(partition, report))

    # Assert that all items in the report are stored
    assert set(report.items()).issubset(stored_result.items())


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_prevents_duplicated_in_latest(
    repository: AbstractRepository, report
):
    old_report = deepcopy(report)
    old_report["report_date"] = report["report_date"] - timedelta(days=1)

    repository.add(Pt.latest_county_report, deepcopy(report))
    repository.add(Pt.latest_county_report, deepcopy(old_report))

    # Test that the date is from the original report, not the second(old) report.
    assert (
        repository.get(Pt.latest_county_report, report["fips"])["report_date"]
        == report["report_date"]
    )


@pytest.mark.parametrize("repository", [FakeAzureTableRepository()])
def test_repository_updates_latest(repository: AbstractRepository, report):
    old_report = deepcopy(report)
    old_report["report_date"] = report["report_date"] - timedelta(days=1)

    repository.add(Pt.latest_county_report, deepcopy(old_report))
    repository.add(Pt.latest_county_report, deepcopy(report))

    # Test that the date is from the original report, not the second(old) report.
    assert (
        repository.get(Pt.latest_county_report, report["fips"])["report_date"]
        == report["report_date"]
    )


def test_get_table_connection_returns_not_null():
    assert get_table_connection(conf.AZ_TABLE_CONN_STR)


def test_master_table_exists():
    table_conn = get_table_connection(conf.AZ_TABLE_CONN_STR)
    assert table_conn.exists("master")


def get_row_key(partition: Pt, report):
    if partition is Pt.latest_county_report:
        return report["fips"]
    elif partition is Pt.historical_county_reports:
        return f"{report['fips']}_{report['report_date']}"
    else:
        raise ValueError(f"Can't get row key for partition: {partition}")


if __name__ == "__main__":
    pass
