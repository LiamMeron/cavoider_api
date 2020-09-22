from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity


def get_table_connection(conn_str):
    conn = {
        k: v
        for i in conn_str.split(";")
        for k, v in [(i.split("=")[0], "=".join(i.split("=")[1:]))]
    }
    return TableService(
        account_name=conn["AccountName"], account_key=conn["AccountKey"]
    )
