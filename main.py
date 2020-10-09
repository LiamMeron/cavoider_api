from fastapi import FastAPI
from cavoider_api_backend.repository import AzureTableRepository, Partition

app = FastAPI()
repo = AzureTableRepository("Test01")


@app.get("/latest/{fips}")
async def read_latest_report_for(fips):
    data = repo.get(partition=Partition.latest_county_report, row_key=f"{fips}")
    return data


if __name__ == "__main__":
    pass
