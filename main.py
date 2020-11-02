from fastapi import FastAPI, Query
from cavoider_api_backend.repository import AzureTableRepository, Partition

app = FastAPI()
repo = AzureTableRepository()


@app.get("/")
async def main():
    return "Hello World!"


@app.get("/latest/{fips}")
async def read_latest_report_for(fips: str):
    data = repo.get(partition=Partition.latest_county_report, row_key=f"{fips}")
    return data


@app.get("/county/{fips}")
async def get_county_outline_for(fips: str):
    data = repo.get(partition=Partition.counties, row_key=fips)
    return {"fips": data["RowKey"], "outline": data["outline"]}


if __name__ == "__main__":
    pass
