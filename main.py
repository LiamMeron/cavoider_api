from fastapi import FastAPI, Query
from cavoider_api_backend.repository import AzureTableRepository, Partition

app = FastAPI()
repo = AzureTableRepository()


@app.get("/")
async def main():
    return "Hello World!"


@app.get("/latest/{fips}")
async def read_latest_report_for(q: str = Query(None)):
    response = []
    for item in q:
        data = repo.get(partition=Partition.latest_county_report, row_key=f"{item}")
        response.append(data)
    return response


@app.get("/county/{fips}")
async def get_county_outline_for(fips: str):
    data = repo.get(partition=Partition.counties, row_key=fips)
    return {"fips": data["RowKey"], "outline": data["outline"]}


if __name__ == "__main__":
    pass
