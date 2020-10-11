from pathlib import Path

from azure.cosmosdb.table.tableservice import TableService
from pykml import parser


def get_table_connection(conn_str):
    conn = {
        k: v
        for i in conn_str.split(";")
        for k, v in [(i.split("=")[0], "=".join(i.split("=")[1:]))]
    }
    return TableService(
        account_name=conn["AccountName"], account_key=conn["AccountKey"]
    )


def extract_county_outlines_from_kml(file: Path):
    file_contents = file.read_bytes()
    root = parser.fromstring(file_contents)
    masterlist = {}
    for placemark in root.Document.Folder.Placemark:
        fips = placemark.ExtendedData.SchemaData.SimpleData[4].text

        try:
            coordinates = str(
                placemark.Polygon.outerBoundaryIs.LinearRing.coordinates
            ).split(" ")
            coordinates = [
                [
                    {"latitude": x.split(",")[0], "longitude": x.split(",")[1]}
                    for x in coordinates
                ]
            ]
        except AttributeError:
            polygons = placemark.MultiGeometry.Polygon
            coordinates = [
                [
                    {"latitude": x.split(",")[0], "longitude": x.split(",")[1]}
                    for x in str(polygon.outerBoundaryIs.LinearRing.coordinates).split(
                        " "
                    )
                ]
                for polygon in polygons
            ]

        masterlist[fips] = coordinates
    return masterlist


def get_county_outlines_from_file_on_disk():
    file = Path("../res/cb_2018_us_county_20m.kml")
    l = extract_county_outlines_from_kml(file)
    return l
