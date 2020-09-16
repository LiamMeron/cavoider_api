from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from conf import DB_PASS  # , DB_USER, DB_PASSWORD, ,DB_HOST, DB_PORT, DB_DBNAME
from APIRequests import get_current_county_data
# conn_str = f"mssql+pyodbc://{conf.db_user}:{conf.db_password}@{conf.db_host}.database.windows.net:{conf.db_port}/{conf.db_dbname}?driver=ODBC+Driver+17+for+SQL+Server"


# engine = create_engine(f"jdbc:sqlserver://cavoider-dev.database.windows.net:1433;database=reporting;
# user=dev-admin@cavoider-dev;password={DB_PASS};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;", echo = True)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

engine = create_engine(
    f"mssql+pyodbc://dev-admin@cavoider-dev:{DB_PASS}@cavoider-dev.database.windows.net:1433/reporting?driver=SQL+Server",
    echo=True
)
meta = MetaData()
base = declarative_base()


class Dim_Municipals(base):
    __tablename__ = 'DIM_Municipals'
    fips = Column(Integer, primary_key=True)
    name = Column(String(64))
    population = Column(Integer)


meta.create_all(engine)

sessionFactory = sessionmaker(engine)
session = sessionFactory()
pass


def populate_municipalities(session: Session, data):
    session.bulk_insert_mappings(Dim_Municipals, data)
    session.commit()
    session.close()


if __name__ == "__main__":
    df = get_current_county_data()
    data = df.to_dict()
    session = sessionFactory()

    new_data = []
    for i, v in enumerate(data["countyFIPS"]):
        if data["countyFIPS"][i] != 0:
            new_data.append(
                {
                    "fips": data["countyFIPS"][i],
                    "name": data["County Name"][i],
                    "population": data["population"][i]
                }
            )
    pass
    populate_municipalities(session, new_data)
