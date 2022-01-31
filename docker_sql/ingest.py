#!/usr/bin/env python
# coding: utf-8


import argparse
import pandas as pd
import os
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    dbname = params.dbname
    table_name = params.table_name
    url = params.url
    print(url)
    # download the csv
    csv_name = "output.csv"
    os.system("wget '{}' -O {}".format(url, csv_name))

    engine = create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, dbname)
    )

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create the table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists="append")
        print("insert another chunk...")


if __name__ == "__main__":
    print("debut ingestion")
    parser = argparse.ArgumentParser(
        description="Ingest CSV files to postgres instance"
    )

    # user
    # password
    # host
    # port
    # databasename
    # tablename
    # url to the csv
    parser.add_argument("--user", help="username for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--dbname", help="databasename for postgres")
    parser.add_argument("--table_name", help="tablename for postgres")
    parser.add_argument("--url", help="url to the csv")

    args = parser.parse_args()
    main(args)
