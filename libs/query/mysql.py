#!/usr/bin/python
import glob
import numpy as np
import pandas as pd
import os

from datetime import datetime
from libs.connections import connection
from sqlalchemy import create_engine

# Create a connection engine used by pandas and sqlalchemy
def __conn():
    connection_string = 'mysql://{user}:{password}@{host}:3306/{db}'.format(
        host = connection['mysql']['host'],
        db = connection['mysql']['db'],
        user = connection['mysql']['user'],
        password = connection['mysql']['password'],
    )

    return create_engine(connection_string, pool_recycle=3600)

# Generate a query to load data from "orders" and "orders_details"
def result(full_path):
    sql = """
        SELECT
            *
        FROM
            orders AS ord 
            INNER JOIN order_details AS dtl ON (ord.order_id = dtl.order_id)
    """
    # Initialize the connection
    connection = __conn()

    # Get a query result
    query = pd.read_sql(sql, connection)

    if full_path == '':
        full_path = 'query_result.csv'

    # Export query result to CSV
    if not query.empty:
        query.to_csv(full_path, index=False)
    else:
        raise Exception('There is no data in the target database from the "orders" and "order_details" tables, import again')