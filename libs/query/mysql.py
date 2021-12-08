#!/usr/bin/python
import glob
import numpy as np
import pandas as pd
import os

from datetime import datetime
from libs.connections import connection

# This module is used to make data validation query

def __conn():
    """
    Create a Mysql connection engine

    Returns:
        SqlAlchemy.Engine
    """
     
    return connection('mysql')

def result(full_path):
    """
    Generate a query to load data from "orders" and "orders_details"

    Args:
        full_path (string): The path of file

    Raises:
        Exception: If not have data raise an exception to alert
    """

    # Create a query to return 'orders' and 'order_detail' joined data
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

    # Checks if the full path was passed
    if full_path == '':
        full_path = 'query_result.csv'

    # Export query result to CSV
    if not query.empty:
        query.to_csv(full_path, index=False)
    else:
        raise Exception('There is no data in the target database from the "orders" and "order_details" tables, import again')