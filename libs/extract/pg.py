#!/usr/bin/python
import numpy as np
import pandas as pd
import psycopg2
import os
import warnings 

from datetime import datetime
from libs.connections import connection
from sqlalchemy import inspect, MetaData, Table, exc as sa_exc

# This module extract data from Postgres database

def __conn():
    """
    Create a Postgres connection engine

    Returns:
        SqlAlchemy.Engine
    """    

    return connection('pg')

def __list_tables():
    """
    List all tables in database

    Returns:
        list: A list of table names from database
    """    
    
    # Initialize the connection engine
    engine = __conn()
    
    # Inspects database elements
    inspector = inspect(engine)

    # Get table object list
    tables = inspector.get_table_names()

    # Make list of table names
    table_list = []
    for table in tables:
        table_list.append(table)

    return table_list

def __list_column_name(table):
    """
    List columns names from table

    Args:
        table (str): Table name to check

    Returns:
        list: A list of column names
    """
    
    # Initialize the connection engine
    engine = __conn()

    # Initialize list of names
    columns_list = []

    # Supress sqlalchemy warnings 
    with warnings.catch_warnings():
        # Set to ignore
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)
        
        # Initialize a reflection of database to see table information
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)

        # Read table schema and return column name
        table_obj = Table(table, metadata_obj, autoload_with=engine)
        
        # Generate a list from table object
        columns_list = [column.name for column in table_obj.columns]
    
    return columns_list


def extract(date = ''):
    """
    Extracts data from Postgres to csv file 

    Args:
        date (str, optional): Date in format YYYY-MM-DD. Defaults to '' for current date.
    """    
    
    # If date was not defined uses current
    if date == '':
        date = datetime.now().strftime("%Y-%m-%d")
    
    # Initialize connection engine
    connection = __conn()

    # Get table list from database
    table_list = __list_tables()

    # Iterate over table list
    for table in table_list:
        # Read names
        columns_names = __list_column_name(table)
        columns_string = ', '.join([str(elem) for elem in columns_names])

        # Create a path and file string
        path = 'data/postgres/{table}/{date}/'.format(table = table, date = date)
        file = '{table}.csv'.format(table = table)

        # If directory not exists create it
        if not os.path.exists(path):
            os.makedirs(path)

        # Read all data from database
        df = pd.read_sql('SELECT {columns_string} FROM {table}'.format(columns_string = columns_string, table = table), connection)

        # Exports to CSV
        df.to_csv("{path}{file}".format(path = path, file = file), index=False, encoding='utf8')

