#!/usr/bin/python
import glob
import numpy as np
import pandas as pd
import os

from datetime import datetime
from libs.connections import connection
from sqlalchemy import create_engine, engine, inspect, MetaData, Table

# Create a connection engine used by pandas and sqlalchemy
def __conn():
    connection_string = 'mysql://{user}:{password}@{host}:3306/{db}'.format(
        host = connection['mysql']['host'],
        db = connection['mysql']['db'],
        user = connection['mysql']['user'],
        password = connection['mysql']['password'],
    )

    return create_engine(connection_string, pool_recycle=3600)

# Scan listed directory looking for CSV files recursively
def __scan_directories(dir, ext):
    file_list = []
    for r, d, f in os.walk(dir):
        for file in f:
            if ext in file:
                file_list.append(os.path.join(r, file))
    
    return file_list

# Select only files from specific date
def __list_files_to_import(path_list, date):
    files_to_import = []
    
    # Iterate over path_list
    for path in path_list:
        
        # In each path try to find CSV files
        file_list = __scan_directories(path, '.csv')
        for file in file_list:
            
            # If file path contains the date, append it on list files_to_import
            if date in file:
                files_to_import.append(file)
    
    return files_to_import

# Load data from CSV file to destination database
def load(date = ''):

    # If date was not defined uses current
    if date == '':
        date = datetime.now().strftime("%Y-%m-%d")

    # Create a connection to save data
    connection = __conn()

    # List of path to get csv files recursively
    paths = ['data/csv', 'data/postgres']

    # Get only csv files
    files_to_import = __list_files_to_import(paths, date)

    # Check if almost one exists
    if len(files_to_import) == 0:
        raise Exception("There is no record on this date, do a new extraction before loading the data")
    
    # Import file to database
    for file in files_to_import:
        # Get file name without path or extension
        file_name = os.path.splitext(os.path.basename(file))[0]

        # Read data to impor
        df = pd.read_csv(file, sep=',')

        # Export data
        df.to_sql(name=file_name, con=connection, index=False, if_exists='replace')

