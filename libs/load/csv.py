#!/usr/bin/python
import glob
import numpy as np
import pandas as pd
import os

from datetime import datetime
from libs.connections import connection

# This module load data to destination database 
# from csv file

def __conn():
    """
    Create a Mysql connection

    Returns:
        SqlAlchemy.Engine
    """ 

    return connection('target_mysql')

def __scan_directories(dir_path, ext):
    """
    Scan listed directory looking for files by extension recursively

    Args:
        dir_path (str): Directory path to scan
        ext (str): File extension we are looking for

    Returns:
        list: List of found files
    """    

    file_list = []
    for path, directories, files in os.walk(dir_path):
        
        # Iterate over file list
        for file in files:
        
            # Filter by extension
            if ext in file:
                file_list.append(os.path.join(path, file))
    
    return file_list

def __list_files_to_import(path_list, date):
    """
    Select only files from specific date

    Args:
        path_list (list): A list with multiple paths to check
        date (str): A string date with YYY-MM-DD format

    Returns:
        list: A list with a fullpath of CSV files
    """    
    
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

def load(date = ''):
    """
    Load data from CSV file to destination database

    Args:
        date (str, optional): A date with YYYY-MM-DD format used in folder reference. Defaults to '' for current date.

    Raises:
        Exception: If a specific date does not exist within the folders
    """    
    
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

