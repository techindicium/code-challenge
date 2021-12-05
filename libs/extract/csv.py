#!/usr/bin/python
import numpy as np
import pandas as pd
import os

from datetime import datetime

# Extracts data from csv file
# Acepts date format YYYY-MM-DD
def extract(date = ''):

    #If date was not defined uses current
    if date == '':
        date = datetime.now().strftime("%Y-%m-%d")

    # Define origin path
    origin_file_name = 'order_details'
    origin_path = 'data/{file}.csv'.format(file = origin_file_name)

    # Create a path and file string
    destination_path = 'data/csv/{date}/'.format(date = date)
    destination_file = '{file}.csv'.format(file = origin_file_name)

    # If directory not exists create it
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    # Read all data from CSV file
    df = pd.read_csv(origin_path)

    # Exports to CSV
    df.to_csv("{path}{file}".format(path = destination_path, file = destination_file), index=False, encoding='utf8')

