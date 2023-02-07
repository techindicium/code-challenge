#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy import MetaData, Table, Column, String
import yaml
import sys
import os
from datetime import datetime

CREDENTIALS_PATH = 'docker-compose.yml'
ORDER_DETAILS_PATH = 'data/order_details.csv'

def get_db_credentials(file_path):
    '''
        Gets the necessary parameters to connect to the provided 
        database from a yaml file.
    '''
    with open(file_path) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    db_service = data['services']['db']
    port = db_service['ports'][0].split(':')[0]
    environment = db_service['environment']
    dbname = environment['POSTGRES_DB']
    user = environment['POSTGRES_USER']
    password = environment['POSTGRES_PASSWORD']
    credentials = {
        'host': 'localhost',
        'dbname': dbname,
        'user': user,
        'password': password,
        'port': port
    }
    return credentials

# Get the extraction date and create a folder to store respective data
if len(sys.argv) == 1:
    extraction_date = datetime.today()
else:
    try:
        extraction_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    except:
        print('''
        Please provide a date in the format 'YYYY-MM-DD' or no date at 
        all to extract today's data.
        ''')
        sys.exit(1)
extraction_date_str = extraction_date.strftime('%Y-%m-%d')
date_folder_path = f'data/postgres/{extraction_date_str}'
csv_folder_path = f'data/csv/{extraction_date_str}'
if not os.path.exists(date_folder_path):
    os.makedirs(date_folder_path)
if not os.path.exists(csv_folder_path):
    os.makedirs(csv_folder_path)
	
 

# extract data from the postgres database
credentials = get_db_credentials(CREDENTIALS_PATH)
user = credentials['user']
password = credentials['password']
host = credentials['host']
port = credentials['port']
db_name = credentials['dbname']
engine = create_engine(
    f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
)
inspector = inspect(engine)
table_names = inspector.get_table_names()
for table_name in table_names:
        table = pd.read_sql_table(table_name, engine.connect())
        output_name = f'{date_folder_path}/{table_name}.csv'
        table.to_csv(output_name)
engine.dispose() # to prevent resource leaks

# extract data from the provided csv file
order_details = pd.read_csv(ORDER_DETAILS_PATH)
csv_output_path = f'{csv_folder_path}/order_details.csv'
order_details.to_csv(csv_output_path)
