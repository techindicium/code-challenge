#!/usr/bin/env python3

import pandas as pd
import psycopg2
import yaml
import sys
from datetime import datetime

CREDENTIALS_PATH = 'docker-compose.yml'
ORDER_DETAILS_PATH = '/data/order_details.csv'

def get_db_credentials(file_path):
    '''
        Gets the necessary parameters to connect to the provided 
        database from a yaml file.
    '''
    with open(file_path) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    db_service = data['services']['db']
    environment = db_service['environment']
    port = db_service['ports'][0].split(':')[0]
    return f'''
    host='localhost',
    dbname={environment['POSTGRES_DB']},
    user={environment['POSTGRES_USER']},
    password={environment['POSTGRES_PASSWORD']}
    'port'={port}
    '''

def get_table_names(connection):
    '''
        Gets the names of all tables in the connected postgres database.
    ''' 
    with connection.cursor() as cursor:
        query = '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type='BASE_TABLE' AND table_schema='public'
        '''
        cursor.execute(query)
        table_names = [row[0] for row in cursor]
    return table_names

# Get the extraction date
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

# extract data from the postgres database
credentials = get_db_credentials(CREDENTIALS_PATH)
with psycopg2.connect(credentials) as conn:
    table_names = get_table_names(conn)
    for table_name in table_names:
        with conn.cursor() as cursor:
            query = f"SELECT * FROM {table_name}"
            table = pd.read_sql(query, conn)
            output_name = f'''
            /data/postgres/{extraction_date}/{table_name}.csv
            '''
            table.to_csv(output_name)

conn.close() # contexts do not close the connection, only commit or
# roll-back transactions in case of success or failure, respectively.

# extract data from the provided csv file
order_details = pd.read_csv(ORDER_DETAILS_PATH)
order_details_output_path = f'''
/data/csv/{extraction_date}/order_details.csv
'''
order_details.to_csv(order_details_output_path)
