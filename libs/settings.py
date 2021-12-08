#!/usr/bin/python

# Data connection information
connection_data = {
    'source_pg':{
        'host': '127.0.0.1',
        'db': 'northwind',
        'user': 'northwind_user',
        'password':'thewindisblowing'
    },
    'target_mysql':{
        'host': '127.0.0.1',
        'db': 'northwind',
        'user': 'northwind_user',
        'password':'thewindisblowing'
    }
}

# CSV file to import in /data folder
csv_file_to_import = 'order_details'