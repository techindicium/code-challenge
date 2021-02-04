import time
import random

from sqlalchemy import create_engine

db_name = 'northwind'
db_user = 'northwind_user'
db_pass = 'thewindisblowing'
db_host = 'db'
db_port = '5432'

# Connecto to the database
db_string = 'postgres://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

def functionCallToPrint(something):
    # Insert a new number into the 'numbers' table.
    return something

if __name__ == '__main__':
    print('Application started')

    while True:
        print('thats something')
        time.sleep(5)
