#!/usr/bin/python
from sqlalchemy import create_engine

# Dictionary with data connection information
__connection_data = {
    'pg':{
        'host': '127.0.0.1',
        'db': 'northwind',
        'user': 'northwind_user',
        'password':'thewindisblowing'
    },
    'mysql':{
        'host': '127.0.0.1',
        'db': 'northwind',
        'user': 'northwind_user',
        'password':'thewindisblowing'
    }
}

def connection(connection_type = 'pg'):
    """
    Create a connection engine instance to use in raw queries or pandas

    Args:
        connection_type (str, optional): Could be 'pg' to Postgres or 'mysql' to Mysql / MariaDB. Defaults to 'pg'.

    Raises:
        Exception: Raise an exception if informed connection type was not setted yet.

    Returns:
        SqlAlchemy.Engine
    """    
    
    if connection_type == 'mysql':
        connection_string = 'mysql://{user}:{password}@{host}:3306/{db}'.format(
            host = __connection_data['mysql']['host'],
            db = __connection_data['mysql']['db'],
            user = __connection_data['mysql']['user'],
            password = __connection_data['mysql']['password'],
        )

    elif connection_type == 'pg':
        connection_string = 'postgresql://{user}:{password}@{host}:5432/{db}'.format(
            host = __connection_data['pg']['host'],
            db = __connection_data['pg']['db'],
            user = __connection_data['pg']['user'],
            password = __connection_data['pg']['password'],
        )

    else:
        raise Exception('This type of connection has not been defined!')

    return create_engine(connection_string, pool_recycle=3600)