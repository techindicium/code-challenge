#!/usr/bin/python
from .settings import connection_data
from sqlalchemy import create_engine


def connection(connection_type = 'source_pg'):
    """
    Create a connection engine instance to use in raw queries or pandas

    Args:
        connection_type (str, optional): Could be 'source_pg' to Postgres or 'target_mysql' to Mysql / MariaDB. Defaults to 'source_pg'.

    Raises:
        Exception: Raise an exception if informed connection type was not setted yet.

    Returns:
        SqlAlchemy.Engine
    """    
    
    if connection_type == 'target_mysql':
        connection_string = 'mysql://{user}:{password}@{host}:3306/{db}'.format(
            host = connection_data[connection_type]['host'],
            db = connection_data[connection_type]['db'],
            user = connection_data[connection_type]['user'],
            password = connection_data[connection_type]['password'],
        )

    elif connection_type == 'source_pg':
        connection_string = 'postgresql://{user}:{password}@{host}:5432/{db}'.format(
            host = connection_data[connection_type]['host'],
            db = connection_data[connection_type]['db'],
            user = connection_data[connection_type]['user'],
            password = connection_data[connection_type]['password'],
        )

    else:
        raise Exception('This type of connection has not been defined!')

    return create_engine(connection_string, pool_recycle=3600)