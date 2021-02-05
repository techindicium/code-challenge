from psycopg2 import connect
from psycopg2 import OperationalError

def database_connection():
    try:
        # Connect to an existing database
        connection = connect(
            host="db",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        print("Successfully connected to database.")
        get_tables(cursor)

    except (Exception, OperationalError) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def get_tables(cursor):
    try:
        cursor.execute("select table_name from information_schema.tables where table_schema='public'")
        tables_names = list(cursor.fetchall())

        print("Transforming into a iterable...", "\n")

        for val in tables_names:
            print(val, "\n")

    except (Exception, OperationalError) as error:
        print("Error during get_tables(),", error, "\n")
    finally:
        print("Stopping..", "\n")

if __name__ == '__main__':
    print("App start..")

    database_connection()

    print("Extraction finished!")
