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
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT * FROM Orders;")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, OperationalError) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == '__main__':
    print("App start..")
    database_connection()
