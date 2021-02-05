from psycopg2 import connect
from psycopg2 import OperationalError

def database_connection():
    try:
        # Connect to an existing database
        connection = connect(
            host="localhost:5432",
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


def fetch_table_data(table_name):
    # The connect() constructor creates a connection to the MySQL server and returns a MySQLConnection object.
    cnx = connect(
        host="0.0.0.0",
        database="northwind",
        user="northwind_user",
        password="thewindisblowing")

    cursor = cnx.cursor()
    cursor.execute('select * from ' + table_name)

    header = [row[0] for row in cursor.description]

    rows = cursor.fetchall()

    # Closing connection
    cnx.close()

    return header, rows


def export(table_name):
    header, rows = fetch_table_data(table_name)

    # Create csv file
    f = open(table_name + '.csv', 'w')

    # Write header
    f.write(','.join(header) + '\n')

    for row in rows:
        f.write(','.join(str(r) for r in row) + '\n')

    f.close()
    print(str(len(rows)) + ' rows written successfully to ' + f.name)
#
# def get_tables(cursor):
#     try:
#         cursor.execute("select table_name from information_schema.tables where table_schema='public'")
#         tables_names = cursor.fetchall()
#
#         tables_names_array = []
#         for tup in tables_names:
#             for val in tup:
#                 export(val)
#
#     except (Exception, OperationalError) as error:
#         print("Error during get_tables(),", error, "\n")
#     finally:
#         print("Stopping..", "\n")

# def export(header, rows):
#     header, rows = get_tables(table_name)
#
#     # Create csv file
#     f = open(table_name + '.csv', 'w')
#
#     # Write header
#     f.write(','.join(header) + '\n')
#
#     for row in rows:
#         f.write(','.join(str(r) for r in row) + '\n')
#
#     f.close()
#     print(str(len(rows)) + ' rows written successfully to ' + f.name)


if __name__ == '__main__':
    print("App start..")

    export('orders')

    print("Extraction finished!")
