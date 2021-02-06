from psycopg2 import connect
from psycopg2 import OperationalError

def database_connection():
    try:
        # Connect to an existing database
        connection = connect(
            host="0.0.0.0",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        return cursor

        print("Successfully connected to database.")

    except (Exception, OperationalError) as error:
        print("Error while connecting to PostgreSQL", error)
    # finally:
    #     if (connection):
    #         cursor.close()
    #         connection.close()
    #         print("PostgreSQL connection is closed")


def fetch_table_data():

    cursor = database_connection();

    array_names = tables_names_array(cursor)
    print(array_names)

    for name in array_names:
        # query = 'SELECT * FROM '
        cursor.execute('select * from ' + name)
        header = [row[0] for row in cursor.description]
        rows = cursor.fetchall()
        export_tables(cursor.fetchall())
    #Closing connection
    cnx.close()
    return header, rows


def export_tables(table_name):

    header, rows = fetch_table_data()

    # Create csv file
    f = open(table_name + '.csv', 'w')

    # Write header
    f.write(','.join(header) + '\n')

    for row in rows:
        f.write(','.join(str(r) for r in row) + '\n')

    f.close()
    print(str(len(rows)) + ' rows written successfully to ' + f.name)


def tables_names_array(cursor):
    try:
        cursor.execute("select table_name from information_schema.tables where table_schema='public'")
        tables_names = cursor.fetchone()
        #
        # for tup in tables_names:
        #     for val in tup:
        #         tables_names_array.append(val)

        return tables_names

    except (Exception, OperationalError) as error:
        print("Error during get_tables(),", error, "\n")
    finally:
        print("Stopping..", "\n")

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

    fetch_table_data()

    print("Extraction finished!")
