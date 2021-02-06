import csv
import os
import psycopg2
import datetime

today_is_the_day = datetime.datetime.now()

filePath = f'./data/postgres/{today_is_the_day.year}-{today_is_the_day.month}-{today_is_the_day.day}/'

# Database connection variable.
connect = None

# Check if the file path exists.
if not os.path.exists(filePath):
    os.mkdir(filePath)
    try:

        # Connect to database.
        connect = psycopg2.connect(
            host="0.0.0.0",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing")

    except psycopg2.DatabaseError as e:

        # Confirm unsuccessful connection and stop program execution.
        print("Database connection unsuccessful.")
        quit()

    # Cursor to execute query.
    cursor = connect.cursor()

    cursor.execute("select table_name from information_schema.tables where table_schema='public'")
    query_response = cursor.fetchall()

    table_names = [name[0] for name in query_response]
    # SQL to select data from the person table.

    try:
        for table_name in table_names:
            sqlSelect = f"SELECT * FROM {table_name}"

            # Execute query.
            cursor.execute(sqlSelect)

            # Fetch the data returned.
            results = cursor.fetchall()

            # Extract the table headers.
            headers = [i[0] for i in cursor.description]

            # Open CSV file for writing.
            csvFile = csv.writer(open(f'{filePath}{table_name}.csv', 'w', newline=''),
                                 delimiter=',', lineterminator='\r\n',
                                 quoting=csv.QUOTE_NONE, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            # Message stating export successful.
            print("Data export successful.")

    except psycopg2.DatabaseError as e:

        # Message stating export unsuccessful.
        print("Data export unsuccessful.")
        quit()

    finally:

        # Close database connection.
        connect.close()

else:
    filePath = f'./data/csv/{today_is_the_day.year}-{today_is_the_day.month}-{today_is_the_day.day}/'
