import csv
import os
import psycopg2

# File path and name.
filePath = './data/postgres/'
fileName = 'orders.csv'

# Database connection variable.
connect = None

# Check if the file path exists.
if os.path.exists(filePath):

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

    # SQL to select data from the person table.
    sqlSelect = \
        "SELECT * \
         FROM orders"

    try:

        # Execute query.
        cursor.execute(sqlSelect)

        # Fetch the data returned.
        results = cursor.fetchall()

        # Extract the table headers.
        headers = [i[0] for i in cursor.description]

        # Open CSV file for writing.
        csvFile = csv.writer(open(filePath + fileName, 'w', newline=''),
                             delimiter=',', lineterminator='\r\n',
                             quoting=csv.QUOTE_ALL, escapechar='\\')

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

    # Message stating file path does not exist.
    print("File path does not exist.")
