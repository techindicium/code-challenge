import os
import csv
import datetime as dt
import psycopg2 as pg
import shutil as st

# define date
_everyday = dt.datetime.now()

# define directories
_path = "data/pg/{}-{}-{}/".format(_everyday.year, _everyday.month, _everyday.day)
_csv_path = "data/csv/{}-{}-{}/".format(_everyday.year, _everyday.month, _everyday.day)

# define connection to PG
_con = None

# PG data extraction
def extraction_pg():
    if not os.path.exists(_path):
        os.makedirs(_path)
        try:
            _con = pg.connect(
                host="0.0.0.0",  # if you change this
                database="northwind",  # if you change this
                user="northwind_user",  # if you change this
                password="thewindisblowing",  # if you change this
            )
        except pg.DatabaseError as e:
            return e

        _cur = _con.cursor()

        _cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        _query = _cur.fetchall()

        _tables = [_name[0] for _name in _query]

        try:
            for _table in _tables:

                _sql = "SELECT * FROM {}".format(_table)
                _cur.execute(_sql)
                _results = _cur.fetchall()
                _headers = [i[0] for i in _cur.description]

                _csv = csv.writer(
                    open(f"{_path}{_table}.csv", "w", newline=""),
                    delimiter=",",
                    lineterminator="\r\n",
                    quoting=csv.QUOTE_NONE,
                    escapechar="\\",
                )

                _csv.writerow(_headers)
                _csv.writerows(_results)

        except pg.DatabaseError as e:
            return e

        _con.close()


# backup of CSV data
def csv_bkp():
    if not os.path.exists(_csv_path):
        os.makedirs(_csv_path)

        try:
            _bkp = st.copy2("data/order_details.csv", _csv_path)

        except OSError as e:
            return e


if __name__ == "__main__":
    try:
        extraction_pg()
        csv_bkp()

    except ValueError:
        print("Error in app... Check the script and re-run.")

    finally:
        print("Every bank table is a backup in the directory --> /bkp/pg/")
        print("The Backup --> /bkp/csv/")
