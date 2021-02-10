#!/usr/bin/python3
# -*- coding: utf-8 -*-

from app.exportPg import extraction_pg, csv_bkp, importTo

if __name__ == "__main__":
    try:
        _runPg = str(input("Perform postgres database extraction? [y/n]")).upper()
        _runCsv = str(input("Perform order_details extraction? [y/n]")).upper()
        _runImport = str(input("Run import to Mysql? [y/n]")).upper()

        if _runPg == "Y" or _runPg == "YES":
            extraction_pg()
            print("--> Every bank table is a backup in the directory --> /bkp/pg/")
        elif _runCsv == "Y" or _runCsv == "YES":
            csv_bkp()
            print("-->The Backup --> /bkp/csv/")
        elif _runImport == "Y" or _runImport == "YES":
            importTo()
            print("-->The import was successful.")

    except ValueError:
        print("Error in app... Check the script and re-run.")

    finally:
        print("Successful Migration!")