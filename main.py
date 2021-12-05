#!/usr/bin/python
import getopt
import re
import sys

from libs.extract.pg import extract as pg_extract
from libs.extract.csv import extract as csv_extract
from libs.load.csv import load as csv_load

# Error message
def __error_message(message):
    print('')
    print('** Error in {message} process **'.format(message = message))
    print('')
    

# Validate date string input
def __validate_string_date(date):
    date_pattern = r"^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$"
    validator = re.compile(date_pattern)

    # Check if date format match
    if validator.match(date) is None:
        print("Please enter a valid date in format '-d YYYY-MM-DD'")
        sys.exit(1)

# Run first step - data extraction
def extract(string_date):
    print('')
    print(" ------------- RUNNING FIRST STEP ---------------")
    print(" - 1.1 Extracting Postgres tables data")
    
    # Extract data from postgres
    try:
        pg_extract(string_date)
    except Exception as e:
        __error_message('PostGres Extraction')
        print(e)
        sys.exit(1)
    
    print(" - 1.1 End Postgres extraction")

    # Extract data from CSV file
    print(" - 1.2 Extracting CSV file data")
    
    # Extract data from postgres
    try:
        csv_extract(string_date)
    except Exception as e:
        __error_message('CSV Extraction')
        print(e)
        sys.exit(1)
    
    print(" - 1.2 End CSV file extraction")
    print(" ------------- END OF FIRST STEP ---------------")
    print('')

# Run second step - data loading
def load(string_date):
    # Load data from CSV to destination database
    print('')
    print(" ------------- RUNNING SECOND STEP --------------")
    print(" - 2.1 Loading data from CSV file to database")
    
    try:
        csv_load(string_date)
    except Exception as e:
        __error_message('CSV to database loading')
        print(e)
        sys.exit(1)
    
    print(" - 2.1 End of loading data from CSV to database")
    print(" ------------- END OF SECOND STEP ---------------")
    print('')

# Run the pipeline with options
def main(argv):
    # Initialize pipeline operation
    operation = 'all'
    string_date = ''

    # Get args from command line to process all or step by step
    try:
        opts, args = getopt.getopt(argv,"h:eld:")

    except getopt.GetoptError:
        print('main.py -e <extract data> | -l <load data>  -d <date YYYY-MM-DD>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('For data Extraction:')
            print('- Use "main.py -e" if you need a specific day add "-d YYYY-MM-DD"')
            
            print('For data Load:')
            print('- Use "main.py -l" if you need a specific day add "-d YYYY-MM-DD"')
            
            print('For all pipeline operantion just run "main.py", if "-d YYYY-MM-DD" was not defined, its will consider current date')
            sys.exit()
        elif opt in ("-l", ""):
            operation = 'load'
        elif opt in ("-e", ""):
            operation = 'extract'
        elif opt in ("-d", ""):
            string_date = arg

    # Validate string date from argv
    if string_date != '':
        __validate_string_date(string_date)

    _header_delimiter = '================================================='
    print(_header_delimiter)
    print('           Starting pipeline exection')
    print(_header_delimiter)
    
    # Run isolated step or full pipeline
    if operation == 'load':
        load(string_date)
    elif operation == 'extract':
        extract(string_date)
    else:
        extract(string_date)
        load(string_date)
    
    print(_header_delimiter)
    print("           Finished pipeline execution")
    print(_header_delimiter)

if __name__ == "__main__":
   main(sys.argv[1:])
