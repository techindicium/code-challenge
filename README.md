## Code Challenge

The purpose of this pipeline is to ingest data from the postgres database and integrate with part of the data in a CSV file, thus centralizing in a Mysql database so that it can be consumed.

The execution is divided into 3 steps:
* First Step - Extraction, where information is collected from the postgress database and from a .csv file to the local file system.
* Second step - Loading information saved in the local file system and sorted by export day to a destination Mysql database.
* Third step - Data import validation with a query that lists these imported data and generate a "query_result.csv" by joining the final tables "order" and "order_details" on Mysql database.





### Built With

* [Python 3](https://python.org/)
* [Pipenv](https://pipenv.pypa.io/)
* [SqlAlchemy](https://sqlalchemy.org/)
* [Pandas](https://pandas.pydata.org/)
* [Docker](https://docker.com/)
* [Mysql](https://mysql.com/)





## Getting Started

The pipeline execution is done within the python virtual environment, we use the pandas library for data extraction and export, with it we work with dataframes, simplifying the process.

### Prerequisites

* Python 3
* Pipenv
  ```sh
  pip install pipenv
  ```
* Configure connection, edit the file and configure it according to your source (pg) and destination (mysql) connection data in the dictionary "connection_data" and CSV file name to import
  ```sh
  ./libs/settings.py
    
    connection_data = {
        'source_pg':{
            'host': '127.0.0.1',
            'db': 'northwind',
            'user': 'northwind_user',
            'password':'thewindisblowing'
        },
        'target_mysql':{
            'host': '127.0.0.1',
            'db': 'northwind',
            'user': 'northwind_user',
            'password':'thewindisblowing'
        }
    }

    csv_file_to_import = 'order_details'
  ```

### Installation

_After installing pipenv we need to initialize the source and target database instances and install the packages (dependencies)._

1. Initialize the Postgres source database instance and the Mysql target database.
```sh
   docker-compose up
```
2. Initialize the Mysql target database.
(See [Mysql](https://dev.mysql.com/doc/mysql-getting-started/en/) Docs to configure your instance)

3. Package installation
```sh
   pipenv install
```
4. Initialize pipenv virtual environment
```sh
   pipenv shell
```




## Usage

To run the complete pipeline, inside the "pipenv shell" run:

```sh
   python main.py
```

As no parameter was informed, the script will run the 3 steps in sequence until the export of the file "query_result.csv".

To run on a specific date (format has to be YYYY-MM-DD) in full excution or step by step add "-d" option.

```sh
   python main.py -d 2021-11-30
```

we can perform the first 2 steps separately:

- For Postgres / CSV extraction only
```sh
   python main.py -e
```

- For loading data to Mysql only (can only be executed if there is an extraction on the date defined as parameter).
```sh
   python main.py -l
```

- To export the "query_result.csv" file along with any of the steps add "-q" option.
```sh
   python main.py -l -q
```

- To see all commands use the help option "-h".
```sh
   python main.py -h
```




## Support

Send me an email: _nicke.dev@gmail.com_