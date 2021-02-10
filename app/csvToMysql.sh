#!/bin/bash

# define database connectivity
_db="northwind"
_db_user="northwind_user"
_db_password="thewindisblowing"
_db_host="127.0.0.1"

# set directory containing CSV files
_csv_directory="../data/outputBkp/"

cd $_csv_directory

# Conversion files
_csv=$(ls *.csv)
_FROM_encoding="ISO-8859-1"
_TO_encoding="UTF-8"
_convert="iconv -f $_FROM_encoding -t $_TO_encoding//TRANSLIT"

for _file in $_csv; do
    echo $_convert "$_file" -o "${_file%.csv}.csv"
done

# Creates and recreates the schema
mysql -u$_db_user -p$_db_password -h$_db_host -e "DROP DATABASE IF EXISTS $_db;"

mysql -u$_db_user -p$_db_password -h$_db_host -e "CREATE DATABASE IF NOT EXISTS $_db CHARACTER SET utf8 COLLATE utf8_general_ci;"

# browse csv files
for _csv in ${_csv[@]}; do
    _csv_file_extensionless=$(echo $_csv | sed 's/\(.*\)\..*/\1/')

    _table_name="${_csv_file_extensionless}"

    _header_columns=$(head -1 $_csv_directory/$_csv | sed 's/;/,/g' | tr ',' '\n' | sed 's/^"//' | sed 's/"$//' | sed 's/ /_/g')
    _header_columns_string=$(head -1 $_csv_directory/$_csv | sed 's/;/,/g' | sed 's/ /_/g' | sed 's/"//g')

    mysql -u$_db_user -p$_db_password -h$_db_host $_db <<eof
CREATE TABLE IF NOT EXISTS \`$_table_name\` (
temp int NOT NULL auto_increment,
PRIMARY KEY (temp)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
eof

    # scroll through header columns
    for _header in ${_header_columns[@]}; do
        mysql -u $_db_user -p$_db_password -h$_db_host $_db --execute="ALTER TABLE \`$_table_name\` ADD COLUMN \`$_header\` TEXT"
    done

    mysql -u $_db_user -p$_db_password -h$_db_host $_db --execute="ALTER TABLE \`$_table_name\` DROP COLUMN temp"

    mysqlimport --local --compress --verbose --fields-terminated-by=',' --fields-enclosed-by='"' --lines-terminated-by="\n" --ignore-lines='1' --columns=$_header_columns_string -u $_db_user -p$_db_password -h$_db_host $_db $_csv_directory/$_csv

done
exit
