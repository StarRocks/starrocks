#!/bin/bash

# Check if the required number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <mysql_cmd> <db>"
    exit 1
fi

# Assign command-line arguments to variables
mysql_cmd=$1

# Loop to insert 300 records with different date values based on the index range
for i in $(seq 1 300); do
    # Determine the date value based on the range of 'i'
    if [ "$i" -le 100 ]; then
        date_value="2020-06-01"
    elif [ "$i" -le 200 ]; then
        date_value="2020-07-01"
    else
        date_value="2020-08-01"
    fi

    # Construct the SQL INSERT statement
    SQL="INSERT INTO t (k, k1) WITH LABEL $2_$i VALUES ($i, '$date_value');"

    # Execute the SQL command using the provided MySQL command
    echo "$SQL" | $mysql_cmd -D$2
done
