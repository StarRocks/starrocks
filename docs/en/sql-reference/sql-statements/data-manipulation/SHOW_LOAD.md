# SHOW LOAD

## description

This statement is used to display the execution of the specified import task.

Syntax:

```sql
SHOW LOAD
[FROM db_name]
[
WHERE
[LABEL [ = "your_label" | LIKE "label_matcher"]]
[STATE = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]
]
[ORDER BY ...]
[LIMIT limit][OFFSET offset];
```

Note：

```plain text
1. If db_ name is not specified, use the current default db
2. If LABEL LIKE is used, the label of the import task will be matched. Include import task of label_ matcher.
3. If LABEL = is used, the specified label will be matched exactly
4. If STATE is specified, LOAD status is matched
5. You can use ORDER BY to sort any combination of columns
6. If LIMIT is specified, limit matching records will be displayed. Otherwise, everything is displayed
7. If OFFSET is specified, query results will be displayed starting from offset. By default, the offset is 0.
8. If you are using broker load, the connection in the URL column can be viewed using the following command:
```

```sql
SHOW LOAD WARNINGS ON 'url'
```

## example

1. Show all import tasks of default db

    ```sql
    SHOW LOAD;
    ```

2. Show the import task of the specified db. The label contains the string "2014_01_02", showing the oldest 10

    ```sql
    SHOW LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;
    ```

3. Show the import task of the specified db, specify the label as "load_example_db_20140102" and sort it in descending order by LoadStartTime

    ```sql
    SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" ORDER BY LoadStartTime DESC;
    ````

4. Display the import task of the specified db, specify label as "load_example_db_20140102", state as "loading", and sort by LoadStartTime in descending order

    ```sql
    SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" AND STATE = "loading" ORDER BY LoadStartTime DESC;
    ```

5. Display the import tasks of the specified db, sort them in descending order by LoadStartTime, and display 10 query results starting from offset 5

    ```sql
    SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 5,10;
    SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 10 offset 5;
    ```

6. Small batch import is a command to view the import status.

    ```bash
    curl --location-trusted -u {user}:{passwd} \
        http://{hostname}:{port}/api/{database}/_load_info?label={labelname}
    ```

## keyword

SHOW,LOAD
