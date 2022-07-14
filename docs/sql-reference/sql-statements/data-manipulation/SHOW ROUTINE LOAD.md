# SHOW ROUTINE LOAD

## example

1. Show all routine import jobs (including stopped or cancelled jobs) with the name test1. The result is one or more rows.

    ```sql
    SHOW ALL ROUTINE LOAD FOR test1;
    ```

2. Show the currently running routine import job with the name test1

    ```sql
    SHOW ROUTINE LOAD FOR test1;
    ```

3. Show all routine import jobs (including stopped or cancelled jobs) in example_ db. The result is one or more rows.

    ```sql
    use example_db;
    SHOW ALL ROUTINE LOAD;
    ```

4. Show all running routine import jobs in example_ db

    ```sql
    use example_db;
    SHOW ROUTINE LOAD;
    ```

5. Show the currently running routine import job named test1 in example_ db

    ```sql
    SHOW ROUTINE LOAD FOR example_db.test1;
    ```

6. Show all routine import jobs (including stopped or cancelled jobs) with the name test1. The result is one or more rows in example_ db

    ```sql
    SHOW ALL ROUTINE LOAD FOR example_db.test1;
    ```

## keyword

SHOW,ROUTINE,LOAD
