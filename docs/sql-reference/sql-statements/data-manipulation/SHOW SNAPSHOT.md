# SHOW SNAPSHOT

## description

This statement is used to view backups that already exist in the repository.

Syntax:

```sql
SHOW SNAPSHOT ON `repo_name`
[WHERE SNAPSHOT = "snapshot" [AND TIMESTAMP = "backup_timestamp"]];
```

Note：

```plain text
1.The meanings of each column are as follows:
Snapshot：   name of the backup
Timestamp：  the time version of the corresponding backup
Status：     if the backup is normal, it displays OK; otherwise, it displays an error message

2.2. If TIMESTAMP is specified, the following additional information will be displayed:
Database：   the name of the database to which the backup data belongs
Details：    display the data directory and file structure of the whole backup in the form of Json
```

## example

1. View existing backups in the example_repo repository:

    ```sql
    SHOW SNAPSHOT ON example_repo;
    ```

2. View only the backup named backup1 in the example_repo repository:

    ```sql
    SHOW SNAPSHOT ON example_repo WHERE SNAPSHOT = "backup1";
    ```

3. View the details of the backup named backup1 in the example_repo repository with the time version of "2018-05-05-15-34-26":

    ```sql
    SHOW SNAPSHOT ON example_repo
    WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
    ```

## keyword

SHOW, SNAPSHOT
