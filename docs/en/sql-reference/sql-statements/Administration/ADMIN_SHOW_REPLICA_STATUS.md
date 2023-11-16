---
displayed_sidebar: "English"
---

# ADMIN SHOW REPLICA STATUS

## Description

This statement is used to show the status of replicas for a table or a partition.

Syntax:

```sql
ADMIN SHOW REPLICA STATUS FROM [db_name.]tbl_name [PARTITION (p1, ...)]
[where_clause]
```

```sql
where_clause:
WHERE STATUS [!]= "replica_status"
```

```plain text
replica_status:
OK:            The replica is healthy
DEAD:          The Backend of replica is not available
VERSION_ERROR: The replica data version is missing
SCHEMA_ERROR:  The schema hash of replica is incorrect
MISSING:       The replica does not exist
```

## Examples

1. View the status of all replicas of the table.

    ```sql
    ADMIN SHOW REPLICA STATUS FROM db1.tbl1;
    ```

2. View the replica of a partition with the status of VERSION_ERROR.

    ```sql
    ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)
    WHERE STATUS = "VERSION_ERROR";
    ```

3. View all unhealthy replicas of the table.

    ```sql
    ADMIN SHOW REPLICA STATUS FROM tbl1
    WHERE STATUS != "OK";
    ```
