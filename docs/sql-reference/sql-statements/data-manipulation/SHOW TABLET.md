# SHOW TABLET

## Description

This statement is used to display tablet related information (for administrators only)

Syntax:

```sql
SHOW TABLET
[FROM [db_name.]table_name | tablet_id] [partition(partition_name_1, partition_name_1)]
[where [version=1] [and backendid=10000] [and state="NORMAL|ROLLUP|CLONE|DECOMMISSION"]]
[order by order_column]
[limit [offset,]size]
```

Now the show tablet command supports filtering according to the following fields: partition, index, name, version, backendid, state. It also supports sorting according to any field, and provides a limit to limit the number of returned items.

## Examples

1. Displays all the table information of the specified db table

    ```sql
    SHOW TABLET FROM example_db.table_name;
    ```

    ```plain text
    // Get the tablet information of partition p1 and p2
    SHOW TABLET FROM example_db.table_name partition(p1, p2);
    
    // Get 10 results
    SHOW TABLET FROM example_db.table_name limit 10;
    
    // Get 10 results from offset 5
    SHOW TABLET FROM example_db.table_name limit 5,10;
    
    // Filter by backendid / version / state field
    SHOW TABLET FROM example_db.table_name where backendid=10000 and version=1 and state="NORMAL";
    
    // Sort by version field
    SHOW TABLET FROM example_db.table_name where backendid=10000 order by version;
    
    // Get the index name as T1_ Rollup tablet related information
    SHOW TABLET FROM example_db.table_name where indexname="t1_rollup";
    ```

2. Displays the parent level id information of a tablet with a specified tablet id of 10000

    ```sql
    SHOW TABLET 10000;
    ```
