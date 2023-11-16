# Data Recovery

StarRocks supports data recovery for mistakenly deleted databases/tables/partitions. After `drop table` or `drop database`, StarRocks will not physically delete the data immediately, but keep it in Trash for a period of time (1 day by default). Administrators can recover the mistakenly deleted data with the `RECOVER` command.

## Related commands

Syntax:

~~~sql
-- 1) Recover database
RECOVER DATABASE db_name;
-- 2) Restore table
RECOVER TABLE [db_name.]table_name;
-- 3) Recover partition
RECOVER PARTITION partition_name FROM [db_name.]table_name;
~~~

## Notes

1. This operation can only restore the deleted meta information. The default time is 1 day, which can be configured by the `catalog_trash_expire_second` parameter in `fe.conf`.
2. If new meta information of the same name and type is created after the meta information is deleted, the previously deleted meta information cannot be restored.

## Examples

1. Recover the database named `example_db`

    ~~~sql
    RECOVER DATABASE example_db;
    ~~~ 2.

2. Recover the table named `example_tbl`

    ~~~sql
    RECOVER TABLE example_db.example_tbl;
    ~~~ 3.

3. Recover the partition named `p1` in the table `example_tbl`

    ~~~sql
    RECOVER PARTITION p1 FROM example_tbl;
    ~~~
