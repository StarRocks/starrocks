# ADMIN CANCEL REPAIR

## description

This statement is used to cancel repairing specified tables or partitions with high priority.

Syntax:

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];
```

1. Note:
   1. This statement only indicates that the system will no longer repair sharding replicas of specified tables or partitions with high priority. It still repairs these copies by default scheduling.

## example

1. Cancel high priority repair

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```

## keyword

ADMIN,CANCEL,REPAIR
