# ADMIN CANCEL REPAIR

## Description

This statement is used to cancel repairing specified tables or partitions with high priority.

## Syntax

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

Note
>
> This statement only indicates that the system will no longer repair sharding replicas of specified tables or partitions with high priority. It still repairs these copies by default scheduling.

## Examples

1. Cancel high priority repair

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```
