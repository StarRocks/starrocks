---
displayed_sidebar: docs
---

# ADMIN CHECK TABLET

## Description

This statement is used to check a group of tablets.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN CHECK TABLE (tablet_id1, tablet_id2, ...)
PROPERTIES("type" = "...")
```

Note:

1. The "type" property in tablet id and PROPERTIES must be specified.

2. Currently, "type" only supports:

   Consistency: Check the consistency of replicas of the tablet. This command is asynchronous. After sending it, StarRocks will start checking the consistency among corresponding tablets. The final results will be shown in the InconsistentTabletNum column in the result of SHOW PROC "/statistic".

## Examples

1. Check the consistency of replicas on a group of specified tablets

    ```sql
    ADMIN CHECK TABLET (10000, 10001)
    PROPERTIES("type" = "consistency");
    ```
