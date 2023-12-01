---
displayed_sidebar: "English"
---

# CANCEL EXPORT

## Description

Cancels a given data unloading job. Unloading jobs with the state `CANCELLED` or `FINISHED` cannot be canceled. Canceling an unloading job is an asynchronous process. You can use the [SHOW EXPORT](../data-manipulation/SHOW_EXPORT.md) statement to check whether an unloading job is successfully canceled. The unloading job is successfully canceled if the value of `State` is `CANCELLED`.

The CANCEL EXPORT statement requires that you have at least one of the following privileges on the database to which the given unloading job belongs: `SELECT_PRIV`, `LOAD_PRIV`, `ALTER_PRIV`, `CREATE_PRIV`, `DROP_PRIV`, and `USAGE_PRIV`. For more information about privilege descriptions, see [GRANT](../account-management/GRANT.md).

## Syntax

```SQL
CANCEL EXPORT
[FROM db_name]
WHERE QUERYID = "query_id"
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | No           | The name of the databases to which the unloading job belongs. If this parameter is not specified, an unloading job in your current database is canceled. |
| query_id      | Yes          | The query ID of the unloading job. You can obtain the ID by using the [LAST_QUERY_ID()](../../sql-functions/utility-functions/last_query_id.md) function. Note that this function only returns the latest query ID. |

## Examples

Example 1: Cancel an unloading job whose query ID is `921d8f80-7c9d-11eb-9342-acde48001121` in the current database.

```SQL
CANCEL EXPORT
WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001121";
```

Example 2: Cancel an unloading job whose query ID is `921d8f80-7c9d-11eb-9342-acde48001121` in the `example_db` database.

```SQL
CANCEL EXPORT 
FROM example_db 
WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
```
