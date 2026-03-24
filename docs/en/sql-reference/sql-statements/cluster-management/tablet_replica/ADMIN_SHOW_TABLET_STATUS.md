---
displayed_sidebar: docs
---

# ADMIN SHOW TABLET STATUS

## Description

Shows the status of Tablets in a cloud-native table or cloud-native materialized view in a shared-data cluster, including whether metadata or data files are missing.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SHOW TABLET STATUS FROM [<db_name>.]<table_name>
[PARTITION (<partition_name> [, <partition_name>, ...])]
[WHERE STATUS [=|!=] {'NORMAL'|'MISSING_META'|'MISSING_DATA'}]
[PROPERTIES ("max_missing_data_files_to_show" = "<num>")]
```

## Parameters

| Parameter                      | Description                                                                                                                                                              |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| db_name                        | Database name. If omitted, the current database is used.                                                                                                                 |
| table_name                     | Table or materialized view name. Only cloud-native tables and cloud-native materialized views in shared-data clusters are supported.                                      |
| PARTITION                      | Partitions to check. If omitted, all partitions in the table are checked.                                                                                                |
| WHERE STATUS [=\|!=]           | Filter tablets by status. Supports `=` (equals) and `!=` (not equals). See **Return columns** for valid status values.                                                   |
| max_missing_data_files_to_show | Maximum number of missing data files to display per tablet. Default: `5`. Set to `-1` to show all missing files. Set to `0` to suppress the file list while still counting missing files. |

## Return Columns

| Column               | Description                                                                                                                    |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| TabletId             | Tablet ID.                                                                                                                     |
| PartitionId          | Physical partition ID of the partition the Tablet belongs to.                                                                  |
| Version              | Current visible version of the Tablet.                                                                                         |
| Status               | Tablet status: `NORMAL` (metadata and data files are all intact), `MISSING_META` (metadata file is missing), `MISSING_DATA` (data file(s) are missing). |
| MissingDataFileCount | Number of missing data files. Populated when `Status` is `MISSING_DATA`.                                                      |
| MissingDataFiles     | List of missing data file paths. Limited by `max_missing_data_files_to_show`. Populated when `Status` is `MISSING_DATA`.      |

## Examples

1. Show the status of all tablets in a table.

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table;
    ```

2. Show the status of tablets in specified partitions.

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table PARTITION (p20250101, p20250102);
    ```

3. Show all abnormal tablets (non-NORMAL) in a table.

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS != "NORMAL";
    ```

4. Show tablets with missing metadata files.

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS = "MISSING_META";
    ```

5. Show tablets with missing data files and display all missing file paths.

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS = "MISSING_DATA"
    PROPERTIES ("max_missing_data_files_to_show" = "-1");
    ```
