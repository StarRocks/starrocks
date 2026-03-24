---
displayed_sidebar: docs
---

# ADMIN SHOW TABLET STATUS

## 功能

查看存算分离集群中云原生表或云原生物化视图的 Tablet 状态，包括元数据文件和数据文件是否缺失。

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN SHOW TABLET STATUS FROM [<db_name>.]<table_name>
[PARTITION (<partition_name> [, <partition_name>, ...])]
[WHERE STATUS [=|!=] {'NORMAL'|'MISSING_META'|'MISSING_DATA'}]
[PROPERTIES ("max_missing_data_files_to_show" = "<num>")]
```

## 参数说明

| 参数                            | 说明                                                                                                                                                       |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| db_name                       | 数据库名称。省略时使用当前数据库。                                                                                                                                 |
| table_name                    | 表名或物化视图名。仅支持存算分离集群中的云原生表和云原生物化视图。                                                                                                         |
| PARTITION                     | 指定分区。省略时查看表中所有分区的 Tablet 状态。                                                                                                                   |
| WHERE STATUS [=\|!=]          | 按状态过滤。支持 `=`（等于）和 `!=`（不等于）两种运算符。状态取值见下方**返回结果**说明。                                                                                    |
| max_missing_data_files_to_show | 每个 Tablet 最多展示的缺失数据文件数量。默认值：`5`。设置为 `-1` 时展示所有缺失文件。设置为 `0` 时不展示缺失文件列表，但仍统计缺失文件数量。                                            |

## 返回结果

| 列名                   | 说明                                                                                                          |
| -------------------- | ----------------------------------------------------------------------------------------------------------- |
| TabletId             | Tablet ID。                                                                                                   |
| PartitionId          | Tablet 所属分区的物理分区 ID。                                                                                       |
| Version              | Tablet 当前可见版本。                                                                                             |
| Status               | Tablet 状态：`NORMAL`（元数据和数据文件均正常）、`MISSING_META`（元数据文件缺失）、`MISSING_DATA`（数据文件缺失）。 |
| MissingDataFileCount | 缺失的数据文件数量。`Status` 为 `MISSING_DATA` 时有值。                                                                  |
| MissingDataFiles     | 缺失的数据文件列表。受 `max_missing_data_files_to_show` 限制。`Status` 为 `MISSING_DATA` 时有值。                          |

## 示例

1. 查看表所有 Tablet 的状态。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table;
    ```

2. 查看表指定分区的 Tablet 状态。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table PARTITION (p20250101, p20250102);
    ```

3. 查看表中所有状态异常（非 NORMAL）的 Tablet。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS != "NORMAL";
    ```

4. 查看表中元数据文件缺失的 Tablet。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS = "MISSING_META";
    ```

5. 查看表中数据文件缺失的 Tablet，并展示所有缺失文件路径。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS = "MISSING_DATA"
    PROPERTIES ("max_missing_data_files_to_show" = "-1");
    ```
