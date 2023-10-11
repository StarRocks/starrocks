# REFRESH EXTERNAL TABLE

## 功能

该语句用于更新缓存在 StarRocks 中的 Apache Hive™ 和 Apache Hudi 元数据，其主要有以下两个使用场景：

- **外部表**：使用 Hive 外部表和 Hudi 外部表查询 Hive 和 Hudi 数据时， 可使用该语句更新缓存的 Hive 和 Hudi 元数据。
- **External catalog**：使用 [Hive catalog](/data_source/catalog/hive_catalog.md) 和 [Hudi catalog](/data_source/catalog/hudi_catalog.md) 查询 Hive 和 Hudi 数据时，可使用该语句更新缓存的 Hive 和 Hudi 元数据。

> **注意**
>
> 只有拥有对应外表 ALTER 权限的用户才可以执行该操作。

## 基本概念

- **Hive 外部表**：在 StarRocks 中创建并保存的表，用于查询 Hive 集群中的数据。
- **Hudi 外部表**：在 StarRocks 中创建并保存的表，用于查询 Hudi 集群中的数据。
- **Hive 表**：在 Hive 中创建并保存的表。
- **Hudi 表**：在 Hudi 中创建并保存的表。

## 语法和参数说明

在不同的使用场景下，对应语法和参数说明如下：

- 外部表

    ```SQL
    REFRESH EXTERNAL TABLE <table_name>
    [PARTITION ('partition_name', ...)]
    ```

    | **参数**       | **必选** | **说明**                                                     |
    | -------------- | -------- | ------------------------------------------------------------ |
    | table_name     | 是       | Hive 外部表或 Hudi 外部表名。                                |
    | partition_name | 否       | Hive 表或 Hudi 表中的分区名。如指定，则更新缓存的 Hive 表或 Hudi 表指定分区的元数据。 |

- External catalog

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]<table_name>
    [PARTITION ('partition_name', ...)]
    ```

    | **参数**         | **必选** | **说明**                                                     |
    | ---------------- | -------- | ------------------------------------------------------------ |
    | external_catalog | 否       | Hive catalog 或 Hudi catalog 名称。                          |
    | db_name          | 否       | Hive 表或 Hudi 表所在的数据库名。                            |
    | table_name       | 是       | Hive 表或 Hudi 表名。                                        |
    | partition_name   | 否       | Hive 表或 Hudi 表中的分区名。如指定，则更新缓存的 Hive 表或 Hudi 表指定分区的元数据。 |

## 注意事项

只有拥有外表 ALTER 权限的用户才可以执行该语句更新缓存的元数据。

## 示例

在不同使用场景下， 对应的示例如下。

### 外部表

示例一：更新外部表 `hive1` 对应的 Hive 表元数据。

```SQL
REFRESH EXTERNAL TABLE hive1;
```

示例二：更新外部表 `hudi1` 对应的 Hudi 表 `p1` 和 `p2` 分区元数据。

```SQL
REFRESH EXTERNAL TABLE hudi1
PARTITION ('p1', 'p2');
```

### External catalog

示例一：更新缓存的 Hive 表 `hive_table` 的元数据。

```SQL
REFRESH EXTERNAL TABLE hive_catalog.hive_db.hive_table;
```

或

```SQL
USE hive_catalog.hive_db;
REFRESH EXTERNAL TABLE hive_table;
```

示例二：更新缓存的 Hudi 表 `hudi_table` 分区 `p1` 和 `p2` 的元数据。

```SQL
REFRESH EXTERNAL TABLE hudi_catalog.hudi_db.hudi_table
PARTITION ('p1', 'p2');
```
