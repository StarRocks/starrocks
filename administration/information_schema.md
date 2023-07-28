# Information Schema

`information_schema` 是 StarRocks 实例中的一个数据库。该数据库包含数张由系统定义的表，这些表中存储了关于 StarRocks 实例中所有对象的大量元数据信息。

## 通过 Information Schema 查看元数据信息

您可以通过查询 `information_schema` 中的表来查看 StarRocks 实例中的元数据信息。

以下示例通过查询表 `tables` 查看 StarRocks 中名为 `sr_member` 的表相关的元数据信息。

```Plain
mysql> SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'sr_member'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: sr_hub
     TABLE_NAME: sr_member
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: NULL
     TABLE_ROWS: 6
 AVG_ROW_LENGTH: 542
    DATA_LENGTH: 3255
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2022-11-17 14:32:30
    UPDATE_TIME: 2022-11-17 14:32:55
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: NULL
  TABLE_COMMENT: OLAP
1 row in set (1.04 sec)
```

## Information Schema 表

StarRocks 优化了 `information_schema` 中以下表提供的元数据信息：

| **Information Schema 表名** | **描述**                                  |
| --------------------------- | ----------------------------------------- |
| tables                      | 提供常规的表元数据信息。                  |
| tables_config               | 提供额外的 StarRocks 独有的表元数据信息。 |

### tables

表 `tables` 提供以下字段：

| **字段**        | **描述**                                                     |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | 表所属的 Catalog 名称。                                      |
| TABLE_SCHEMA    | 表所属的数据库名称。                                         |
| TABLE_NAME      | 表名。                                                       |
| TABLE_TYPE      | 表的类型。 有效值：“BASE TABLE” 或 “VIEW”。                  |
| ENGINE          | 表的引擎类型。 有效值：“StarRocks”、“MySQL”、“MEMORY”或空字符串。 |
| VERSION         | 该字段暂不可用。                                             |
| ROW_FORMAT      | 该字段暂不可用。                                             |
| TABLE_ROWS      | 表的行数。                                                   |
| AVG_ROW_LENGTH  | 表的平均行长度（大小），等于 `DATA_LENGTH` / `TABLE_ROWS`。 单位：Byte。 |
| DATA_LENGTH     | 表的数据文件长度（大小）。单位：Byte。                         |
| MAX_DATA_LENGTH | 该字段暂不可用。                                             |
| INDEX_LENGTH    | 该字段暂不可用。                                             |
| DATA_FREE       | 该字段暂不可用。                                             |
| AUTO_INCREMENT  | 该字段暂不可用。                                             |
| CREATE_TIME     | 创建表的时间。                                               |
| UPDATE_TIME     | 最后一次更新表的时间。                                       |
| CHECK_TIME      | 最后一次对表进行一致性检查的时间。                           |
| TABLE_COLLATION | 表的默认 Collation。                                         |
| CHECKSUM        | 该字段暂不可用。                                             |
| CREATE_OPTIONS  | 该字段暂不可用。                                             |
| TABLE_COMMENT   | 表的 Comment。                                               |

### tables_config

表 `tables_config` 提供以下字段：

| **字段**         | **描述**                                                     |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | 表所属的数据库名称。                                         |
| TABLE_NAME       | 表名。                                                       |
| TABLE_ENGINE     | 表的引擎类型。                                               |
| TABLE_MODEL      | 表的数据模型。 有效值：“DUP_KEYS”、“AGG_KEYS”、“UNQ_KEYS” 或 “PRI_KEYS”。 |
| PRIMARY_KEY      | 主键模型或更新模型表的主键。如果该表不是主键模型或更新模型表，则返回空字符串。 |
| PARTITION_KEY    | 表的分区键。                                                 |
| DISTRIBUTE_KEY   | 表的分桶键。                                                 |
| DISTRIBUTE_TYPE  | 表的分桶方式。                                               |
| DISTRIBUTE_BUCKET | 表的分桶数。                                                 |
| SORT_KEY         | 表的排序键。                                                 |
| PROPERTIES       | 表的属性。                                                   |
| TABLE_ID         | 表的 ID。                                                   |
