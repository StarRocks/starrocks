---
displayed_sidebar: "Chinese"
---

# tables_config

`tables_config` 提供有关表配置的信息。

`tables_config` 提供以下字段：

| **字段**         | **描述**                                                     |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | 表所属的数据库名称。                                         |
| TABLE_NAME       | 表名。                                                       |
| TABLE_ENGINE     | 表的引擎类型。                                               |
| TABLE_MODEL      | 表的类型。 有效值：`DUP_KEYS`、`AGG_KEYS`、`UNQ_KEYS` 和 `PRI_KEYS`。 |
| PRIMARY_KEY      | 主键模型或更新模型表的主键。如果该表不是主键模型或更新模型表，则返回空字符串。 |
| PARTITION_KEY    | 表的分区键。                                                 |
| DISTRIBUTE_KEY   | 表的分桶键。                                                 |
| DISTRIBUTE_TYPE  | 表的分桶方式。                                               |
| DISTRUBTE_BUCKET | 表的分桶数。                                                 |
| SORT_KEY         | 表的排序键。                                                 |
| PROPERTIES       | 表的属性。                                                   |
| TABLE_ID         | 表的 ID。                                                    |
