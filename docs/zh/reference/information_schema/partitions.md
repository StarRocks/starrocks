---
displayed_sidebar: "Chinese"
---

# partitions

`partitions` 提供有关表分区的信息。

`partitions` 提供以下字段：

| 字段                       | 描述                                                         |
| -------------------------- | ------------------------------------------------------------ |
| TABLE_CATALOG              | 表所属的目录的名称。该值始终为 def。                         |
| TABLE_SCHEMA               | 表所属的数据库的名称。                                       |
| TABLE_NAME                 | 包含分区的表的名称。                                         |
| PARTITION_NAME             | 分区的名称。                                                 |
| SUBPARTITION_NAME          | 如果 PARTITIONS 表行表示子分区，则为子分区的名称；否则为 NULL。对于 NDB：该值始终为 NULL。 |
| PARTITION_ORDINAL_POSITION | 所有分区按照定义的顺序进行索引，其中 1 是分配给第一个分区的数字。随着添加、删除和重新组织分区，索引可能会发生变化；此列中显示的数字反映了当前的顺序，考虑到任何索引更改。 |
| PARTITION_METHOD           | 有效值：RANGE、LIST、HASH、LINEAR HASH、KEY 或 LINEAR KEY。  |
| SUBPARTITION_METHOD        | 有效值：HASH、LINEAR HASH、KEY 或 LINEAR KEY。               |
| PARTITION_EXPRESSION       | 用于 CREATE TABLE 或 ALTER TABLE 语句中创建表的当前分区方案的分区函数的表达式。 |
| SUBPARTITION_EXPRESSION    | 对于子分区表达式，其功能与 PARTITION_EXPRESSION 对于定义表分区的分区表达式相同。如果表没有子分区，则此列为 NULL。 |
| PARTITION_DESCRIPTION      | 此列用于 RANGE 和 LIST 分区。对于 RANGE 分区，它包含在分区的 VALUES LESS THAN 子句中设置的值，可以是整数或 MAXVALUE。对于 LIST 分区，此列包含在分区的 VALUES IN 子句中定义的值，这是一个逗号分隔的整数值列表。对于 PARTITION_METHOD 不是 RANGE 或 LIST 的分区，此列始终为 NULL。 |
| TABLE_ROWS                 | 分区中的表行数。                                             |
| AVG_ROW_LENGTH             | 存储在此分区或子分区中的行的平均长度，以字节为单位。这与 DATA_LENGTH 除以 TABLE_ROWS 相同。 |
| DATA_LENGTH                | 存储在此分区或子分区中的所有行的总长度，以字节为单位；即存储在分区或子分区中的字节总数。 |
| MAX_DATA_LENGTH            | 可以存储在此分区或子分区中的最大字节数。                     |
| INDEX_LENGTH               | 此分区或子分区的索引文件长度，以字节为单位。                 |
| DATA_FREE                  | 为分区或子分区分配但未使用的字节数。                         |
| CREATE_TIME                | 创建分区或子分区的时间。                                     |
| UPDATE_TIME                | 上次修改分区或子分区的时间。                                 |
| CHECK_TIME                 | 检查属于此分区或子分区的表的最后一次时间。                   |
| CHECKSUM                   | 校验和值，如果有的话；否则为 NULL。                          |
| PARTITION_COMMENT          | 分区的注释文本，如果有的话。如果没有，则此值为空。           |
| NODEGROUP                  | 分区所属的节点组。                                           |
| TABLESPACE_NAME            | 分区所属的表空间的名称。                                     |
