---
displayed_sidebar: docs
---

# statistics

:::note

该视图不适用于 StarRocks 当前支持的功能。

:::

`statistics` 提供有关表索引的信息。

`statistics` 提供以下字段：

| 字段          | 描述                                                         |
| ------------- | ------------------------------------------------------------ |
| TABLE_CATALOG | 包含索引的表所属的目录的名称。该值始终为 def。               |
| TABLE_SCHEMA  | 包含索引的表所属的数据库的名称。                             |
| TABLE_NAME    | 包含索引的表的名称。                                         |
| NON_UNIQUE    | 如果索引不能包含重复值，则为 0；如果可以，则为 1。           |
| INDEX_SCHEMA  | 索引所属的数据库的名称。                                     |
| INDEX_NAME    | 索引的名称。如果索引是主键，则名称始终为 PRIMARY。           |
| SEQ_IN_INDEX  | 索引中的列序号，从 1 开始。                                  |
| COLUMN_NAME   | 列名。有关 EXPRESSION 列的描述，请参见下文。                 |
| COLLATION     | 列在索引中的排序方式。可以有值 A（升序）、D（降序）或 NULL（未排序）。 |
| CARDINALITY   | 索引中唯一值的估计数量。                                     |
| SUB_PART      | 索引前缀。即，如果只对列的一部分进行索引，则为索引的字符数，如果整个列都被索引，则为 NULL。 |
| PACKED        | 指示键如何被打包。如果没有打包，则为 NULL。                  |
| NULLABLE      | 如果列可能包含 NULL 值，则为 YES，如果不可能，则为''。       |
| INDEX_TYPE    | 使用的索引方法（BTREE、FULLTEXT、HASH、RTREE）。             |
| COMMENT       | 有关索引的信息，未在其自己的列中描述，例如，如果索引已禁用，则为 disabled。 |
| INDEX_COMMENT | 在创建索引时使用 COMMENT 属性为索引提供的任何注释。          |
