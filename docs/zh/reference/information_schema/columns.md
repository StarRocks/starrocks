---
displayed_sidebar: "Chinese"
---

# columns

`columns` 包含有关所有表（或视图）中列的信息。

`columns` 提供以下字段：

| 字段                     | 描述                                                         |
| ------------------------ | ------------------------------------------------------------ |
| TABLE_CATALOG            | 包含列的表所属的目录的名称。该值始终为 NULL。                |
| TABLE_SCHEMA             | 包含列的表所属的模式（数据库）的名称。                       |
| TABLE_NAME               | 包含列的表的名称。                                           |
| COLUMN_NAME              | 列的名称。                                                   |
| ORDINAL_POSITION         | 列在表内的顺序位置。                                         |
| COLUMN_DEFAULT           | 列的默认值。如果列具有显式的 NULL 默认值，或者列定义中不包含 DEFAULT 子句，则为 NULL。 |
| IS_NULLABLE              | 列是否可为 NULL。如果可以在列中存储 NULL 值，则值为 `YES`，否则为 `NO`。 |
| DATA_TYPE                | 列的数据类型。<br />`DATA_TYPE` 值仅为类型名称，不包含其他信息。`COLUMN_TYPE` 值包含类型名称，可能还包含其他信息，如精度或长度。 |
| CHARACTER_MAXIMUM_LENGTH | 对于字符串列，字符的最大长度。                               |
| CHARACTER_OCTET_LENGTH   | 对于字符串列，字节的最大长度。                               |
| NUMERIC_PRECISION        | 对于数值列，数值的精度。                                     |
| NUMERIC_SCALE            | 对于数值列，数值的刻度。                                     |
| DATETIME_PRECISION       | 对于时间列，小数秒的精度。                                   |
| CHARACTER_SET_NAME       | 对于字符字符串列，字符集的名称。                             |
| COLLATION_NAME           | 对于字符字符串列，排序规则的名称。                           |
| COLUMN_TYPE              | 列的数据类型。<br />`DATA_TYPE` 值仅为类型名称，不包含其他信息。`COLUMN_TYPE` 值包含类型名称，可能还包含其他信息，如精度或长度。 |
| COLUMN_KEY               | 列是否已建立索引：<br />如果 `COLUMN_KEY` 为空，则该列未建立索引，或仅作为多列非唯一索引中的次要列建立索引。<br />如果 `COLUMN_KEY` 为 `PRI`，则该列是主键，或是多列主键中的一列。<br />如果 `COLUMN_KEY` 为 `UNI`，则该列是唯一索引的第一列。<br />如果 `COLUMN_KEY` 为 `DUP`，则该列是允许在列内有多个相同值的非唯一索引的第一列。<br />如果给定表的列的多个 `COLUMN_KEY` 值都适用，则 `COLUMN_KEY` 会显示具有最高优先级的值，按照 `PRI`、`UNI`、`DUP` 的顺序。<br />如果 UNIQUE 索引不允许包含 NULL 值，并且表中没有主键，则唯一索引可能显示为 `PRI`。如果多个列形成复合唯一索引，则尽管这些列的组合是唯一的，每个列仍然可以包含多个相同值，因此可能显示为 `MUL`。 |
| EXTRA                    | 关于给定列的任何其他信息。                                   |
| PRIVILEGES               | 您对该列拥有的权限。                                         |
| COLUMN_COMMENT           | 列定义中包含的任何注释。                                     |
| COLUMN_SIZE              |                                                              |
| DECIMAL_DIGITS           |                                                              |
| GENERATION_EXPRESSION    | 对于生成的列，显示用于计算列值的表达式。非生成列为空。       |
| SRS_ID                   | 此值适用于空间列。它包含列 SRID 值，指示存储在列中的值的空间参考系统。 |
