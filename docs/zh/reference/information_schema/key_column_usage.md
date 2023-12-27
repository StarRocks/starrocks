---
displayed_sidebar: "Chinese"
---

# key_column_usage

`key_column_usage` 用于识别受某些唯一、主键或外键约束限制的所有列。

`key_column_usage` 提供以下字段：

| 字段                          | 描述                                                         |
| ----------------------------- | ------------------------------------------------------------ |
| CONSTRAINT_CATALOG            | 约束所属的目录的名称。该值始终为 def。                       |
| CONSTRAINT_SCHEMA             | 约束所属的数据库的名称。                                     |
| CONSTRAINT_NAME               | 约束的名称。                                                 |
| TABLE_CATALOG                 | 表所属的目录的名称。该值始终为 def。                         |
| TABLE_SCHEMA                  | 表所属的数据库的名称。                                       |
| TABLE_NAME                    | 具有约束的表的名称。                                         |
| COLUMN_NAME                   | 具有约束的列的名称。如果约束是外键，则这是外键的列，而不是外键引用的列。 |
| ORDINAL_POSITION              | 列在约束内的位置，而不是列在表内的位置。列位置从 1 开始编号。 |
| POSITION_IN_UNIQUE_CONSTRAINT | 对于唯一和主键约束，为 NULL。对于外键约束，此列是被引用的表中 key 的序数位置。 |
| REFERENCED_TABLE_SCHEMA       | 约束引用的模式的名称。                                       |
| REFERENCED_TABLE_NAME         | 约束引用的表的名称。                                         |
| REFERENCED_COLUMN_NAME        | 约束引用的列的名称。                                         |
