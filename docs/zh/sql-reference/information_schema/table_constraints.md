---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "StarRocks 中未实现的信息架构占位符视图。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# table_constraints

:::note

该视图不适用于 StarRocks 当前支持的功能。

:::

`table_constraints` 描述具有约束的表。

`table_constraints` 提供以下字段：

| 字段               | 描述                                   |
| ------------------ | -------------------------------------- |
| CONSTRAINT_CATALOG | 约束所属的目录的名称。该值始终为 def。 |
| CONSTRAINT_SCHEMA  | 约束所属的数据库的名称。               |
| CONSTRAINT_NAME    | 约束的名称。                           |
| TABLE_SCHEMA       | 表所属的数据库的名称。                 |
| TABLE_NAME         | 表的名称。                             |
| CONSTRAINT_TYPE    | 约束的类型。                           |
