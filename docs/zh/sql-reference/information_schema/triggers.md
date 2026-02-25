---
displayed_sidebar: docs
---

# triggers

:::note

该视图不适用于 StarRocks 当前支持的功能。

:::

`triggers` 提供有关触发器的信息。

`triggers` 提供以下字段：

| 字段                       | 描述                                                         |
| -------------------------- | ------------------------------------------------------------ |
| TRIGGER_CATALOG            | 触发器所属的目录的名称。该值始终为 def。                     |
| TRIGGER_SCHEMA             | 触发器所属的数据库的名称。                                   |
| TRIGGER_NAME               | 触发器的名称。                                               |
| EVENT_MANIPULATION         | 触发器事件。这是触发器激活的相关表上的操作类型。该值可以是 INSERT（插入了一行）、DELETE（删除了一行）或 UPDATE（修改了一行）。 |
| EVENT_OBJECT_CATALOG       | 每个触发器与确切的一张表关联。这张表所在的目录。             |
| EVENT_OBJECT_SCHEMA        | 每个触发器与确切的一张表关联。这张表所在的数据库。           |
| EVENT_OBJECT_TABLE         | 触发器关联的表的名称。                                       |
| ACTION_ORDER               | 触发器动作在与相同 EVENT_MANIPULATION 和 ACTION_TIMING 值的同一张表上的触发器列表中的顺序位置。 |
| ACTION_CONDITION           | 该值始终为 NULL。                                            |
| ACTION_STATEMENT           | 触发器体；也就是触发器激活时执行的语句。此文本使用 UTF-8 编码。 |
| ACTION_ORIENTATION         | 该值始终为 ROW。                                             |
| ACTION_TIMING              | 触发器是在触发事件之前还是之后激活。该值为 BEFORE 或 AFTER。 |
| ACTION_REFERENCE_OLD_TABLE | 该值始终为 NULL。                                            |
| ACTION_REFERENCE_NEW_TABLE | 该值始终为 NULL。                                            |
| ACTION_REFERENCE_OLD_ROW   | 旧列标识符。该值始终为 OLD。                                 |
| ACTION_REFERENCE_NEW_ROW   | 新列标识符。该值始终为 NEW。                                 |
| CREATED                    | 触发器创建的日期和时间。对于触发器，这是一个 DATETIME(2) 值（百分之一秒的小数部分）。 |
| SQL_MODE                   | 触发器创建时有效的 SQL 模式，以及触发器执行的 SQL 模式。     |
| DEFINER                    | 在 DEFINER 子句中命名的用户（通常是创建触发器的用户）。      |
| CHARACTER_SET_CLIENT       |                                                              |
| COLLATION_CONNECTION       |                                                              |
| DATABASE_COLLATION         | 与触发器关联的数据库的排序规则。                             |
