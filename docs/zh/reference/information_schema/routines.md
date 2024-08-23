---
displayed_sidebar: docs
---

# routines

:::note

该视图不适用于 StarRocks 当前支持的功能。

:::

`routines` 包含所有存储的过程（Routine），包括流程和函数。

`routine` 提供以下字段：

| 字段                 | 描述                                                         |
| -------------------- | ------------------------------------------------------------ |
| SPECIFIC_NAME        | 过程的名称。                                                 |
| ROUTINE_CATALOG      | 过程所属的目录的名称。该值始终为 def。                       |
| ROUTINE_SCHEMA       | 过程所属的数据库的名称。                                     |
| ROUTINE_NAME         | 过程的名称。                                                 |
| ROUTINE_TYPE         | 存储过程的类型为 PROCEDURE，存储函数的类型为 FUNCTION。      |
| DTD_IDENTIFIER       | 如果过程是存储函数，则为返回值的数据类型。如果过程是存储过程，则此值为空。 |
| ROUTINE_BODY         | 用于过程定义的语言。此值始终为 SQL。                         |
| ROUTINE_DEFINITION   | 过程执行的 SQL 语句的文本。                                  |
| EXTERNAL_NAME        | 此值始终为 NULL。                                            |
| EXTERNAL_LANGUAGE    | 存储过程的语言。                                             |
| PARAMETER_STYLE      | 此值始终为 SQL。                                             |
| IS_DETERMINISTIC     | 取决于是否使用 DETERMINISTIC 特性定义了过程。可以是 YES 或 NO。 |
| SQL_DATA_ACCESS      | 过程的数据访问特性。该值是 CONTAINS SQL、NO SQL、READS SQL DATA 或 MODIFIES SQL DATA 中的一个。 |
| SQL_PATH             | 此值始终为 NULL。                                            |
| SECURITY_TYPE        | 过程的 SQL SECURITY 特性。该值是 DEFINER 或 INVOKER 中的一个。 |
| CREATED              | 过程创建的日期和时间。这是一个 DATETIME 值。                 |
| LAST_ALTERED         | 过程上次修改的日期和时间。这是一个 DATETIME 值。如果自创建以来过程未被修改，则此值与 CREATED 值相同。 |
| SQL_MODE             | 过程创建或修改时生效的 SQL 模式，并在其中运行过程。          |
| ROUTINE_COMMENT      | 过程的注释文本，如果有的话。如果没有，则此值为空。           |
| DEFINER              | 在 DEFINER 子句中指定的用户（通常是创建过程的用户）。        |
| CHARACTER_SET_CLIENT |                                                              |
| COLLATION_CONNECTION |                                                              |
| DATABASE_COLLATION   | 过程关联的数据库的排序规则。                                 |

