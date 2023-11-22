---
displayed_sidebar: "Chinese"
---

# events

`events` 提供有关 Event Manager 事件的信息。

`events` 提供以下字段：

| 字段                 | 描述                                                         |
| -------------------- | ------------------------------------------------------------ |
| EVENT_CATALOG        | 事件所属的目录的名称。该值始终为 def。                       |
| EVENT_SCHEMA         | 事件所属的模式（数据库）的名称。                             |
| EVENT_NAME           | 事件的名称。                                                 |
| DEFINER              | DEFINER 子句中指定的用户（通常是创建事件的用户）。           |
| TIME_ZONE            | 事件的时区，用于调度事件并在事件执行时生效。默认值为 SYSTEM。 |
| EVENT_BODY           | 事件的 DO 子句中语句所使用的语言。该值始终为 SQL。           |
| EVENT_DEFINITION     | 事件的 DO 子句组成的 SQL 语句的文本；换句话说，此事件执行的语句。 |
| EVENT_TYPE           | 事件的重复类型，可以是 ONE TIME（一次性）或 RECURRING（重复）。 |
| EXECUTE_AT           | 对于一次性事件，这是在创建事件的 CREATE EVENT 语句的 AT 子句中指定的 DATETIME 值，或者在最后一个修改事件的 ALTER EVENT 语句中指定的 DATETIME 值。此列中显示的值反映了事件的 AT 子句中包含的任何 INTERVAL 值的加法或减法。例如，如果使用 ON SCHEDULE AT CURRENT_DATETIME + '1:6' DAY_HOUR 创建事件，并且事件在 2018-02-09 14:05:30 创建，则此列中显示的值将为 '2018-02-10 20:05:30'。如果事件的时间是由 EVERY 子句而不是 AT 子句确定的（即事件是重复的），则此列的值为 NULL。 |
| INTERVAL_VALUE       | 对于重复事件，事件执行之间等待的间隔数。对于一次性事件，该值始终为 NULL。 |
| INTERVAL_FIELD       | 重复事件等待重复之前使用的间隔的时间单位。对于一次性事件，该值始终为 NULL。 |
| SQL_MODE             | 事件创建或更改时有效的 SQL 模式，以及事件执行时使用的 SQL 模式。 |
| STARTS               | 重复事件的开始日期和时间。显示为 DATETIME 值，如果未为事件定义开始日期和时间，则为 NULL。对于一次性事件，此列始终为 NULL。对于定义包含 STARTS 子句的重复事件，此列包含相应的 DATETIME 值。与 EXECUTE_AT 列一样，此值解析了任何使用的表达式。如果没有 STARTS 子句影响事件的时间，此列为 NULL。 |
| ENDS                 | 对于定义包含 ENDS 子句的重复事件，此列包含相应的 DATETIME 值。与 EXECUTE_AT 列一样，此值解析了任何使用的表达式。如果没有 ENDS 子句影响事件的时间，此列为 NULL。 |
| STATUS               | 事件的状态。ENABLED、DISABLED 或 SLAVESIDE_DISABLED 中的一个。SLAVESIDE_DISABLED 表示事件的创建发生在充当复制源的另一台 MySQL 服务器上，并且复制到充当副本的当前 MySQL 服务器，但事件当前未在副本上执行。 |
| ON_COMPLETION        | 有效值：PRESERVE 和 NOT PRESERVE。                           |
| CREATED              | 事件创建的日期和时间。这是一个 DATETIME 值。                 |
| LAST_ALTERED         | 事件上次修改的日期和时间。这是一个 DATETIME 值。如果事件自创建以来未被修改，则此值与 CREATED 值相同。 |
| LAST_EXECUTED        | 事件上次执行的日期和时间。这是一个 DATETIME 值。如果事件从未执行过，则此列为 NULL。LAST_EXECUTED 指示事件何时开始执行。因此，ENDS 列永远不会小于 LAST_EXECUTED。 |
| EVENT_COMMENT        | 事件的注释文本，如果事件有注释。如果没有，则此值为空。       |
| ORIGINATOR           | 事件创建时所在的 MySQL 服务器的服务器 ID；用于复制。如果在复制源上执行，则 ALTER EVENT 可更新此值为该语句发生的服务器的服务器 ID。默认值为 0。 |
| CHARACTER_SET_CLIENT | 创建事件时 character_set_client 系统变量的会话值。           |
| COLLATION_CONNECTION | 创建事件时 collation_connection 系统变量的会话值。           |
| DATABASE_COLLATION   | 与事件关联的数据库的排序规则。                               |
