---
displayed_sidebar: docs
description: "SHOW WARNINGS 和 SHOW ERRORS 返回当前会话中上一条语句的诊断信息，例如 INSERT 过滤掉或替换为 NULL 的行。"
---

# SHOW WARNINGS

## 功能

`SHOW WARNINGS` 显示当前会话中最近执行的一条语句产生的诊断信息（note、warning 和 error）。`SHOW ERRORS` 是同一条语句，但只返回 error 级别的诊断信息。

这两条语句实现了 MySQL 的 diagnostics area，因此 MySQL 客户端（或通过 `Statement.getWarnings()` 的 JDBC 驱动）可以读取上一条语句报告的诊断信息。有两种情况会写入缓冲区：`INSERT` 过滤掉部分行或将超出范围的值替换为 `NULL` 而没有失败，以及某条语句执行失败时产生的错误。

该操作无需任何权限。

## 语法

```SQL
SHOW WARNINGS [LIMIT [offset,] row_count]
SHOW ERRORS [LIMIT [offset,] row_count]
```

## 参数说明

`LIMIT [offset,] row_count`：最多返回 `row_count` 条诊断信息，并跳过前 `offset` 条。`offset` 为可选参数，默认值为 `0`。

## 返回

| 字段    | 说明                                                 |
| ------- | ---------------------------------------------------- |
| Level   | 诊断信息的级别：`Note`、`Warning` 或 `Error`。       |
| Code    | 诊断信息的代码。                                     |
| Message | 诊断信息的可读描述。                                 |

## 使用说明

缓冲区保存上一条语句的诊断信息，并在下一条语句开始执行时被替换；语句失败时（包括解析失败）则替换为该语句自身的错误信息。以下三类语句执行成功时不会改变缓冲区：`SET`、`BEGIN`/`COMMIT`/`ROLLBACK`，以及包括 `SHOW WARNINGS` 和 `SHOW ERRORS` 在内的 `SHOW` 语句。因此 `SHOW WARNINGS` 可以重复执行，并且在 `COMMIT` 之后仍会返回上一次 `INSERT` 的诊断信息。使用 `USE` 切换数据库同样会替换缓冲区，无论客户端将其作为语句发送还是作为 MySQL `COM_INIT_DB` 命令发送。

- 只有 `INSERT` 会以这种方式报告被过滤的行。Broker Load、Spark Load 和 Routine Load 是异步作业，提交语句仅负责注册作业；Stream Load 则完全没有 SQL 会话。因此这些导入方式都不会为 `SHOW WARNINGS` 留下任何内容，请改用 [SHOW LOAD](../../loading_unloading/SHOW_LOAD.md)。
- 只有在允许的前提下 `INSERT` 才会过滤行。在默认值 `enable_insert_strict = true` 和 `insert_max_filter_ratio = 0` 下，只要有一行不合格，语句就会直接失败，此时缓冲区中保存的是该错误而不是警告。参见第一个示例。
- 对于 `INSERT`，OK 包中的 `warning_count` 表示被过滤或被替换的行数，而 `SHOW WARNINGS` 返回一行汇总信息，两者的数值并不一致。在 MySQL 中，`warning_count` 等于 `SHOW WARNINGS` 返回的行数。
- 诊断信息保存在实际执行该语句的会话中。当会话连接到 Follower FE 时，`INSERT` 会被转发到 Leader，并在 Leader 上一个随请求创建、请求结束即销毁的会话中执行，因此在 Follower 上执行 `SHOW WARNINGS` 会返回空结果，Leader 上也没有可供读取的会话。请从直接连接 Leader FE 的会话执行该 `INSERT`，或打开诊断信息中携带的 tracking URL。转发的语句执行失败时行为相同：Follower 转发 Leader 的错误响应但不携带错误码，因此不会记录该错误，`SHOW ERRORS` 也返回空结果。
- 读取数据过程中产生的诊断信息（例如 `CAST` 溢出后返回 `NULL`）目前尚未记录。

## 示例

示例一：读取 `INSERT` 过滤掉的行。两个会话变量都是必需的，因为在默认值下语句会在遇到第一行不合格数据时失败，而不是将其过滤。

```Plain
mysql> SET enable_insert_strict = false;
Query OK, 0 rows affected (0.00 sec)

mysql> SET insert_max_filter_ratio = 0.5;
Query OK, 0 rows affected (0.00 sec)

mysql> INSERT INTO t_dst SELECT CAST(v AS INT) FROM t_src;
Query OK, 2 rows affected, 1 warning (0.36 sec)

mysql> SHOW WARNINGS;
+---------+------+----------------------------------------------------------------------------------------------------------------------------------------+
| Level   | Code | Message                                                                                                                                |
+---------+------+----------------------------------------------------------------------------------------------------------------------------------------+
| Warning | 1265 | 1 row(s) filtered or substituted to NULL during load; tracking_url=http://172.26.92.1:8040/api/_load_error_log?file=error_log_9a1c2b3d |
+---------+------+----------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

示例二：最多返回一条诊断信息。

```SQL
SHOW WARNINGS LIMIT 1;
```

示例三：读取执行失败语句的错误信息。`SHOW ERRORS` 返回的代码和消息与错误响应中发送的完全一致。

```Plain
mysql> SELECT * FROM no_such_table;
ERROR 5502 (42602): Getting analyzing error. Detail message: Unknown table 'example_db.no_such_table'.

mysql> SHOW ERRORS;
+-------+------+------------------------------------------------------------------------------------+
| Level | Code | Message                                                                            |
+-------+------+------------------------------------------------------------------------------------+
| Error | 5502 | Getting analyzing error. Detail message: Unknown table 'example_db.no_such_table'. |
+-------+------+------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
