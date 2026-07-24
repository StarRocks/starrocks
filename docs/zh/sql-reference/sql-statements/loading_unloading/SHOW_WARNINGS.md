---
displayed_sidebar: docs
description: "SHOW WARNINGS 和 SHOW ERRORS 返回当前会话中上一条语句产生的诊断信息，例如导入过程中被过滤或被替换为 NULL 的行。"
---

# SHOW WARNINGS

`SHOW WARNINGS` 显示当前会话中最近一次执行的语句所产生的诊断信息（Note、Warning 和 Error）。`SHOW ERRORS` 是同一条语句，但仅返回 Error 级别的诊断信息。

这两条语句实现了 MySQL 诊断区（Diagnostics Area）语义，因此 MySQL 客户端或通过 `Statement.getWarnings()` 的 JDBC 驱动可以获取上一条语句报告的警告。典型场景是非严格模式下的导入或 `INSERT` 静默过滤了行、或将超出取值范围的值替换为 `NULL`：操作成功，OK 包中报告非零的 `warning_count`，详细信息可以通过 `SHOW WARNINGS` 查看。

## 语法

```SQL
SHOW WARNINGS [LIMIT [offset,] row_count]
SHOW ERRORS [LIMIT [offset,] row_count]
```

## 返回字段

| 字段    | 说明                                                  |
| ------- | ---------------------------------------------------- |
| Level   | 诊断信息的级别：`Note`、`Warning` 或 `Error`。          |
| Code    | 诊断代码。                                            |
| Message | 诊断信息的可读描述。                                   |

警告缓冲区保存上一条语句的诊断信息，并在下一条可能产生诊断信息的语句开始执行时被替换（解析失败的语句会将其替换为自身的错误信息）。不访问表且不产生消息的语句——`SET`、`BEGIN`/`COMMIT`/`ROLLBACK`，以及包括 `SHOW WARNINGS` 和 `SHOW ERRORS` 在内的 `SHOW` 语句——不会改变缓冲区（与 MySQL 一致）。因此 `SHOW WARNINGS` 可以重复执行，并且在 `COMMIT` 之后仍会返回上一次导入的诊断信息。

## 示例

查看非严格模式下过滤了行的 `INSERT` 产生的警告：

```Plain
mysql> SHOW WARNINGS;
+---------+------+------------------------------------------------------------------------+
| Level   | Code | Message                                                                |
+---------+------+------------------------------------------------------------------------+
| Warning | 1265 | 3 row(s) filtered or substituted to NULL during load; tracking_url=... |
+---------+------+------------------------------------------------------------------------+
```

最多返回一行：

```SQL
SHOW WARNINGS LIMIT 1;
```

仅查看 Error 级别的诊断信息。语句失败后，`SHOW ERRORS` 返回其错误（与错误响应中发送的代码和消息相同）：

```Plain
mysql> SELECT * FROM no_such_table;
ERROR 5502 (42602): Getting analyzing error. Detail message: Unknown table 'no_such_table'.

mysql> SHOW ERRORS;
+-------+------+---------------------------------------------------------------------------+
| Level | Code | Message                                                                   |
+-------+------+---------------------------------------------------------------------------+
| Error | 5502 | Getting analyzing error. Detail message: Unknown table 'no_such_table'.  |
+-------+------+---------------------------------------------------------------------------+
```
