# SHOW VARIABLES

## 功能

查看当前集群的系统变量信息。

## 语法

```SQL
SHOW [ GLOBAL | SESSION ] VARIABLES
    [ LIKE <pattern> | WHERE <expr> ]
```

## 参数说明

| **参数**              | **说明**                                                     |
| --------------------- | ------------------------------------------------------------ |
| 修饰符：<ul><li>GLOBAL</li><li>SESSION</li></ul> | <ul><li>使用 `GLOBAL` 修饰符，该语句显示全局系统变量值。在生成新连接时，StarRocks 自动初始化所有变量的会话值作为全局值。如果变量没有全局值，则不显示任何值。</li><li>使用 `SESSION` 修饰符，该语句显示对当前连接有效的系统变量值。如果变量没有会话值，则显示全局值。您可以使用 `LOCAL` 替代 `SESSION`。</li><li>如不指定修饰符，默认值为 `SESSION`。</li></ul> |
| pattern               | 用于匹配变量名的 LIKE 子句模式。您可以在当前参数中使用 % 通配符。 |
| expr                  | 用于匹配变量名 `variable_name` 或变量值 `value` 的 WHERE 子句匹配表达式。 |

## 返回

| **返回**      | **说明** |
| ------------- | -------- |
| Variable_name | 变量名。 |
| Value         | 变量值。 |

## 示例

示例一：通过 LIKE 子句精确匹配变量名显示变量信息。

```Plain
mysql> SHOW VARIABLES LIKE 'wait_timeout';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.01 sec)
```

示例二：通过 LIKE 子句与 % 通配符模糊匹配变量名显示变量信息。

```Plain
mysql> SHOW VARIABLES LIKE '%imeou%';
+------------------------------------+-------+
| Variable_name                      | Value |
+------------------------------------+-------+
| interactive_timeout                | 3600  |
| net_read_timeout                   | 60    |
| net_write_timeout                  | 60    |
| new_planner_optimize_timeout       | 3000  |
| query_delivery_timeout             | 300   |
| query_queue_pending_timeout_second | 300   |
| query_timeout                      | 300   |
| wait_timeout                       | 28800 |
+------------------------------------+-------+
9 rows in set (0.00 sec)
```

示例三：通过 WHERE 子句精确匹配变量名显示变量信息。

```Plain
mysql> show variables where variable_name = 'wait_timeout';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.17 sec)
```

示例四：通过 WHERE 子句精确匹配变量值显示变量信息。

```Plain
mysql> show variables where value = '28800';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.70 sec)
```
