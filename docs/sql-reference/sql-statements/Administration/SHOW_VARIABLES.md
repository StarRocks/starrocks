---
displayed_sidebar: "English"
---

# SHOW VARIABLES

## Description

Shows the system variables of StarRocks. For details about system variables, see [System Variables](../../../sql-reference/System_variable.md).

## Syntax

```SQL
SHOW [ GLOBAL | SESSION ] VARIABLES
    [ LIKE <pattern> | WHERE <expr> ]
```

## Parameters

| **Parameter**          | **Description**                                              |
| ---------------------- | ------------------------------------------------------------ |
| Modifier:<ul><li>GLOBAL</li><li>SESSION</li></ul> | <ul><li>With a `GLOBAL` modifier, the statement displays global system variable values. These are the values used to initialize the corresponding session variables for new connections to StarRocks. If a variable has no global value, no value is displayed.</li><li>With a `SESSION` modifier, the statement displays the system variable values that are in effect for the current connection. If a variable has no session value, the global value is displayed. `LOCAL` is a synonym for `SESSION`.</li><li>If no modifier is present, the default is `SESSION`.</li></ul> |
| pattern                | The pattern used to match the variable by the variable name with the LIKE clause. You can use the % wildcard in this parameter. |
| expr                   | The expression used to match the variable by the variable name `variable_name` or the variable value `value` with the WHERE clause. |

## Return value

| **Return**    | **Description**            |
| ------------- | -------------------------- |
| Variable_name | The name of the variable.  |
| Value         | The value of the variable. |

## Examples

Example 1: Show a variable by exactly matching the variable name with the LIKE clause.

```Plain
mysql> SHOW VARIABLES LIKE 'wait_timeout';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.01 sec)
```

Example 2: Show variables by approximately matching the variable name with the LIKE clause and the wildcard (%).

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
| tx_visible_wait_timeout            | 10    |
| wait_timeout                       | 28800 |
+------------------------------------+-------+
9 rows in set (0.00 sec)
```

Example 3: Show a variable by exactly matching the variable name with the WHERE clause.

```Plain
mysql> show variables where variable_name = 'wait_timeout';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.17 sec)
```

Example 4: Show a variable by exactly matching the value of the variable with the WHERE clause.

```Plain
mysql> show variables where value = '28800';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.70 sec)
```
