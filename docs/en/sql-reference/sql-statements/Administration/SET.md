# SET

## Description

Sets the specified system variables or user-defined variables for StarRocks. You can view the system variables of StarRocks using [SHOW VARIABLES](../Administration/SHOW_VARIABLES.md). For details about system variables, see [System Variables](../../../reference/System_variable.md). For details about user-defined variables, see [User-defined variables](../../../reference/user_defined_variables.md).

## Syntax

```SQL
SET [ GLOBAL | SESSION ] <variable_name> = <value> [, <variable_name> = <value>] ...
```

## Parameters

| **Parameter**          | **Description**                                              |
| ---------------------- | ------------------------------------------------------------ |
| Modifier:<ul><li>GLOBAL</li><li>SESSION</li></ul> | <ul><li>With a `GLOBAL` modifier, the statement sets the variables globally.</li><li>With a `SESSION` modifier, the statement sets the variables within the session. `LOCAL` is a synonym for `SESSION`.</li><li>If no modifier is present, the default is `SESSION`.</li></ul>For details about global and session variables, see [System Variables](../../../reference/System_variable.md).<br/>**NOTE**<br/>Only users with the ADMIN privilege can set the variables globally. |
| variable_name          | The name of the variable.                                    |
| value                  | The value of the variable.                                   |

## Examples

Example 1: Set the `time_zone` to `Asia/Shanghai` within the session.

```Plain
mysql> SET time_zone = "Asia/Shanghai";
Query OK, 0 rows affected (0.00 sec)
```

Example 2: Set the `exec_mem_limit` to `2147483648` globally.

```Plain
mysql> SET GLOBAL exec_mem_limit = 2147483648;
Query OK, 0 rows affected (0.00 sec)
```
