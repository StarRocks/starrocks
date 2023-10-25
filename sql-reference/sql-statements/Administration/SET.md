# SET

## 功能

为 StarRocks 设置指定的系统变量或用户自定义变量。您可以通过 [SHOW VARIABLES](../Administration/SHOW_VARIABLES.md) 查看 StarRocks 的系统变量。有关系统变量的详细信息，请参阅[系统变量](../../../reference/System_variable.md)。有关用户自定义变量的详细信息，请参阅用户自定义变量。

## 语法

```SQL
SET [ GLOBAL | SESSION ] <variable_name> = <value> [, <variable_name> = <value>] ...
```

## 参数说明

| **参数**              | **说明**                                                     |
| --------------------- | ------------------------------------------------------------ |
| 修饰符：<ul><li>GLOBAL</li><li>SESSION</li></ul> | <ul><li>使用 `GLOBAL` 修饰符，该语句设置该变量值为全局变量值。</li><li>使用 `SESSION` 修饰符，该语句设置该变量值对当前连接有效。您可以使用 `LOCAL` 替代 `SESSION`。</li><li>如不指定修饰符，默认值为 `SESSION`。</li></ul>有关全局变量和会话变量的详细信息，请参阅 [系统变量](../../../reference/System_variable.md)。<br/>**说明**<br/>仅拥有 ADMIN 权限的用户才能设置全局变量。 |
| variable_name         | 变量名。                                                     |
| value                 | 变量值。                                                     |

## 示例

示例一：在当前会话内设置 `time_zone` 为 `Asia/Shanghai`。

```Plain
mysql> SET time_zone = "Asia/Shanghai";
Query OK, 0 rows affected (0.00 sec)
```

示例二：全局设置 `exec_mem_limit` 为 `2147483648`。

```Plain
mysql> SET GLOBAL exec_mem_limit = 2147483648;
Query OK, 0 rows affected (0.00 sec)
```
