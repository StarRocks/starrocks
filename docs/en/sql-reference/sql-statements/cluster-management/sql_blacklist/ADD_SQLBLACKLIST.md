---
displayed_sidebar: docs
---

# ADD SQLBLACKLIST

## Description

Adds a regular expression to the SQL blacklist to forbid certain SQL patterns. When the SQL Blacklist feature is enabled, StarRocks compares all SQL statements to be executed against the SQL regular expressions in the blacklist. StarRocks does not execute SQLs that match any regular expression in the blacklist and returns an error. This prevents certain SQLs from triggering cluster crashes or unexpected behavior.

For more about SQL Blacklist, see [Manage SQL Blacklist](../../../../administration/management/resource_management/Blacklist.md).

:::tip

- This operation requires the SYSTEM-level BLACKLIST privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.
- Currently, StarRocks supports adding SELECT statements to the SQL Blacklist.

:::

## Syntax

```SQL
ADD SQLBLACKLIST "<sql_reg_expr>"
```

## Parameter

`sql_reg_expr`: the regular expression that is used to specify a certain SQL pattern. To distinguish the special characters in the SQL statement and those in the regular expression, you need to use the escape character `\` as a prefix for the special characters in the SQL statement, such as `(`, `)`, and `+`. Whereas `(` and `)` have been often used in SQL statements, StarRocks can identify `(` and `)` in SQL statements directly. You do not need to use the escape character for `(` and `)`.

## Examples

Example 1: Add `count(\*)` to the SQL blacklist.

```Plain
mysql> ADD SQLBLACKLIST "select count(\\*) from .+";
```

Example 2: Add `count(distinct )` to the SQL blacklist.

```Plain
mysql> ADD SQLBLACKLIST "select count(distinct .+) from .+";
```

Example 3: Add `order by limit x, y, 1 <= x <=7, 5 <=y <=7` to the SQL blacklist.

```Plain
mysql> ADD SQLBLACKLIST "select id_int from test_all_type_select1 
    order by id_int 
    limit [1-7], [5-7]";
```

Example 4: Add a complex SQL regular expression to the SQL blacklist. This example is to demonstrate how to use escape characters for `*` and `-` in SQL statements.

```Plain
mysql> ADD SQLBLACKLIST 
    "select id_int \\* 4, id_tinyint, id_varchar 
        from test_all_type_nullable 
    except select id_int, id_tinyint, id_varchar 
        from test_basic 
    except select (id_int \\* 9 \\- 8) \\/ 2, id_tinyint, id_varchar 
        from test_all_type_nullable2 
    except select id_int, id_tinyint, id_varchar 
        from test_basic_nullable";
```
