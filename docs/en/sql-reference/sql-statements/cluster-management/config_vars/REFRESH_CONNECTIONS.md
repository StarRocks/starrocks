---
displayed_sidebar: docs
---

# REFRESH CONNECTIONS

Refreshes all active connections to apply global variables that have been changed by `SET GLOBAL`. This command allows administrators to update long-lived connections (such as those from connection pools) when runtime parameters are adjusted, without requiring connections to reconnect.

## Background

By default, `SET GLOBAL` in StarRocks only affects newly created connections. Existing sessions keep their local copies of session variables until they disconnect or explicitly run `SET SESSION`. This behavior is consistent with MySQL but makes it difficult to update long-lived connections when administrators adjust runtime parameters.

The `REFRESH CONNECTIONS` command solves this problem by allowing administrators to refresh all active connections to apply the latest global variable values.

:::tip

This command requires the OPERATE privilege on the SYSTEM object.

:::

## Syntax

```SQL
REFRESH CONNECTIONS [FORCE];
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| FORCE         | No           | If specified, forces refresh of all variables even if they have been modified by the session using `SET SESSION`. When `FORCE` is not specified, only variables that haven't been modified by the session are refreshed. |

## Behavior

- **Selective refresh**: By default, the command only refreshes variables that have not been modified by the session itself. Variables that were explicitly set using `SET SESSION` are preserved. When `FORCE` is specified, all variables are refreshed regardless of session modifications.

- **Immediate execution**: Running statements are not affected immediately. The refresh takes effect before the next command is executed.

- **Distribution**: The command is automatically distributed to all FE nodes via RPC, ensuring consistency across the cluster.

- **Privilege requirement**: Only users with OPERATE privilege on the SYSTEM object can execute this command.

## Examples

Example 1: Refresh all connections after changing a global variable.

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS;
Query OK, 0 rows affected (0.00 sec)
```

After executing `REFRESH CONNECTIONS`, all active connections (except those that have modified `query_timeout` in their session) will have their `query_timeout` value updated to 600.

Example 2: Refresh connections after changing multiple global variables.

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> SET GLOBAL exec_mem_limit = 2147483648;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS;
Query OK, 0 rows affected (0.00 sec)
```

Example 3: Force refresh all connections, including those with session-modified variables.

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS FORCE;
Query OK, 0 rows affected (0.00 sec)
```

After executing `REFRESH CONNECTIONS FORCE`, all active connections will have their `query_timeout` value updated to 600, even if they previously modified this variable using `SET SESSION query_timeout = 500`.

## Notes

- By default, variables that have been modified in a session using `SET SESSION` are not refreshed. For example, if a connection has executed `SET SESSION query_timeout = 500`, the `query_timeout` value for that connection will remain 500 even after `REFRESH CONNECTIONS` is executed. However, if you use `REFRESH CONNECTIONS FORCE`, all variables will be refreshed regardless of session modifications.

- When `FORCE` is specified, variables that were previously modified by the session are reset to the global default values, and their modification flags are cleared. This allows future non-forced refreshes to update these variables.

- The command affects all active connections on all FE nodes in the cluster.

- Session-only variables (variables that cannot be set globally) are not affected by this command.

- Variables with the `DISABLE_FORWARD_TO_LEADER` flag are not refreshed.

## Related Statements

- [SET](./SET.md): Set system variables or user-defined variables
- [SHOW VARIABLES](./SHOW_VARIABLES.md): Show system variables
