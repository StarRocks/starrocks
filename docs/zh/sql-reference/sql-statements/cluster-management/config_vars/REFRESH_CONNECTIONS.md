---
displayed_sidebar: docs
---

# REFRESH CONNECTIONS

刷新所有活动连接以应用通过 `SET GLOBAL` 更改的全局变量。该命令允许管理员在调整运行时参数时更新长连接（如来自连接池的连接），而无需重新连接。

## 背景

默认情况下，StarRocks 中的 `SET GLOBAL` 仅影响新创建的连接。现有会话会保留其本地会话变量副本，直到断开连接或显式执行 `SET SESSION`。此行为与 MySQL 一致，但在管理员调整运行时参数时，很难更新长连接。

`REFRESH CONNECTIONS` 命令通过允许管理员刷新所有活动连接以应用最新的全局变量值来解决此问题。

:::tip

该命令需要 SYSTEM 对象的 OPERATE 权限。

:::

## 语法

```SQL
REFRESH CONNECTIONS [FORCE];
```

## 参数说明

| **参数** | **必选** | **说明**                                              |
| -------- | -------- | ------------------------------------------------------------ |
| FORCE     | 否       | 如果指定，即使变量已通过 `SET SESSION` 在会话中修改，也会强制刷新所有变量。如果不指定 `FORCE`，则只刷新未被会话修改的变量。 |

## 行为说明

- **选择性刷新**：默认情况下，该命令仅刷新会话本身未修改的变量。使用 `SET SESSION` 显式设置的变量将被保留。当指定 `FORCE` 时，无论会话是否修改过，都会刷新所有变量。

- **立即执行**：正在运行的语句不会立即受到影响。刷新会在执行下一个命令之前生效。

- **分布式执行**：该命令通过 RPC 自动分发到所有 FE 节点，确保整个集群的一致性。

- **权限要求**：只有拥有 SYSTEM 对象 OPERATE 权限的用户才能执行此命令。

## 示例

示例一：更改全局变量后刷新所有连接。

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS;
Query OK, 0 rows affected (0.00 sec)
```

执行 `REFRESH CONNECTIONS` 后，所有活动连接（已在其会话中修改 `query_timeout` 的连接除外）的 `query_timeout` 值将更新为 600。

示例二：更改多个全局变量后刷新连接。

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> SET GLOBAL exec_mem_limit = 2147483648;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS;
Query OK, 0 rows affected (0.00 sec)
```

示例三：强制刷新所有连接，包括已通过会话修改变量的连接。

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS FORCE;
Query OK, 0 rows affected (0.00 sec)
```

执行 `REFRESH CONNECTIONS FORCE` 后，所有活动连接的 `query_timeout` 值将更新为 600，即使它们之前使用 `SET SESSION query_timeout = 500` 修改过此变量。

## 注意事项

- 默认情况下，已在会话中使用 `SET SESSION` 修改的变量不会被刷新。例如，如果连接已执行 `SET SESSION query_timeout = 500`，即使执行 `REFRESH CONNECTIONS` 后，该连接的 `query_timeout` 值仍将保持为 500。但是，如果使用 `REFRESH CONNECTIONS FORCE`，无论会话是否修改过，都会刷新所有变量。

- 当指定 `FORCE` 时，之前被会话修改的变量将被重置为全局默认值，并清除其修改标志。这使得后续的非强制刷新可以更新这些变量。

- 该命令会影响集群中所有 FE 节点上的所有活动连接。

- 仅会话变量（无法全局设置的变量）不受此命令影响。

- 具有 `DISABLE_FORWARD_TO_LEADER` 标志的变量不会被刷新。

## 相关语句

- [SET](./SET.md)：设置系统变量或用户自定义变量
- [SHOW VARIABLES](./SHOW_VARIABLES.md)：显示系统变量
