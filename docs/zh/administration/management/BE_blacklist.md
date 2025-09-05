---
displayed_sidebar: docs
---

# 管理 BE 和 CN 黑名单

本主题介绍如何管理 BE 和 CN 黑名单。

从 v3.3.0 开始，StarRocks 支持 BE 黑名单功能，该功能允许您禁止在查询执行中使用某些 BE 节点，从而避免由于与 BE 节点的连接失败导致的频繁查询失败或其他意外行为。

从 v4.0 开始，StarRocks 支持将 COMPUTE NODE (CN) 添加到黑名单。

默认情况下，StarRocks 可以自动管理 BE 和 CN 黑名单，将失去连接的 BE 或 CN 节点添加到黑名单，并在连接重新建立时将其从黑名单中移除。但是，如果节点是手动加入黑名单的，StarRocks 将不会自动将其移除。

:::note

- 只有具有 SYSTEM 级别 BLACKLIST 权限的用户才能使用此功能。
- 每个 FE 节点保留自己的 BE 和 CN 黑名单，并且不会与其他 FE 节点共享。

:::

## 将 BE/CN 添加到黑名单

您可以使用 [ADD BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/ADD_BACKEND_BLACKLIST.md) 手动将 BE/CN 节点添加到黑名单。在此语句中，您必须指定要加入黑名单的 BE/CN 节点的 ID。您可以通过执行 [SHOW BACKENDS](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md) 获取 BE ID，通过执行 [SHOW COMPUTE NODES](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_COMPUTE_NODES.md) 获取 CN ID。

示例：

```SQL
-- 获取 BE ID。
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- 将 BE 添加到黑名单。
ADD BACKEND BLACKLIST 10001;

-- 获取 CN ID。
SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10005
                   IP: xxx.xx.xx.xxx
                   ...
-- 将 CN 添加到黑名单。
ADD COMPUTE NODE BLACKLIST 10005;
```

## 从黑名单中移除 BE/CN

您可以使用 [DELETE BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/DELETE_BACKEND_BLACKLIST.md) 手动从黑名单中移除 BE/CN 节点。在此语句中，您也必须指定 BE/CN 节点的 ID。

示例：

```SQL
-- 从黑名单中移除 BE。
DELETE BACKEND BLACKLIST 10001;

-- 从黑名单中移除 CN。
DELETE COMPUTE NODE BLACKLIST 10005;
```

## 查看 BE/CN 黑名单

您可以使用 [SHOW BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKEND_BLACKLIST.md) 查看黑名单中的 BE/CN 节点。

示例：

```SQL
-- 查看 BE 黑名单。
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+

-- 查看 CN 黑名单。
SHOW COMPUTE NODE BLACKLIST;
+---------------+------------------+---------------------+------------------------------+--------------------+
| ComputeNodeId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+---------------+------------------+---------------------+------------------------------+--------------------+
| 10005         | MANUAL           | 2025-08-18 10:47:51 | 0                            | 5                  |
+---------------+------------------+---------------------+------------------------------+--------------------+
```

返回的字段如下：

- `AddBlackListType`：BE/CN 节点被加入黑名单的方式。`MANUAL` 表示由用户手动加入黑名单。`AUTO` 表示由 StarRocks 自动加入黑名单。
- `LostConnectionTime`：
  - 对于 `MANUAL` 类型，表示 BE/CN 节点被手动加入黑名单的时间。
  - 对于 `AUTO` 类型，表示最后一次成功连接建立的时间。
- `LostConnectionNumberInPeriod`：在 `CheckTimePeriod(s)` 内检测到的断开连接次数，即 StarRocks 检查黑名单中 BE/CN 节点连接状态的时间间隔。
- `CheckTimePeriod(s)`：StarRocks 检查黑名单中 BE/CN 节点连接状态的时间间隔。其值等于您为 FE 配置项 `black_host_history_sec` 指定的值。单位：秒。

## 配置 BE/CN 黑名单的自动管理

每当 BE/CN 节点与 FE 节点失去连接，或由于 BE/CN 节点上的超时导致查询失败时，FE 节点会将 BE/CN 节点添加到其 BE 和 CN 黑名单中。FE 节点将通过统计其在特定时间段内的连接失败次数来持续评估黑名单中 BE/CN 节点的连接性。只有当黑名单中的 BE/CN 节点的连接失败次数低于预设的阈值时，StarRocks 才会将其移除。

您可以使用以下 [FE 配置](./FE_configuration.md) 配置 BE 和 CN 黑名单的自动管理：

- `black_host_history_sec`：在黑名单中保留 BE/CN 节点历史连接失败的时间长度。
- `black_host_connect_failures_within_time`：允许黑名单中 BE/CN 节点的连接失败次数阈值。

如果 BE/CN 节点是自动加入黑名单的，StarRocks 将评估其连接性并判断是否可以将其从黑名单中移除。在 `black_host_history_sec` 时间内，只有当黑名单中的 BE/CN 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才可以将其从黑名单中移除。
