---
displayed_sidebar: docs
---

# 管理 BE 黑名单

本文介绍如何管理 BE 黑名单。

从 v3.3.0 版本开始，StarRocks 支持 BE 黑名单功能，允许您在查询执行中禁止使用特定 BE 节点，从而避免由于节点断连而引起的查询超时失败或其他意外行为。

默认情况下，StarRocks 可以自动管理 BE 黑名单，将断连的 BE 节点添加到黑名单中，并在重新连接成功时将其从黑名单中移除。但 StarRocks 不会主动将用户手动加入黑名单的节点移除。

:::note

- 使用该功能需要 SYSTEM 级 BLACKLIST 权限。
- 每个 FE 节点仅维护各自的 BE 黑名单，不与其他 FE 节点共享。

:::

## 添加 BE 节点至黑名单

您可以使用 [ADD BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/ADD_BACKEND_BLACKLIST.md) 语句手动将一个 BE 节点添加到 BE 黑名单中。您需要在此语句中指定该 BE 节点的 ID。您可以通过执行 [SHOW BACKENDS](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md) 语句来获取 BE 节点的 ID。

示例：

```SQL
-- 获取 BE ID。
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- 添加 BE 节点至黑名单。
ADD BACKEND BLACKLIST 10001;
```

## 从黑名单中移除 BE 节点

您可以使用 [DELETE BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/DELETE_BACKEND_BLACKLIST.md) 语句手动将一个 BE 节点从 BE 黑名单中移除。您同样需要在此语句中指定 BE 节点的 ID。

示例：

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## 查看 BE 黑名单

您可以使用 [SHOW BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKEND_BLACKLIST.md) 语句查看 BE 黑名单中的节点。

示例：

```SQL
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+
```

返回以下字段：

- `AddBlackListType`：BE 节点被添加到黑名单的方式。`MANUAL` 表示由用户手动添加到黑名单。`AUTO` 表示由 StarRocks 自动添加到黑名单。
- `LostConnectionTime`：
  - 对于 `MANUAL` 类型，表示 BE 节点被手动添加到黑名单的时间。
  - 对于 `AUTO` 类型，表示最后一次成功建立连接的时间。
- `LostConnectionNumberInPeriod`：在 `CheckTimePeriod(s)` 内检测到的断连次数。
- `CheckTimePeriod(s)`：StarRocks 检查黑名单中 BE 节点连接状态的间隔。其值等于您在 FE 配置项 `black_host_history_sec` 中指定的值。单位：秒。

## 配置 BE 黑名单自动管理

当某个 BE 节点断连，或查询因某个 BE 节点超时而失败时，FE 节点会将该 BE 节点添加到 BE 黑名单中。FE 节点将不断评估黑名单中 BE 节点的连接状态，统计其在一定时间内连接失败的次数。只有当该 BE 节点的连接失败次数低于预先指定的阈值时，StarRocks 才会将其从黑名单中移除。

您可以使用以下 [FE 配置项](./FE_configuration.md)来配置 BE 黑名单的自动管理：

- `black_host_history_sec`：黑名单中 BE 节点连接失败记录的保留时长。
- `black_host_connect_failures_within_time`：黑名单中的 BE 节点允许连接失败的上限。

如果一个 BE 节点被自动添加到 BE 黑名单中，StarRocks 将评估其连接状态，并判断是否可以将其从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当黑名单中的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，StarRocks 才会将其从 BE 黑名单中移除。
