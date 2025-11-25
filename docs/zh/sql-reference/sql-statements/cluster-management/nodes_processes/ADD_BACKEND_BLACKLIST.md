---
displayed_sidebar: docs
---

# ADD BACKEND/COMPUTE NODE BLACKLIST

将 BE 或 CN 节点添加到 BE 和 CN 黑名单中。您可以手动将 BE/CN 节点添加到黑名单，以禁止在查询执行中使用这些节点，从而避免由于节点连接失败导致的频繁查询失败或其他意外行为。

BE 黑名单从 v3.3.0 开始支持，CN 黑名单从 v4.0 开始支持。更多信息，请参见 [Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md)。

:::note

只有具有 SYSTEM 级别 BLACKLIST 权限的用户才能执行此操作。

:::

默认情况下，StarRocks 可以自动管理 BE 和 CN 黑名单，将失去连接的 BE/CN 节点添加到黑名单中，并在连接重新建立时将其从黑名单中移除。然而，如果节点是手动加入黑名单的，StarRocks 不会将其从黑名单中移除。

## 语法

```SQL
ADD { BACKEND | COMPUTE NODE } BLACKLIST { <be_id>[, ...] | <cn_id>[, ...] }
```

## 参数

`be_id` 或 `cn_id`：要加入黑名单的 BE 或 CN 节点的 ID。您可以通过执行 [SHOW BACKENDS](./SHOW_BACKENDS.md) 获取 BE ID，通过执行 [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md) 获取 CN ID。

## 示例

```SQL
-- 获取 BE ID。
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- 将 BE 加入黑名单。
ADD BACKEND BLACKLIST 10001;

-- 获取 CN ID。
SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10005
                   IP: xxx.xx.xx.xxx
                   ...
-- 将 CN 加入黑名单。
ADD COMPUTE NODE BLACKLIST 10005;
```

## 相关 SQL

- [DELETE BACKEND/COMPUTE NODE BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](./SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)