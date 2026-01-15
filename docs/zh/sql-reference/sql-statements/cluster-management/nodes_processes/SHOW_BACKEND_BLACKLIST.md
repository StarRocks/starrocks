---
displayed_sidebar: docs
---

# SHOW BACKEND/COMPUTE NODE BLACKLIST

显示在 BE 和 CN 黑名单中的 BE/CN 节点。

BE 黑名单从 v3.3.0 开始支持，CN 黑名单从 v4.0 开始支持。有关更多信息，请参阅 [Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md)。

:::note

只有具有 SYSTEM 级别 BLACKLIST 权限的用户才能执行此操作。

:::

## Syntax

```SQL
SHOW { BACKEND | COMPUTE NODE } BLACKLIST
```

## Return value

| **Return**                   | **Description**                                              |
| ---------------------------- | ------------------------------------------------------------ |
| AddBlackListType             | BE/CN 节点被添加到黑名单的方式。`MANUAL` 表示由用户手动加入黑名单。`AUTO` 表示由 StarRocks 自动加入黑名单。 |
| LostConnectionTime           | 对于 `MANUAL` 类型，表示 BE/CN 节点被手动加入黑名单的时间。<br />对于 `AUTO` 类型，表示上次成功连接的时间。 |
| LostConnectionNumberInPeriod | 在 `CheckTimePeriod(s)` 内检测到的断开连接次数。 |
| CheckTimePeriod(s)           | StarRocks 检查黑名单中 BE/CN 节点连接状态的间隔。其值为您为 FE 配置项 `black_host_history_sec` 指定的值。单位：秒。 |

## Examples

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

## Relevant SQLs

- [ADD BACKEND/COMPUTE NODE BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [DELETE BACKEND/COMPUTE NODE BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)