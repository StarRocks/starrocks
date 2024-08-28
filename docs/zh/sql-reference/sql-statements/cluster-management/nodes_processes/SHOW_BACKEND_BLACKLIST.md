---
displayed_sidebar: docs
---

# SHOW BACKEND BLACKLIST

## 功能

查看 BE 黑名单中的节点。

此功能自 v3.3.0 起支持。更多信息，参考[管理 BE 黑名单](../../../../administration/management/BE_blacklist.md)。

:::note

该操作需要有 SYSTEM 级 BLACKLIST 权限。

:::

## 语法

```SQL
SHOW BACKEND BLACKLIST
```

## 返回

| **返回**                     | **说明**                                                     |
| ---------------------------- | ------------------------------------------------------------ |
| AddBlackListType             | BE 节点被添加到黑名单的方式。`MANUAL` 表示由用户手动添加到黑名单。`AUTO` 表示由 StarRocks 自动添加到黑名单。 |
| LostConnectionTime           | 对于 `MANUAL` 类型，表示 BE 节点被手动添加到黑名单的时间。<br />对于 `AUTO` 类型，表示最后一次成功建立连接的时间。 |
| LostConnectionNumberInPeriod | 在 `CheckTimePeriod(s)` 内检测到的断连次数。                 |
| CheckTimePeriod(s)           | StarRocks 检查黑名单中 BE 节点连接状态的间隔。其值等于您在 FE 配置项 `black_host_history_sec` 中指定的值。单位：秒。 |

## 示例

```SQL
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+
```

## 相关 SQL

- [ADD BACKEND BLACKLIST](ADD_BACKEND_BLACKLIST.md)
- [DELETE BACKEND BLACKLIST](DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](./SHOW_BACKENDS.md)

