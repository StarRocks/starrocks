---
displayed_sidebar: docs
---

# ADD BACKEND BLACKLIST

## 功能

将一个 BE 节点添加到 BE 黑名单中。您可以手动将 BE 节点添加到黑名单中，禁止在查询执行中使用该节点，从而避免由于节点断连而引起的查询超时失败或其他意外行为。

此功能自 v3.3.0 起支持。更多信息，参考[管理 BE 黑名单](../../../../administration/management/BE_blacklist.md)。

:::note

该操作需要有 SYSTEM 级 BLACKLIST 权限。

:::

默认情况下，StarRocks 可以自动管理 BE 黑名单，将断连的 BE 节点添加到黑名单中，并在重新连接成功时将其从黑名单中移除。但 StarRocks 不会主动将用户手动加入黑名单的节点移除。

## 语法

```SQL
ADD BACKEND BLACKLIST <be_id>[, ...]
```

## 参数说明

`be_id`：要添加到黑名单中的 BE 节点的 ID。您可以通过执行 [SHOW BACKENDS](SHOW_BACKENDS.md) 语句来获取 BE 节点的 ID。

## 示例

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

## 相关 SQL

- [DELETE BACKEND BLACKLIST](DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKEND BLACKLIST](SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)

