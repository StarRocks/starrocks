---
displayed_sidebar: docs
---

# DELETE BACKEND BLACKLIST

## 功能

从 BE 黑名单中移除 BE 节点。请注意，StarRocks 不会主动将用户手动加入黑名单的节点移除。

此功能自 v3.3.0 起支持。更多信息，参考[管理 BE 黑名单](../../../../administration/management/BE_blacklist.md)。

:::note

该操作需要有 SYSTEM 级 BLACKLIST 权限。

:::

## 语法

```SQL
DELETE BACKEND BLACKLIST <be_id>[, ...]
```

## 参数说明

`be_id`：要从黑名单中移除的 BE 节点的 ID。您可以通过执行 [SHOW BACKEND BLACKLIST](SHOW_BACKEND_BLACKLIST.md) 语句获取被列入黑名单的 BE 的 ID。

## 示例

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## 相关 SQL

- [ADD BACKEND BLACKLIST](ADD_BACKEND_BLACKLIST.md)
- [SHOW BACKEND BLACKLIST](SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)

