---
displayed_sidebar: docs
---

# DELETE BACKEND/COMPUTE NOT BLACKLIST

从 BE 黑名单中移除一个 BE/CN 节点。请注意，StarRocks 不会移除用户手动加入黑名单的 BE/CN 节点。

BE 黑名单从 v3.3.0 开始支持，CN 黑名单从 v4.0 开始支持。更多信息，请参见 [Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md)。

:::note

只有拥有 SYSTEM 级别 BLACKLIST 权限的用户才能执行此操作。

:::

## Syntax

```SQL
DELETE { BACKEND | COMPUTE NODE } BLACKLIST { <be_id>[, ...] | <cn_id>[, ...] }
```

## Parameters

`be_id` 或 `cn_id`：要从黑名单中移除的 BE 或 CN 节点的 ID。您可以通过执行 [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md) 获取被列入黑名单的 BE/CN 的 ID。

## Examples

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## Relevant SQLs

- [ADD BACKEND/COMPUTE NODE BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](./SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)