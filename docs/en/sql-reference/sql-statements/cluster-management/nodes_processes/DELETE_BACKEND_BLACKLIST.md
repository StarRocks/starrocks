---
displayed_sidebar: docs
---

# DELETE BACKEND/COMPUTE NOT BLACKLIST

Removes a BE/CN node from the BE Blacklist. Please note that StarRocks will not remove the BE/CN nodes that are manually blacklisted by users.

BE Blacklist is supported from v3.3.0 onwards and CN Blacklist is supported from v4.0 onwards. For more information, see [Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md).

:::note

Only users with the SYSTEM-level BLACKLIST privilege can perform this operation.

:::

## Syntax

```SQL
DELETE { BACKEND | COMPUTE NODE } BLACKLIST { <be_id>[, ...] | <cn_id>[, ...] }
```

## Parameters

`be_id` or `cn_id`: ID of the BE or CN node to be removed from the blacklist. You can obtain the ID of the blacklisted BE/CN by executing [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md).

## Examples

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## Relevant SQLs

- [ADD BACKEND/COMPUTE NODE BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](./SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)

