---
displayed_sidebar: docs
---

# ADD BACKEND/COMPUTE NOTE BLACKLIST

Adds a BE or CN node to the BE and CN Blacklist. You can manually add BE/CN nodes to the blacklist to forbid the usage of the nodes in query execution, thereby avoiding frequent query failures or other unexpected behaviors caused by failed connections to the nodes.

BE Blacklist is supported from v3.3.0 onwards and CN Blacklist is supported from v4.0 onwards. For more information, see [Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md).

:::note

Only users with the SYSTEM-level BLACKLIST privilege can perform this operation.

:::

By default, StarRocks can automatically manage the BE and CN Blacklist, adding the BE/CN nodes that have lost connection to the blacklist and removing them from the blacklist when the connection is reestablished. However, StarRocks will not remove the node from the blacklist if it is manually blacklisted.

## Syntax

```SQL
ADD { BACKEND | COMPUTE NODE } BLACKLIST { <be_id>[, ...] | <cn_id>[, ...] }
```

## Parameters

`be_id` or `cn_id`: ID of the BE or CN node to be blacklisted. You can obtain the BE ID by executing [SHOW BACKENDS](./SHOW_BACKENDS.md) and CN ID by executing [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md).

## Examples

```SQL
-- Obtain BE ID.
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- Add BE to the blacklist.
ADD BACKEND BLACKLIST 10001;

-- Obtain CN ID.
SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10005
                   IP: xxx.xx.xx.xxx
                   ...
-- Add CN to the blacklist.
ADD COMPUTE NODE BLACKLIST 10005;
```

## Relevant SQLs

- [DELETE BACKEND/COMPUTE NODE BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](./SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)
