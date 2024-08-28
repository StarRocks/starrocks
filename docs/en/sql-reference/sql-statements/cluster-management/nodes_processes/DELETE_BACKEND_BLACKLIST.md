---
displayed_sidebar: docs
---

# DELETE BACKEND BLACKLIST

## Description

Removes a BE node from the BE Blacklist. Please note that StarRocks will not remove the BE nodes that are manually blacklisted by users.

This feature is supported from v3.3.0 onwards. For more information, see [Manage BE Blacklist](../../../../administration/management/BE_blacklist.md).

:::note

Only users with the SYSTEM-level BLACKLIST privilege can perform this operation.

:::

## Syntax

```SQL
DELETE BACKEND BLACKLIST <be_id>[, ...]
```

## Parameters

`be_id`: ID of the BE node to be removed from the blacklist. You can obtain the ID of the blacklisted BE by executing [SHOW BACKEND BLACKLIST](./SHOW_BACKEND_BLACKLIST.md).

## Examples

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## Relevant SQLs

- [ADD BACKEND BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [SHOW BACKEND BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)

