---
displayed_sidebar: docs
description: "Queries roles that are activated for the current user."
products: [oss, enterprise, cloud, anywhere]
---

# current_role



Queries roles that are activated for the current user.

## Syntax

```plaintext
current_role();
current_role;
```

## Parameters

None.

## Return value

Returns a VARCHAR value.

## Examples

```Plain
mysql> select current_role();
+----------------+
| current_role() |
+----------------+
| db_admin       |
+----------------+
```
