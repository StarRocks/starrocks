---
displayed_sidebar: "English"
---

# current_role

## Description

Queries roles that are activated for the current user.

## Syntax

```Haskell
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
