---
displayed_sidebar: docs
---

# current_role

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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
