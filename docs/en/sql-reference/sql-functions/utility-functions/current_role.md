---
displayed_sidebar: docs
---

# current_role

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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
