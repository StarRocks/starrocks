---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Queries roles that are activated for the current user."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
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
