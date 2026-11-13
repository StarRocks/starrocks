---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Obtains the hostname of the node on which the computation is performed."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# host_name



Obtains the hostname of the node on which the computation is performed.

## Syntax

```plaintext
host_name();
```

## Parameters

None

## Return value

Returns a VARCHAR value.

## Examples

```Plaintext
select host_name();
+-------------+
| host_name() |
+-------------+
| sandbox-sql |
+-------------+
1 row in set (0.01 sec)
```
