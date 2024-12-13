---
displayed_sidebar: docs
---

# host_name

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Obtains the hostname of the node on which the computation is performed.

## Syntax

```Haskell
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
