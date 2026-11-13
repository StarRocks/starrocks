---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns a random UUID of the LARGEINT type."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# uuid_numeric



Returns a random UUID of the LARGEINT type. This function has an execution performance 2 orders of magnitude better than the `uuid` function.

## Syntax

```plaintext
uuid_numeric();
```

## Parameters

None

## Return value

Returns a value of the LARGEINT type.

## Examples

```Plain Text
MySQL > select uuid_numeric();
+--------------------------+
| uuid_numeric()           |
+--------------------------+
| 558712445286367898661205 |
+--------------------------+
1 row in set (0.00 sec)
```
