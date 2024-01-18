---
displayed_sidebar: "English"
---

# ends_with

## Description

Returns `true` if a string ends with a specified suffix. Otherwise, it returns `false`. If the argument is NULL, the result is NULL.

## Syntax

```Haskell
BOOLEAN ENDS_WITH (VARCHAR str, VARCHAR suffix)
```

## Examples

```Plain Text
MySQL > select ends_with("Hello starrocks", "starrocks");
+-----------------------------------+
| ends_with('Hello starrocks', 'starrocks') |
+-----------------------------------+
|                                 1 |
+-----------------------------------+

MySQL > select ends_with("Hello starrocks", "Hello");
+-----------------------------------+
| ends_with('Hello starrocks', 'Hello') |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```

## keyword

ENDS_WITH
