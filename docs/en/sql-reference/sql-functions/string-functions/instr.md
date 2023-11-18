---
displayed_sidebar: "English"
---

# instr

## Description

This function returns the position where str first appeared in substr (start counting from 1 and measured in characters). If str is not found in substr, then this function will return 0.

## Syntax

```Haskell
INT instr(VARCHAR str, VARCHAR substr)
```

## Examples

```Plain Text
MySQL > select instr("abc", "b");
+-------------------+
| instr('abc', 'b') |
+-------------------+
|                 2 |
+-------------------+

MySQL > select instr("abc", "d");
+-------------------+
| instr('abc', 'd') |
+-------------------+
|                 0 |
+-------------------+
```

## keyword

INSTR
