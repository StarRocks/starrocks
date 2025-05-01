---
displayed_sidebar: docs
---

# instr

## Description

この関数は、`str` が `substr` に最初に現れる位置を返します（1 から数え始め、文字単位で測定します）。`str` が `substr` に見つからない場合、この関数は 0 を返します。

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