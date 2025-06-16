---
displayed_sidebar: docs
---

# instr

この関数は、`str` が `substr` に最初に現れた位置を返します（1から数え始め、文字数で測定します）。`str` が `substr` に見つからない場合、この関数は 0 を返します。

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