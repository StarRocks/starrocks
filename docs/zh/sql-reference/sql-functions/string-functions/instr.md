# instr

## description

### Syntax

```Haskell
INT instr(VARCHAR str, VARCHAR substr)
```

返回 substr 在 str 中第一次出现的位置（从1开始计数，按「字符」计算）。如果 substr 不在 str 中出现，则返回0。

## example

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
