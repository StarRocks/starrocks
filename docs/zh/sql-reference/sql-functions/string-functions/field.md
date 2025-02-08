---
displayed_sidebar: docs
---

# field

返回字符串 str 在 str1, str2, str3, … 列表中的索引（位置）。如果未找到 str，则返回 0。

## 语法

```Haskell
INT field(expr, ...)
```

## 示例

```Plain Text
MYSQL > select field('a', 'b', 'a', 'd');
+---------------------------+
| field('a', 'b', 'a', 'd') |
+---------------------------+
|                         2 |
+---------------------------+
```