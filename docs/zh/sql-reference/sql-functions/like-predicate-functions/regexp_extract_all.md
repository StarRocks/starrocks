---
displayed_sidebar: "Chinese"
---

# regexp_extract_all

## 功能

从 `str` 中提取与正则表达式 `pattern` 相匹配的子字符串，子字符串的索引位置由 `pos` 指定。该函数返回一个字符串数组。

`pattern` 必须完全匹配 `str` 的一部分。如果没有匹配的字符串，返回空字符串。

## 语法

```Haskell
ARRAY<VARCHAR> regexp_extract_all(VARCHAR str, VARCHAR pattern, BIGINT pos)
```

## 示例

```Plain Text
MySQL > SELECT regexp_extract_all('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1);
+-------------------------------------------------------------------+
| regexp_extract_all('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1)   |
+-------------------------------------------------------------------+
| ['b']                                                             |
+-------------------------------------------------------------------+

MySQL > SELECT regexp_extract_all('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 2);
+---------------------------------------------------------------------+
| regexp_extract_all('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 2) |
+---------------------------------------------------------------------+
| ['d','e']                                                           |
+---------------------------------------------------------------------+
```

## Keywords

REGEXP_EXTRACT_ALL,REGEXP,EXTRACT
