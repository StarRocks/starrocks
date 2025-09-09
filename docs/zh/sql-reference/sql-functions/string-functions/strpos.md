---
displayed_sidebar: docs
---

# strpos

返回字符串中第 N 个子串实例的位置。当 N 为负数时，搜索将从字符串的末尾开始。位置从 1 开始计数。如果未找到，则返回 0。

## Syntax

```Haskell
INT strpos(VARCHAR str, VARCHAR substr [, INT instance])
```

## Parameters

- `str`：要从中查找子字符串的字符串。
- `substr`：要查找的子字符串。
- `instance`：要查找的第 N 个子字符串实例。如果此项设置为负值，则搜索将从字符串末尾开始。默认值：`1`。

## Return value

返回一个整数。如果未找到子字符串，则返回 `0`。

## Examples

```SQL
SELECT strpos('hello world', 'world');
+-----------------------------+
| strpos('hello world', 'world') |
+-----------------------------+
|                           7 |
+-----------------------------+

SELECT strpos('Hello World', 'world');
+-----------------------------+
| strpos('Hello World', 'world') |
+-----------------------------+
|                           0 |
+-----------------------------+

SELECT strpos('hello world hello', 'hello', 2);
+--------------------------------------+
| strpos('hello world hello', 'hello', 2) |
+--------------------------------------+
|                                   13 |
+--------------------------------------+

SELECT strpos('StarRocks', 'Spark');
+----------------------------+
| strpos('StarRocks', 'Spark') |
+----------------------------+
|                          0 |
+----------------------------+

SELECT strpos(NULL, 'test');
+--------------------+
| strpos(NULL, 'test') |
+--------------------+
|               NULL |
+--------------------+
```

## 关键词

STRPOS, STRING, POSITION, SUBSTRING