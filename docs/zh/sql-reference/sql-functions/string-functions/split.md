---
displayed_sidebar: "Chinese"
---

# split

## 功能

根据分隔符拆分字符串，将拆分后的所有字符串以 ARRAY 的格式返回。

## 语法

```Haskell
split(content, delimiter)
```

## 参数说明

`content`: 支持的数据类型为 VARCHAR。

`delimiter`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 ARRAY。

## 示例

```Plain Text
mysql> select split("a,b,c",",");
+---------------------+
| split('a,b,c', ',') |
+---------------------+
| ["a","b","c"]       |
+---------------------+
mysql> select split("a,b,c",",b,");
+-----------------------+
| split('a,b,c', ',b,') |
+-----------------------+
| ["a","c"]             |
+-----------------------+
mysql> select split("abc","");
+------------------+
| split('abc', '') |
+------------------+
| ["a","b","c"]    |
+------------------+
```
