---
displayed_sidebar: docs
---

# parse_url

## 功能

从目标 URL 中提取一部分信息。

## 语法

```Haskell
parse_url(expr1,expr2);
```

## 参数说明

`expr1`: 目标 URL，支持的数据类型为 VARCHAR。

`expr2`: 待提取的信息，支持的数据类型为 VARCHAR。取值如下，注意取值**大小写敏感**：

- PROTOCOL
- HOST
- PATH
- REF
- AUTHORITY
- FILE
- USERINFO
- QUERY（不支持返回 QUERY 里面的特定参数。如果您想返回特定参数，可以配合 [trim](trim.md) 函数使用，见示例。）

## 返回值说明

返回值的数据类型为 VARCHAR。如果输入的 URL 字符串无效，返回报错。如果未找到请求的信息，返回 NULL。

## 示例

```Plain Text
select parse_url('http://facebook.com/path/p1.php?query=1', 'HOST');
+--------------------------------------------------------------+
| parse_url('http://facebook.com/path/p1.php?query=1', 'HOST') |
+--------------------------------------------------------------+
| facebook.com                                                 |
+--------------------------------------------------------------+

select parse_url('http://facebook.com/path/p1.php?query=1', 'AUTHORITY');
+-------------------------------------------------------------------+
| parse_url('http://facebook.com/path/p1.php?query=1', 'AUTHORITY') |
+-------------------------------------------------------------------+
| facebook.com                                                      |
+-------------------------------------------------------------------+

select parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY');
+---------------------------------------------------------------+
| parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY') |
+---------------------------------------------------------------+
| query=1                                                       |
+---------------------------------------------------------------+

select trim(parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY'),'query='); 
+-------------------------------------------------------------------------------+
| trim(parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY'), 'query=') |
+-------------------------------------------------------------------------------+
| 1                                                                             |
+-------------------------------------------------------------------------------+

```
