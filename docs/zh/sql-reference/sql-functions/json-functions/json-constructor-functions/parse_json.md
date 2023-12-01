---
displayed_sidebar: "Chinese"
---

# PARSE_JSON

## 功能

将字符串类型的数据构造为 JSON 类型的数据。

## 语法

```Plain Text
PARSE_JSON(string_expr)
```

## 参数说明

`string_expr`: 字符串的表达式。支持的数据类型为字符串类型（STRING、VARCHAR、CHAR）。

## 返回值说明

返回 JSON 类型的值。

> 如果字符串不能解析为规范的 JSON，则返回 NULL，参见示例五。JSON 规范，请参见 [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4)。

## 示例

示例一： 将字符串类型的 1 构造为 JSON 类型的 1。

```Plain Text
mysql> SELECT PARSE_JSON('1');
+-----------------+
| parse_json('1') |
+-----------------+
| "1"             |
+-----------------+
```

示例二：将一个字符串类型的数组构造为一个 JSON 类型的数组。

```Plain Text
mysql> SELECT PARSE_JSON('[1,2,3]');
+-----------------------+
| parse_json('[1,2,3]') |
+-----------------------+
| [1, 2, 3]             |
+-----------------------+ 
```

示例三：将一个字符串类型的对象构造为一个 JSON 类型的对象。

```Plain Text
mysql> SELECT PARSE_JSON('{"star": "rocks"}');
+---------------------------------+
| parse_json('{"star": "rocks"}') |
+---------------------------------+
| {"star": "rocks"}               |
+---------------------------------+
```

示例四：构造一个 JSON 类型的 NULL。

```Plain Text
mysql> SELECT PARSE_JSON('null');
+--------------------+
| parse_json('null') |
+--------------------+
| "null"             |
+--------------------+
```

示例五：如果字符串不能解析为规范的 JSON，则返回 NULL。 如下示例中，star 没有用双引号括起来，无法解析为合法的 JSON，因此返回 NULL。

```Plain Text
mysql> SELECT PARSE_JSON('{star: "rocks"}');
+-------------------------------+
| parse_json('{star: "rocks"}') |
+-------------------------------+
| NULL                          |
+-------------------------------+
```

## Keywords

parse_json, parse json
