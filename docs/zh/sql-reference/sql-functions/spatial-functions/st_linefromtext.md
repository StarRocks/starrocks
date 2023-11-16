---
displayed_sidebar: "Chinese"
---

# ST_LineFromText, ST_LineStringFromText

## 功能

将一个 WKT（Well Known Text）转化为一个 Line 形式的内存表现形式。

## 语法

```Haskell
ST_LineFromText(wkt)
```

## 参数说明

`wkt`: 待转化的 WKT，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 GEOMETRY。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_LineFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_linefromtext('LINESTRING (1 1, 2 2)'))     |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```
