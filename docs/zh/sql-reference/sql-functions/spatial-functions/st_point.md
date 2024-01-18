---
displayed_sidebar: "Chinese"
---

# ST_Point

## 功能

通过给定的 X 坐标值、Y 坐标值返回对应的 Point，当前这个值只在球面集合上有意义，X/Y 对应的是经度/纬度(longitude/latitude)。

## 语法

```Haskell
ST_Point(x, y)
```

## 参数说明

`x`: X 坐标值，支持数据类型为 DOUBLE。

`y`: Y 坐标值，支持数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 POINT。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```
