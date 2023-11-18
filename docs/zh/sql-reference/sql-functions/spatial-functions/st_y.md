---
displayed_sidebar: "Chinese"
---

# ST_Y

## 功能

当 point 是一个合法的 POINT 类型时，返回对应的 Y 坐标值。

## 语法

```Haskell
ST_Y(point)
```

## 参数说明

`point`: 支持的数据类型为 POINT。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
MySQL > SELECT ST_Y(ST_Point(24.7, 56.7));
+----------------------------+
| st_y(st_point(24.7, 56.7)) |
+----------------------------+
|                       56.7 |
+----------------------------+
```
