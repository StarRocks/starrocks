# ST_Circle

## 功能

将一个 WKT(Well Known Text)转化为地球球面上的一个圆。

## 语法

```Haskell
ST_Circle(center_lng, center_lat, radius)
```

## 参数说明

`center_lng`: 表示圆心的经度，支持的数据类型为 DOUBLE。

`center_lat`: 表示圆心的纬度，支持的数据类型为 DOUBLE。

`radius`: 表示的是圆的半径，单位是「米」，最大支持 9999999，支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 GEOMETRY。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_Circle(111, 64, 10000));
+--------------------------------------------+
| st_astext(st_circle(111.0, 64.0, 10000.0)) |
+--------------------------------------------+
| CIRCLE ((111 64), 10000)                   |
+--------------------------------------------+
```
