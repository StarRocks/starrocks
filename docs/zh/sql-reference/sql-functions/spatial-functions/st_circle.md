# ST_Circle

## description

### Syntax

```Haskell
GEOMETRY ST_Circle(DOUBLE center_lng, DOUBLE center_lat, DOUBLE radius)
```

将一个WKT（Well Known Text）转化为地球球面上的一个圆。

* center_lng 表示的圆心的经度
* center_lat 表示的是圆心的纬度
* radius 表示的是圆的半径，单位是「米」，最大支持9999999

## example

```Plain Text
MySQL > SELECT ST_AsText(ST_Circle(111, 64, 10000));
+--------------------------------------------+
| st_astext(st_circle(111.0, 64.0, 10000.0)) |
+--------------------------------------------+
| CIRCLE ((111 64), 10000)                   |
+--------------------------------------------+
```

## keyword

ST_CIRCLE,ST,CIRCLE
