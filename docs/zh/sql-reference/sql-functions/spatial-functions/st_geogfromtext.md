---
displayed_sidebar: docs
description: "从 WKT 构造原生 GEOGRAPHY 值。"
---

# ST_GeogFromText

从 Well-Known Text（WKT）构造原生 `GEOGRAPHY` 值。

## 语法

```Haskell
GEOGRAPHY ST_GeogFromText(VARCHAR wkt)
GEOGRAPHY ST_GeogFromText(VARCHAR wkt, INT srid)
```

## 参数说明

- `wkt`：二维 OGC WKT 值。支持全部七种 OGC 几何类型和 `EMPTY` 成员。
- `srid`：可选。空间参考标识符。当前仅支持 `4326`；省略时默认为 `4326`。

坐标采用球面 `OGC:CRS84` 语义：经度范围为 `[-180, 180]`，纬度范围为 `[-90, 90]`。

## 返回值说明

返回 `GEOGRAPHY` 值。如果任一参数为 `NULL`、WKT 无效、坐标超出支持范围或 `srid` 不是 `4326`，则返回 `NULL`。

## 示例

```SQL
SELECT ST_AsText(ST_GeogFromText('LINESTRING (1 2, 3 4)', 4326));
```

```text
LINESTRING (1 2, 3 4)
```

```SQL
SELECT ST_AsText(ST_GeogFromText('POINT EMPTY'));
```

```text
POINT EMPTY
```

## 关键字

ST_GEOGFROMTEXT, GEOGRAPHY, WKT
