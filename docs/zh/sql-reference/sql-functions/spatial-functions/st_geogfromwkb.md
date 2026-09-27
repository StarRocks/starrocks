---
displayed_sidebar: docs
description: "从 WKB 构造原生 GEOGRAPHY 值。"
---

# ST_GeogFromWKB

从 Well-Known Binary（WKB）构造原生 `GEOGRAPHY` 值。

## 语法

```Haskell
GEOGRAPHY ST_GeogFromWKB(VARBINARY wkb)
GEOGRAPHY ST_GeogFromWKB(VARBINARY wkb, INT srid)
```

## 参数说明

- `wkb`：采用小端或大端字节序的二维 OGC WKB 值。支持全部七种 OGC 几何类型和 `EMPTY` 成员。不支持 EWKB 扩展。
- `srid`：可选。空间参考标识符。当前仅支持 `4326`；省略时默认为 `4326`。

坐标采用球面 `OGC:CRS84` 语义：经度范围为 `[-180, 180]`，纬度范围为 `[-90, 90]`。

## 返回值说明

返回 `GEOGRAPHY` 值。如果任一参数为 `NULL`、WKB 无效、坐标超出支持范围或 `srid` 不是 `4326`，则返回 `NULL`。

## 示例

```SQL
SELECT ST_AsText(ST_GeogFromWKB(unhex('0101000000000000000000F03F0000000000000040')));
```

```text
POINT (1 2)
```

## 关键字

ST_GEOGFROMWKB, GEOGRAPHY, WKB
