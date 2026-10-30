---
displayed_sidebar: docs
description: "将原生 GEOMETRY 或 GEOGRAPHY 值序列化为 WKB。"
---

# ST_AsBinary, ST_AsWKB

将原生 `GEOMETRY` 或 `GEOGRAPHY` 值序列化为 Well-Known Binary（WKB）。`ST_AsWKB` 是 `ST_AsBinary` 的别名。

## 语法

```Haskell
VARBINARY ST_AsBinary(GEOMETRY geometry)
VARBINARY ST_AsBinary(GEOGRAPHY geography)
VARBINARY ST_AsWKB(GEOMETRY geometry)
VARBINARY ST_AsWKB(GEOGRAPHY geography)
```

该函数保留全部七种 OGC 几何类型和 `EMPTY` 成员。输入为 `NULL` 时返回 `NULL`。

## 示例

```SQL
SELECT hex(ST_AsBinary(ST_GeogFromText('POINT (1 2)')));
```

```text
0101000000000000000000F03F0000000000000040
```

相同的序列化函数名称也接受原生 `GEOMETRY` 值：

```SQL
SELECT hex(ST_AsBinary(ST_GeomFromText('POINT (1 2)', 'EPSG:3857')));
```

## 关键字

ST_ASBINARY, ST_ASWKB, GEOMETRY, GEOGRAPHY, WKB
