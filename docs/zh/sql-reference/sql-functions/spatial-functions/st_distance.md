---
displayed_sidebar: docs
description: "返回两个 GEOGRAPHY 点之间以米为单位的球面距离。"
---

# ST_DISTANCE

在 OGC:CRS84 语义下，返回两个 `GEOGRAPHY` 点之间以米为单位的球面距离。

## 语法

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
```

## 返回值说明

两个输入值都必须是 `POINT`。如果任一输入为 `NULL` 或 EMPTY，则返回 `NULL`。输入不是 `POINT`、维度不受支持或描述符不受支持时返回错误。

目前只支持 `GEOGRAPHY` 重载，不支持 `GEOMETRY` 输入和其他几何类型组合。

## 示例

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 0)'),
    ST_GeogFromText('POINT (1 0)'));
-- 约 111195.1 米
```

## 关键字

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY
