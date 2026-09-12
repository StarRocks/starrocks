---
displayed_sidebar: docs
description: "返回给定 H3 索引在更粗精度级别下的父单元。"
---

# h3ToParent

返回给定 H3 索引在指定更粗精度级别下的父单元。在 H3 层级体系中，精度 r 下的每个单元都完全包含在精度 r-1 下的唯一父单元中。

## 语法

```Haskell
BIGINT h3ToParent(BIGINT h3index, INT resolution)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。
- `resolution`：目标父精度级别，必须小于或等于 `h3index` 的精度级别。支持的数据类型为 INT。

## 返回值说明

返回 BIGINT 类型，表示指定精度级别下的父单元索引。如果任意参数为 NULL、`resolution` 大于该单元的精度级别，或 `h3index` 不是有效的 H3 单元索引，则返回 NULL。

## 示例

```sql
SELECT h3ToParent(599405990164561919, 3);
+-----------------------------------+
| h3ToParent(599405990164561919, 3) |
+-----------------------------------+
| 590398848891879423                |
+-----------------------------------+
```

## 关键词

H3TOPARENT,H3,SPATIAL
