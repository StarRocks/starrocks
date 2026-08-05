---
displayed_sidebar: docs
description: "返回所有 122 个 H3 精度 0 基础单元索引。"
---

# h3GetRes0Indexes

返回所有 122 个 H3 精度 0 基础单元索引。这些是 H3 层级体系中最粗粒度的单元，构成网格的顶层。

## 语法

```Haskell
ARRAY<BIGINT> h3GetRes0Indexes()
```

## 返回值说明

返回 `ARRAY<BIGINT>` 类型，包含所有 122 个精度 0 基础单元索引。

## 示例

```sql
SELECT array_length(h3GetRes0Indexes());
+-----------------------------------+
| array_length(h3GetRes0Indexes())  |
+-----------------------------------+
| 122                               |
+-----------------------------------+
```

## 关键词

H3GETRES0INDEXES,H3,SPATIAL
