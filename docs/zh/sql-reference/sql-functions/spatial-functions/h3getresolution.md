---
displayed_sidebar: docs
description: "返回 H3 单元索引的精度级别（0–15）。"
---

# h3GetResolution

返回给定 H3 单元索引的精度级别。H3 精度范围从 0（最粗，约 4,250 km 边长）到 15（最细，约 0.5 m 边长）。

## 语法

```Haskell
INT h3GetResolution(BIGINT h3index)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 INT 类型，取值范围为 [0, 15]，表示该 H3 单元的精度级别。如果参数为 NULL 或索引不是有效的 H3 单元，则返回 NULL。

## 示例

```sql
SELECT h3GetResolution(617700169958293503);
+-------------------------------------+
| h3GetResolution(617700169958293503) |
+-------------------------------------+
|                                   9 |
+-------------------------------------+
```

## 关键词

H3GETRESOLUTION,H3,SPATIAL,RESOLUTION
