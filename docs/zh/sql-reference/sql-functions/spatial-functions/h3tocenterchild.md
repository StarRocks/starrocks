---
displayed_sidebar: docs
description: "返回给定 H3 索引在更细精度级别下的中心子单元。"
---

# h3ToCenterChild

返回给定 H3 索引在指定更细精度级别下的中心子单元。每个 H3 单元在下一级更细精度下包含 7 个子单元；此函数返回中心最接近父单元中心的那个子单元。

## 语法

```Haskell
BIGINT h3ToCenterChild(BIGINT h3index, INT resolution)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。
- `resolution`：目标子精度级别，必须大于或等于 `h3index` 的精度级别。支持的数据类型为 INT。

## 返回值说明

返回 BIGINT 类型，表示指定精度级别下的中心子单元索引。如果任意参数为 NULL、`resolution` 小于该单元的精度级别，或 `h3index` 不是有效的 H3 单元索引，则返回 NULL。

## 示例

```sql
SELECT h3ToCenterChild(577023702256844799, 1);
+----------------------------------------+
| h3ToCenterChild(577023702256844799, 1) |
+----------------------------------------+
| 581496515558637567                     |
+----------------------------------------+
```

## 关键词

H3TOCENTERCHILD,H3,SPATIAL
