---
displayed_sidebar: docs
description: "判断 BIGINT 值是否为有效的 H3 单元索引。"
---

# h3IsValid

判断给定的 BIGINT 值是否为有效的 H3 单元索引。该函数对非 NULL 输入始终返回 true 或 false，不会因索引无效而返回 NULL。

## 语法

```Haskell
BOOLEAN h3IsValid(BIGINT h3index)
```

## 参数说明

- `h3index`：待验证的 H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 BOOLEAN 类型：如果值是有效的 H3 单元索引，则返回 `true`（1）；否则返回 `false`（0）。仅当参数为 NULL 时返回 NULL。

## 示例

示例一：有效的 H3 单元索引。

```sql
SELECT h3IsValid(617700169958293503);
+-------------------------------+
| h3IsValid(617700169958293503) |
+-------------------------------+
|                             1 |
+-------------------------------+
```

示例二：无效值。

```sql
SELECT h3IsValid(0);
+--------------+
| h3IsValid(0) |
+--------------+
|            0 |
+--------------+
```

## 关键词

H3ISVALID,H3,SPATIAL,VALID
