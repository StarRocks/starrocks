---
displayed_sidebar: docs
description: "将 H3 单元索引从 BIGINT 整数表示转换为十六进制字符串。"
---

# h3ToString

将 H3 单元索引从 BIGINT 整数表示转换为小写十六进制字符串。这是 H3 库及外部工具使用的标准字符串格式。

## 语法

```Haskell
VARCHAR h3ToString(BIGINT h3index)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 VARCHAR 类型，包含 H3 索引的小写十六进制字符串表示。如果参数为 NULL，则返回 NULL。

## 示例

```sql
SELECT h3ToString(617420388352917503);
+-----------------------------------+
| h3ToString(617420388352917503)    |
+-----------------------------------+
| 89184926cdbffff                   |
+-----------------------------------+
```

## 关键词

H3TOSTRING,H3,SPATIAL
