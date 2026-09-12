---
displayed_sidebar: docs
description: "解析十六进制 H3 字符串，返回对应的 BIGINT 索引。"
---

# stringToH3

解析十六进制 H3 字符串，返回对应的 BIGINT 索引。这是 `h3ToString` 的逆操作。输入必须是有效的 H3 十六进制字符串（不含 `0x` 前缀）。

## 语法

```Haskell
BIGINT stringToH3(VARCHAR h3string)
```

## 参数说明

- `h3string`：十六进制 H3 索引字符串。支持的数据类型为 VARCHAR。

## 返回值说明

返回 BIGINT 类型，表示 H3 单元索引。如果输入为 NULL 或不是有效的 H3 十六进制字符串，则返回 NULL。

## 示例

```sql
SELECT stringToH3('89184926cc3ffff');
+--------------------------------+
| stringToH3('89184926cc3ffff') |
+--------------------------------+
| 617420388351344639             |
+--------------------------------+
```

## 关键词

STRINGTOH3,H3,SPATIAL
