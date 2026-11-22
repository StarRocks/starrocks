---
displayed_sidebar: docs
description: "提取 FILE 值的 inline 字段。"
sidebar_position: 80
products: [oss, enterprise, cloud, anywhere]
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_inline

<Experimental />

提取 [FILE](../../data-types/other-data-types/FILE.md) 值的 `inline` 字段。

## 语法

```sql
VARBINARY fl_get_inline(FILE file_expr)
```

## 参数

`file_expr`：FILE 表达式，例如 [to_file](to_file.md) 或 [try_to_file](try_to_file.md) 的结果。

## 返回值

返回 VARBINARY 值。FILE 值或 `inline` 字段为 NULL 时返回 NULL。字段值原样返回，不补默认值。保留空字节串及内嵌零字节，不对二进制内容进行编码或解码。

## 示例

### 示例 1：提取字段

```sql
SELECT hex(fl_get_inline(to_file(named_struct('inline', to_binary('610062', 'hex'))))) AS inline_hex;
```

```plaintext
+------------+
| inline_hex |
+------------+
| 610062     |
+------------+
```

### 示例 2：字段为 NULL

```sql
SELECT hex(fl_get_inline(to_file(named_struct('uri', 'a.jpg')))) AS inline_hex;
```

```plaintext
+------------+
| inline_hex |
+------------+
| NULL       |
+------------+
```

## keyword

FL_GET_INLINE, FILE
