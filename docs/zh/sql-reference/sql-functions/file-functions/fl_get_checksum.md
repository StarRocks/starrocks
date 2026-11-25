---
displayed_sidebar: docs
description: "提取 FILE 值的 checksum 字段。"
sidebar_position: 70
products: [oss, enterprise, cloud, anywhere]
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_checksum

<Experimental />

提取 [FILE](../../data-types/other-data-types/FILE.md) 值的 `checksum` 字段。

## 语法

```sql
VARCHAR fl_get_checksum(FILE file_expr)
```

## 参数

`file_expr`：FILE 表达式，例如 [to_file](to_file.md) 或 [try_to_file](try_to_file.md) 的结果。

## 返回值

返回 VARCHAR 值。FILE 值或 `checksum` 字段为 NULL 时返回 NULL。字段值原样返回，不补默认值。保留空字符串。

## 示例

### 示例 1：提取字段

```sql
SELECT fl_get_checksum(to_file(named_struct('uri', 'a.jpg', 'checksum', 'opaque'))) AS checksum;
```

```plaintext
+----------+
| checksum |
+----------+
| opaque   |
+----------+
```

### 示例 2：字段为 NULL

```sql
SELECT fl_get_checksum(to_file(named_struct('uri', 'a.jpg'))) AS checksum;
```

```plaintext
+----------+
| checksum |
+----------+
| NULL     |
+----------+
```

## keyword

FL_GET_CHECKSUM, FILE
