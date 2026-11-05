---
displayed_sidebar: docs
description: "提取 FILE 值的 offset 字段。"
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_offset

<Experimental />

提取 [FILE](../../data-types/other-data-types/FILE.md) 值的 `offset` 字段。

## 语法

```sql
BIGINT fl_get_offset(FILE file_expr)
```

## 参数

`file_expr`：FILE 表达式，例如 [to_file](to_file.md) 或 [try_to_file](try_to_file.md) 的结果。

## 返回值

返回 BIGINT 值。FILE 值或 `offset` 字段为 NULL 时返回 NULL。字段值原样返回，不补默认值。

## 示例

### 示例 1：提取字段

```sql
SELECT fl_get_offset(to_file(named_struct('uri', 'a.jpg', 'offset', 0))) AS file_offset;
```

```plaintext
+-------------+
| file_offset |
+-------------+
| 0           |
+-------------+
```

### 示例 2：字段为 NULL

```sql
SELECT fl_get_offset(to_file(named_struct('uri', 'a.jpg'))) AS file_offset;
```

```plaintext
+-------------+
| file_offset |
+-------------+
| NULL        |
+-------------+
```

## keyword

FL_GET_OFFSET, FILE
