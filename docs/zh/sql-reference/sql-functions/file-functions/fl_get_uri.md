---
displayed_sidebar: docs
description: "提取 FILE 值的 uri 字段。"
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_uri

<Experimental />

提取 [FILE](../../data-types/other-data-types/FILE.md) 值的 `uri` 字段。

## 语法

```sql
VARCHAR fl_get_uri(FILE file_expr)
```

## 参数

`file_expr`：FILE 表达式，例如 [to_file](to_file.md) 或 [try_to_file](try_to_file.md) 的结果。

## 返回值

返回 VARCHAR 值。FILE 值或 `uri` 字段为 NULL 时返回 NULL。字段值原样返回，不补默认值。保留空字符串。

## 示例

### 示例 1：提取字段

```sql
SELECT fl_get_uri(to_file(named_struct('uri', 'https://example.com/a.jpg'))) AS uri;
```

```plaintext
+---------------------------+
| uri                       |
+---------------------------+
| https://example.com/a.jpg |
+---------------------------+
```

### 示例 2：字段为 NULL

```sql
SELECT fl_get_uri(to_file(named_struct('inline', to_binary('61', 'hex')))) AS uri;
```

```plaintext
+------+
| uri  |
+------+
| NULL |
+------+
```

## keyword

FL_GET_URI, FILE
