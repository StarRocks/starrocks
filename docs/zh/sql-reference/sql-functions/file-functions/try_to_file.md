---
displayed_sidebar: docs
description: "根据 STRUCT 表达式构造 FILE 值，字段值非法的行返回 NULL。"
sidebar_position: 20
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# try_to_file

<Experimental />

使用与 [to_file](to_file.md) 相同的字段、类型和校验规则构造临时 FILE。仅将字段值非法的行置为 NULL，其他行不受影响。

## 语法

```sql
FILE try_to_file(STRUCT struct_expr)
```

## 参数

`struct_expr`：与 [to_file](to_file.md) 相同。

## 返回值

返回 FILE 值。NULL STRUCT 返回 NULL。对于非 NULL STRUCT，`uri` 和 `inline` 都为 NULL、都非 NULL，或非 NULL 的 `offset`、`size` 小于 0 时，该行返回 NULL。

## 使用说明

- 未知字段、重复字段、不支持的类型以及错误参数数量仍在分析阶段报错。参数表达式求值错误、内部错误和资源错误不会转成 NULL。

## 示例

### 示例 1：合法输入

```sql
SELECT try_to_file(named_struct('uri', 'a.jpg', 'size', 12)) AS f;
```

```plaintext
+-------------------------------------------------------------------------------------------+
| f                                                                                         |
+-------------------------------------------------------------------------------------------+
| {"uri":"a.jpg","offset":null,"size":12,"content_type":null,"checksum":null,"inline":null} |
+-------------------------------------------------------------------------------------------+
```

### 示例 2：非法行返回 NULL

```sql
SELECT
    try_to_file(named_struct('uri', 'a.jpg', 'size', -1)) IS NULL AS negative_size,
    try_to_file(named_struct('uri', NULL, 'inline', NULL)) IS NULL AS neither_set,
    try_to_file(named_struct('uri', 'a.jpg', 'inline', to_binary('61', 'hex'))) IS NULL AS both_set;
```

```plaintext
+---------------+-------------+----------+
| negative_size | neither_set | both_set |
+---------------+-------------+----------+
| 1             | 1           | 1        |
+---------------+-------------+----------+
```

## keyword

TRY_TO_FILE, FILE
