---
displayed_sidebar: docs
description: "根据 STRUCT 表达式构造 FILE 值。"
sidebar_position: 10
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# to_file

<Experimental />

根据 STRUCT 表达式构造临时 [FILE](../../data-types/other-data-types/FILE.md) 值，支持 `named_struct(...)` 和 STRUCT 列。本函数不访问存储，也不推断元信息。

## 语法

```sql
FILE to_file(STRUCT struct_expr)
```

## 参数

`struct_expr`：描述文件的 STRUCT 表达式。可以使用以下字段：

| 字段 | 接受的输入类型 | FILE 字段类型 |
|---|---|---|
| `uri`, `content_type`, `checksum` | CHAR, VARCHAR | VARCHAR |
| `offset`, `size` | TINYINT, SMALLINT, INT, BIGINT | BIGINT |
| `inline` | VARBINARY | VARBINARY |

## 返回值

返回 FILE 值。`struct_expr` 为 NULL 时返回 NULL。

## 使用说明

- 字段名称大小写不敏感，顺序不限。未知或重复字段名在分析阶段报错。
- 缺失字段和显式 NULL 均填为 NULL，不补默认值，包括 `offset` 和 `size`。
- 对于每个非 NULL STRUCT，`uri` 和 `inline` 必须恰好一个非 NULL；字段缺失等同于 NULL。
- 整数输入扩宽为 BIGINT。每个字段都接受未定型 NULL。其他输入类型需要显式 CAST。
- URI 引用的 `offset`、`size` 可以为 NULL；非 NULL 值必须大于等于 0。内联内容的 `offset`、`size` 必须均为 NULL。任意一行违反此规则或 `uri`/`inline` 二选一规则，查询都会失败。如需对这类行返回 NULL，请使用 [try_to_file](try_to_file.md)。
- 不校验 URI 格式、MIME 类型、校验和，以及引用的字节范围。
- `inline` 保存原始字节。空字符串和零长度二进制值仍算非 NULL，并会被保留。

## 示例

### 示例 1：构造文件引用

```sql
SELECT to_file(named_struct('uri', 'https://example.com/a.jpg', 'content_type', 'image/jpeg')) AS f;
```

```plaintext
+-------------------------------------------------------------------------------------------------------------------------+
| f                                                                                                                       |
+-------------------------------------------------------------------------------------------------------------------------+
| {"uri":"https://example.com/a.jpg","offset":null,"size":null,"content_type":"image/jpeg","checksum":null,"inline":null} |
+-------------------------------------------------------------------------------------------------------------------------+
```

### 示例 2：构造内联内容

```sql
SELECT to_file(named_struct('inline', to_binary('610062', 'hex'))) AS f;
```

```plaintext
+----------------------------------------------------------------------------------------------+
| f                                                                                            |
+----------------------------------------------------------------------------------------------+
| {"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":"610062"} |
+----------------------------------------------------------------------------------------------+
```

### 示例 3：NULL 参数

```sql
SELECT to_file(NULL) AS f;
```

```plaintext
+------+
| f    |
+------+
| NULL |
+------+
```

## keyword

TO_FILE, FILE
