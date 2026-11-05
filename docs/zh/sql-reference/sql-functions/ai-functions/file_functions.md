---
displayed_sidebar: docs
description: "使用构造函数创建 FILE 值，并通过 Getter 提取文件元信息和内联内容。"
sidebar_position: 30
toc_max_heading_level: 2
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# FILE 函数

<Experimental />

这些函数用于构造 [FILE](../../data-types/other-data-types/FILE.md) 值或提取其中的字段，不访问存储，也不推断元信息。

| 函数 | 用途 | 返回类型 |
| --- | --- | --- |
| [to_file](#to_file) | 根据 STRUCT 构造 FILE，字段值非法时报错 | FILE |
| [try_to_file](#try_to_file) | 根据 STRUCT 构造 FILE，字段值非法的行返回 NULL | FILE |
| [fl_get_uri](#fl_get_uri) | 提取文件位置 | VARCHAR |
| [fl_get_offset](#fl_get_offset) | 提取起始字节偏移 | BIGINT |
| [fl_get_size](#fl_get_size) | 提取字节数 | BIGINT |
| [fl_get_content_type](#fl_get_content_type) | 提取媒体类型 | VARCHAR |
| [fl_get_checksum](#fl_get_checksum) | 提取校验值 | VARCHAR |
| [fl_get_inline](#fl_get_inline) | 提取内联字节 | VARBINARY |

## 公共约定

`FL_GET_*` 接受 FILE 表达式，包括 [TO_FILE](#to_file) 或 [TRY_TO_FILE](#try_to_file) 的结果。FILE 值或对应字段为 NULL 时返回 NULL。字段值原样返回，不补默认值；保留空字符串、空字节串及内嵌零字节，不对二进制内容进行编码或解码。

## to_file

根据 STRUCT 表达式构造临时 FILE 值，支持 `named_struct(...)` 和 STRUCT 列。

### 语法

```sql
TO_FILE(struct_expr)
```

### 字段

| 字段 | 接受的输入类型 | FILE 字段类型 |
|---|---|---|
| `uri`, `content_type`, `checksum` | CHAR, VARCHAR | VARCHAR |
| `offset`, `size` | TINYINT, SMALLINT, INT, BIGINT | BIGINT |
| `inline` | VARBINARY | VARBINARY |

### 语义

- 字段名称大小写不敏感，顺序不限。未知或重复字段名在分析阶段报错。
- 缺失字段和显式 NULL 均填为 NULL，不补默认值，包括 `offset` 和 `size`。
- NULL STRUCT 返回 NULL，并跳过字段值校验。对于每个非 NULL STRUCT，`uri` 和 `inline` 必须恰好一个非 NULL；字段缺失等同于 NULL。
- 整数输入扩宽为 BIGINT。每个字段都接受未定型 NULL。其他输入类型需要显式 CAST。
- `offset`、`size` 可以为 NULL；非 NULL 值必须大于等于 0。任意一行违反此规则或 `uri`/`inline` 二选一规则，查询都会失败。
- 不校验 URI 格式、MIME 类型、校验和，以及 size 与实际字节长度是否一致。
- `inline` 保存原始字节。空字符串和零长度二进制值仍算非 NULL，并会被保留。

### 示例

```sql
SELECT TO_FILE(named_struct('uri', 'https://example.com/a.jpg', 'content_type', 'image/jpeg'));
-- {"uri":"https://example.com/a.jpg","offset":null,"size":null,"content_type":"image/jpeg","checksum":null,"inline":null}

SELECT TO_FILE(named_struct('inline', to_binary('610062', 'hex')));
-- {"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":"610062"}

SELECT TO_FILE(named_struct('uri', '', 'offset', 0, 'size', NULL));
-- {"uri":"","offset":0,"size":null,"content_type":null,"checksum":null,"inline":null}

SELECT TO_FILE(NULL) IS NULL;
-- 1
```

## try_to_file

使用与 [TO_FILE](#to_file) 相同的字段、类型和校验规则构造临时 FILE。仅将字段值非法的行置为 NULL，其他行不受影响。

### 语法

```sql
TRY_TO_FILE(struct_expr)
```

### 语义

- NULL STRUCT 返回 NULL。对于非 NULL STRUCT，`uri` 和 `inline` 都为 NULL、都非 NULL，或非 NULL 的 `offset`、`size` 小于 0 时，该行返回 NULL。
- 未知字段、重复字段、不支持的类型以及错误参数数量仍在分析阶段报错。参数表达式求值错误、内部错误和资源错误不会转成 NULL。

### 示例

```sql
SELECT FL_GET_URI(TRY_TO_FILE(named_struct('uri', 'a.jpg', 'size', 12)));
-- a.jpg

SELECT HEX(FL_GET_INLINE(TRY_TO_FILE(named_struct('inline', to_binary('610062', 'hex')))));
-- 610062

SELECT TRY_TO_FILE(named_struct('uri', 'a.jpg', 'size', -1)) IS NULL;
-- 1

SELECT TRY_TO_FILE(named_struct('uri', NULL, 'inline', NULL)) IS NULL;
-- 1

SELECT TRY_TO_FILE(named_struct('uri', 'a.jpg', 'inline', to_binary('61', 'hex'))) IS NULL;
-- 1
```

## fl_get_uri

提取 `uri` 字段，返回 VARCHAR。

### 语法

```sql
FL_GET_URI(file_expr)
```

### 示例

```sql
SELECT FL_GET_URI(TO_FILE(named_struct('uri', 'https://example.com/a.jpg')));
-- https://example.com/a.jpg
```

## fl_get_offset

提取 `offset` 字段，返回 BIGINT。

### 语法

```sql
FL_GET_OFFSET(file_expr)
```

### 示例

```sql
SELECT FL_GET_OFFSET(TO_FILE(named_struct('uri', 'a.jpg', 'offset', 0)));
-- 0
```

## fl_get_size

提取 `size` 字段，返回 BIGINT。

### 语法

```sql
FL_GET_SIZE(file_expr)
```

### 示例

```sql
SELECT FL_GET_SIZE(TO_FILE(named_struct('uri', 'a.jpg', 'size', 12)));
-- 12
```

## fl_get_content_type

提取 `content_type` 字段，返回 VARCHAR。

### 语法

```sql
FL_GET_CONTENT_TYPE(file_expr)
```

### 示例

```sql
SELECT FL_GET_CONTENT_TYPE(TO_FILE(named_struct('uri', 'a.jpg', 'content_type', 'image/jpeg')));
-- image/jpeg
```

## fl_get_checksum

提取 `checksum` 字段，返回 VARCHAR。

### 语法

```sql
FL_GET_CHECKSUM(file_expr)
```

### 示例

```sql
SELECT FL_GET_CHECKSUM(TO_FILE(named_struct('uri', 'a.jpg', 'checksum', 'opaque')));
-- opaque
```

## fl_get_inline

提取 `inline` 字段，返回 VARBINARY。

### 语法

```sql
FL_GET_INLINE(file_expr)
```

### 示例

```sql
SELECT HEX(FL_GET_INLINE(TO_FILE(named_struct('inline', to_binary('610062', 'hex')))));
-- 610062
```
