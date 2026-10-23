---
displayed_sidebar: docs
description: "FILE 是只读标量类型，通过一组固定字段引用一个文件或文件中的一段字节。"
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# FILE

<Experimental />

FILE 是一种标量类型，用于描述一个文件或文件内部的一段字节。FILE 值整体读取、整体返回：它包含一组固定的字段，但这些字段不能在 SQL 中单独访问。

目前 FILE 类型不支持持久化，只能从 External Catalog 获取。

## 结构

每个 FILE 值都按以下顺序包含相同的字段，每个字段都可以为 NULL。

| 字段            | 类型       | 说明                                                        |
|----------------|-----------|-------------------------------------------------------------|
| `uri`          | VARCHAR   | 文件位置，例如对象存储或 HDFS 路径。                            |
| `offset`       | BIGINT    | 被引用内容在文件内的起始字节位置。                               |
| `size`         | BIGINT    | 被引用内容的字节数，从 `offset` 开始计算。                       |
| `content_type` | VARCHAR   | 内容的媒体类型，例如 `image/png`。                              |
| `checksum`     | VARCHAR   | 被引用内容的校验值。                                            |
| `inline`       | VARBINARY | 直接存放在值中的内容本身，而不是通过 `uri` 引用。                 |

一个 FILE 值属于以下两种形式之一：

- **引用**：`uri` 有值，通常同时带有 `offset` 和 `size`，`inline` 为 NULL。
- **内联内容**：`inline` 保存内容字节，`uri`、`offset` 和 `size` 为 NULL。

## 输出格式

FILE 值以类 JSON 的对象形式输出，列出全部字段：

```plain text
{"uri":"s3://bucket/images/a.png","offset":4,"size":8,"content_type":null,"checksum":null,"inline":null}
{"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":"89504e470d0a1a0a"}
```

`inline` 字节的编码方式由会话变量 [`binary_encoding_format`](../../System_variable.md#binary_encoding_format) 控制。

## 使用限制

- FILE 不能作为 CREATE TABLE、CREATE TABLE AS SELECT 或物化视图中的列类型。
- FILE 值不能比较、排序、分组、作为 JOIN 条件、与 DISTINCT 一起使用、与其他类型互相转换，也不能作为窗口函数的参数。
- 支持 `IS NULL` 和 `IS NOT NULL`。
