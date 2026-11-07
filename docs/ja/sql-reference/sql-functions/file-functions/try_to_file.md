---
displayed_sidebar: docs
description: "STRUCT 式から FILE 値を構築し、フィールド値が無効な行には NULL を返します。"
sidebar_position: 20
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# try_to_file

<Experimental />

[to_file](to_file.md) と同じフィールド、型、検証ルールを使用して一時的な FILE を構築します。フィールド値が無効な行だけが NULL になり、他の行には影響しません。

## 構文

```sql
FILE try_to_file(STRUCT struct_expr)
```

## パラメータ

`struct_expr`：[to_file](to_file.md) と同じです。

## 戻り値

FILE 値を返します。NULL STRUCT は NULL を返します。非 NULL STRUCT では、`uri` と `inline` が両方 NULL または両方非 NULL の場合、あるいは非 NULL の `offset` または `size` が負の場合、その行は NULL を返します。

## 使用上の注意

- 不明なフィールド、重複したフィールド、未対応の型、引数の数の誤りは引き続き解析エラーになります。引数式の評価エラー、内部エラー、リソースエラーは NULL に変換しません。

## 例

### 例 1：有効な入力

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

### 例 2：無効な行は NULL を返す

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

## キーワード

TRY_TO_FILE, FILE
