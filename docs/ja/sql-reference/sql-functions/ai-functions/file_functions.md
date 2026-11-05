---
displayed_sidebar: docs
description: "FILE 値を構築し、Getter 関数でファイルのメタデータとインライン内容を取得します。"
sidebar_position: 30
toc_max_heading_level: 2
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# FILE 関数

<Experimental />

これらの関数は [FILE](../../data-types/other-data-types/FILE.md) 値を構築したり、そのフィールドを取得したりします。ストレージへのアクセスやメタデータの推論は行いません。

| 関数 | 用途 | 戻り値の型 |
| --- | --- | --- |
| [to_file](#to_file) | STRUCT から FILE を構築し、無効なフィールド値はエラーにする | FILE |
| [try_to_file](#try_to_file) | STRUCT から FILE を構築し、無効な行は NULL を返す | FILE |
| [fl_get_uri](#fl_get_uri) | ファイルの場所を取得する | VARCHAR |
| [fl_get_offset](#fl_get_offset) | 開始バイトオフセットを取得する | BIGINT |
| [fl_get_size](#fl_get_size) | バイト数を取得する | BIGINT |
| [fl_get_content_type](#fl_get_content_type) | メディアタイプを取得する | VARCHAR |
| [fl_get_checksum](#fl_get_checksum) | チェックサムを取得する | VARCHAR |
| [fl_get_inline](#fl_get_inline) | インラインのバイト列を取得する | VARBINARY |

## 共通の動作

`FL_GET_*` は FILE 式を受け付けます。[TO_FILE](#to_file) または [TRY_TO_FILE](#try_to_file) の結果も使用できます。FILE 値または対象のフィールドが NULL の場合は NULL を返します。フィールド値はそのまま返され、デフォルト値は補完しません。空文字列、空のバイト列、埋め込まれたゼロバイトも保持し、バイナリ内容のエンコードやデコードは行いません。

## to_file

`named_struct(...)` や STRUCT カラムなどの STRUCT 式から一時的な FILE 値を構築します。

### 構文

```sql
TO_FILE(struct_expr)
```

### フィールド

| フィールド | 使用できる入力型 | FILE フィールドの型 |
|---|---|---|
| `uri`, `content_type`, `checksum` | CHAR, VARCHAR | VARCHAR |
| `offset`, `size` | TINYINT, SMALLINT, INT, BIGINT | BIGINT |
| `inline` | VARBINARY | VARBINARY |

### 動作

- フィールド名は大文字と小文字を区別せず、順序は任意です。不明な名前や重複した名前は解析エラーになります。
- 省略したフィールドと明示的な NULL は NULL になります。`offset` と `size` を含め、デフォルト値は補完しません。
- NULL STRUCT は NULL を返し、フィールド値の検証を省略します。非 NULL の各 STRUCT では、`uri` と `inline` のどちらか一方だけが非 NULL である必要があります。省略したフィールドは NULL とみなします。
- 整数入力は BIGINT に拡張されます。すべてのフィールドで型未指定の NULL を使用できます。その他の入力型には明示的な CAST が必要です。
- `offset` と `size` は NULL にできますが、非 NULL の値は 0 以上である必要があります。いずれかの行がこのルール、または `uri` と `inline` のルールに違反すると、クエリは失敗します。
- URI の構文、MIME タイプ、チェックサム、size と実際のバイト長の整合性は検証しません。
- `inline` は生のバイト列です。空文字列と長さゼロのバイナリ値も非 NULL とみなし、そのまま保持します。

### 例

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

[TO_FILE](#to_file) と同じフィールド、型、検証ルールを使用して一時的な FILE を構築します。フィールド値が無効な行だけが NULL になり、他の行には影響しません。

### 構文

```sql
TRY_TO_FILE(struct_expr)
```

### 動作

- NULL STRUCT は NULL を返します。非 NULL STRUCT では、`uri` と `inline` が両方 NULL または両方非 NULL の場合、あるいは非 NULL の `offset` または `size` が負の場合、その行は NULL を返します。
- 不明なフィールド、重複したフィールド、未対応の型、引数の数の誤りは引き続き解析エラーになります。引数式の評価エラー、内部エラー、リソースエラーは NULL に変換しません。

### 例

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

`uri` フィールドを取得し、VARCHAR を返します。

### 構文

```sql
FL_GET_URI(file_expr)
```

### 例

```sql
SELECT FL_GET_URI(TO_FILE(named_struct('uri', 'https://example.com/a.jpg')));
-- https://example.com/a.jpg
```

## fl_get_offset

`offset` フィールドを取得し、BIGINT を返します。

### 構文

```sql
FL_GET_OFFSET(file_expr)
```

### 例

```sql
SELECT FL_GET_OFFSET(TO_FILE(named_struct('uri', 'a.jpg', 'offset', 0)));
-- 0
```

## fl_get_size

`size` フィールドを取得し、BIGINT を返します。

### 構文

```sql
FL_GET_SIZE(file_expr)
```

### 例

```sql
SELECT FL_GET_SIZE(TO_FILE(named_struct('uri', 'a.jpg', 'size', 12)));
-- 12
```

## fl_get_content_type

`content_type` フィールドを取得し、VARCHAR を返します。

### 構文

```sql
FL_GET_CONTENT_TYPE(file_expr)
```

### 例

```sql
SELECT FL_GET_CONTENT_TYPE(TO_FILE(named_struct('uri', 'a.jpg', 'content_type', 'image/jpeg')));
-- image/jpeg
```

## fl_get_checksum

`checksum` フィールドを取得し、VARCHAR を返します。

### 構文

```sql
FL_GET_CHECKSUM(file_expr)
```

### 例

```sql
SELECT FL_GET_CHECKSUM(TO_FILE(named_struct('uri', 'a.jpg', 'checksum', 'opaque')));
-- opaque
```

## fl_get_inline

`inline` フィールドを取得し、VARBINARY を返します。

### 構文

```sql
FL_GET_INLINE(file_expr)
```

### 例

```sql
SELECT HEX(FL_GET_INLINE(TO_FILE(named_struct('inline', to_binary('610062', 'hex')))));
-- 610062
```
