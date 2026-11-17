---
displayed_sidebar: docs
description: "STRUCT 式から FILE 値を構築します。"
sidebar_position: 10
products: [oss, enterprise, cloud, anywhere]
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# to_file

<Experimental />

`named_struct(...)` や STRUCT カラムなどの STRUCT 式から一時的な [FILE](../../data-types/other-data-types/FILE.md) 値を構築します。ストレージへのアクセスやメタデータの推論は行いません。

## 構文

```sql
FILE to_file(STRUCT struct_expr)
```

## パラメータ

`struct_expr`：ファイルを記述する STRUCT 式。次のフィールドを使用できます。

| フィールド | 使用できる入力型 | FILE フィールドの型 |
|---|---|---|
| `uri`, `content_type`, `checksum` | CHAR, VARCHAR | VARCHAR |
| `offset`, `size` | TINYINT, SMALLINT, INT, BIGINT | BIGINT |
| `inline` | VARBINARY | VARBINARY |

## 戻り値

FILE 値を返します。`struct_expr` が NULL の場合は NULL を返します。

## 使用上の注意

- フィールド名は大文字と小文字を区別せず、順序は任意です。不明な名前や重複した名前は解析エラーになります。
- 省略したフィールドと明示的な NULL は NULL になります。`offset` と `size` を含め、デフォルト値は補完しません。
- 非 NULL の各 STRUCT では、`uri` と `inline` のどちらか一方だけが非 NULL である必要があります。省略したフィールドは NULL とみなします。
- 整数入力は BIGINT に拡張されます。すべてのフィールドで型未指定の NULL を使用できます。その他の入力型には明示的な CAST が必要です。
- URI 参照の `offset` と `size` は NULL にできますが、非 NULL の値は 0 以上である必要があります。インライン内容では、両方とも NULL である必要があります。いずれかの行がこのルール、または `uri` と `inline` のルールに違反すると、クエリは失敗します。このような行で NULL を返すには、[try_to_file](try_to_file.md) を使用してください。
- URI の構文、MIME タイプ、チェックサム、参照するバイト範囲は検証しません。
- `inline` は生のバイト列です。空文字列と長さゼロのバイナリ値も非 NULL とみなし、そのまま保持します。

## 例

### 例 1：ファイル参照を構築する

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

### 例 2：インライン内容を構築する

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

### 例 3：NULL 引数

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

## キーワード

TO_FILE, FILE
