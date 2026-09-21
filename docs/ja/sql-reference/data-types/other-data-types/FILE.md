---
displayed_sidebar: docs
description: "FILE は、固定されたフィールドの組を通じてファイルまたはファイル内のバイト範囲を参照する読み取り専用のスカラー型です。"
---

# FILE

FILE は、ファイルまたはファイル内部のバイト範囲を表すスカラー型です。FILE 値は全体として読み取られ、全体として返されます。固定されたフィールドの組を持ちますが、各フィールドを SQL で個別に参照することはできません。

現時点では FILE 型は永続化をサポートしておらず、FILE 値は External Catalog からのみ取得できます。

## 構造

すべての FILE 値は、次の順序で同じフィールドを持ちます。各フィールドは NULL になり得ます。

| フィールド        | 型         | 説明                                                                 |
|----------------|-----------|----------------------------------------------------------------------|
| `uri`          | VARCHAR   | ファイルの場所。オブジェクトストレージや HDFS のパスなど。                  |
| `offset`       | BIGINT    | 参照する内容が始まるファイル内のバイト位置。                                |
| `size`         | BIGINT    | 参照する内容のバイト数。`offset` から数えます。                             |
| `content_type` | VARCHAR   | 内容のメディアタイプ。`image/png` など。                                  |
| `checksum`     | VARCHAR   | 参照する内容のチェックサム。                                              |
| `inline`       | VARBINARY | `uri` で参照するのではなく、値の中に直接格納された内容そのもの。               |

FILE 値は次のいずれかの形をとります。

- **参照**: `uri` が設定され、通常は `offset` と `size` も伴います。`inline` は NULL です。
- **インライン内容**: `inline` が内容のバイト列を保持し、`uri`、`offset`、`size` は NULL です。

## 出力形式

FILE 値は、すべてのフィールドを列挙した JSON 風のオブジェクトとして表示されます。

```plain text
{"uri":"s3://bucket/images/a.png","offset":4,"size":8,"content_type":null,"checksum":null,"inline":null}
{"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":"89504e470d0a1a0a"}
```

`inline` のバイト列は、セッション変数 [`binary_encoding_format`](../../System_variable.md#binary_encoding_format) に従ってエンコードされます。

## 制限事項

- FILE は CREATE TABLE、CREATE TABLE AS SELECT、マテリアライズドビューのカラム型として使用できません。
- FILE 値は比較、ソート、グループ化、JOIN 条件、DISTINCT、他の型との相互変換、ウィンドウ関数の引数には使用できません。
- `IS NULL` と `IS NOT NULL` はサポートされます。
