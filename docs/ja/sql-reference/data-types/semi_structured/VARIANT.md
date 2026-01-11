---
displayed_sidebar: docs
---

# VARIANT

StarRocks は 4.0 以降、Parquet 形式の Iceberg テーブルに含まれる半構造化データを扱うために VARIANT 型をサポートします。本記事では VARIANT の基本概念と、VARIANT データのクエリ方法、variant 関数の使い方を説明します。

## VARIANT とは

VARIANT は複数のデータ型を格納できる半構造化データ型です。基本型（整数、浮動小数点数、文字列、ブール、日付、タイムスタンプ）と複雑型（STRUCT、MAP、ARRAY）を保持できます。VARIANT はバイナリ形式でエンコードされ、効率的に保存・クエリされます。

Parquet の variant エンコーディング仕様については [Parquet Variant Encoding specification](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) を参照してください。

## VARIANT データの使用

### Iceberg テーブルの VARIANT 列をクエリする

Parquet の variant エンコーディングを使用する Iceberg テーブルをクエリする場合、StarRocks は VARIANT 列を自動的に認識します。

```SQL
SELECT
    id,
    variant_col
FROM iceberg_catalog.db.table_with_variants;
```

### VARIANT から値を取り出す

VARIANT から型付きの値を取り出すために、以下の関数が提供されています。

```SQL
SELECT
    get_variant_int(variant_col, '$') AS int_value,
    get_variant_string(variant_col, '$') AS string_value,
    get_variant_bool(variant_col, '$') AS bool_value,
    get_variant_double(variant_col, '$') AS double_value
FROM iceberg_catalog.db.table_with_variants;
```

```SQL
SELECT
    get_variant_string(variant_col, '$.user.name') AS user_name,
    get_variant_int(variant_col, '$.user.age') AS user_age,
    get_variant_string(variant_col, '$.address.city') AS city
FROM iceberg_catalog.db.table_with_variants;
```

```SQL
SELECT
    get_variant_int(variant_col, '$.scores[0]') AS first_score,
    get_variant_int(variant_col, '$.scores[1]') AS second_score
FROM iceberg_catalog.db.table_with_variants;
```

```SQL
SELECT
    variant_query(variant_col, '$.metadata') AS metadata,
    variant_query(variant_col, '$.items[0]') AS first_item
FROM iceberg_catalog.db.table_with_variants;
```

```SQL
SELECT
    variant_typeof(variant_col) AS root_type,
    variant_typeof(variant_query(variant_col, '$.data')) AS data_type
FROM iceberg_catalog.db.table_with_variants;
```

### VARIANT を SQL 型にキャスト

```SQL
SELECT
    CAST(variant_query(variant_col, '$.count') AS INT) AS count,
    CAST(variant_query(variant_col, '$.price') AS DECIMAL(10, 2)) AS price,
    CAST(variant_query(variant_col, '$.active') AS BOOLEAN) AS is_active,
    CAST(variant_query(variant_col, '$.name') AS STRING) AS name
FROM iceberg_catalog.db.table_with_variants;
```

複雑型へのキャストも可能です。

```SQL
SELECT
    CAST(variant_col AS STRUCT<id INT, name STRING>) AS user_struct,
    CAST(variant_col AS MAP<STRING, INT>) AS config_map,
    CAST(variant_col AS ARRAY<DOUBLE>) AS values_array
FROM iceberg_catalog.db.table_with_variants;
```

### SQL 型を VARIANT にキャスト

SQL 値を VARIANT にキャストできます。対応する入力型は BOOLEAN、整数型、FLOAT/DOUBLE、DECIMAL、STRING/CHAR/VARCHAR、JSON、DATE/DATETIME/TIME です。HLL、BITMAP、PERCENTILE、VARBINARY、および複雑型（ARRAY、MAP、STRUCT）は未対応です。

```SQL
SELECT
    CAST(123 AS VARIANT) AS v_int,
    CAST(3.14 AS VARIANT) AS v_double,
    CAST(DECIMAL(10, 2) '12.34' AS VARIANT) AS v_decimal,
    CAST('hello' AS VARIANT) AS v_string,
    CAST(PARSE_JSON('{"k":1}') AS VARIANT) AS v_json;
```

## VARIANT 関数

VARIANT 関数については、各関数のドキュメントを参照してください。

- [variant_query](../../sql-functions/variant-functions/variant_query.md)
- [get_variant](../../sql-functions/variant-functions/get_variant.md)
- [variant_typeof](../../sql-functions/variant-functions/variant_typeof.md)

## VARIANT パス式

- `$` はルート要素
- `.` はオブジェクトのフィールドアクセス
- `[index]` は配列アクセス（0-based）

```plaintext
$
$.field
$.nested.field
$."field.with.dots"
$[0]
$.array[1]
$.users[0].name
$.config["key"]
```

## データ型変換

Parquet の variant エンコーディングを読み込む際にサポートされる型変換は以下の通りです。

| Parquet Variant Type | StarRocks VARIANT Type |
| -------------------- | ------------ |
| INT8, INT16, INT32, INT64 | int8, int16, int32, int64 |
| FLOAT, DOUBLE | float, double |
| BOOLEAN | boolean |
| STRING | string |
| DATE | Date |
| TIMESTAMP, TIMESTAMP_NTZ | Timestamp |
| DECIMAL | Decimal, float, double|
| STRUCT | Object |
| MAP | Object |
| ARRAY | Array |

## 制限事項

- VARIANT は Parquet の variant エンコーディングを使用する Iceberg テーブルの読み取り、または StarRocks ファイルライターでの Parquet 書き込み（unshredded variant）に対応しています。
- VARIANT の最大サイズは 16 MB です。
- 現在は unshredded variant のみ対応しています。
- ネストの深さは Parquet ファイル構造に依存します。
