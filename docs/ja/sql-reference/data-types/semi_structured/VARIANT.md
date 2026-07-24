---
displayed_sidebar: docs
description: "VARIANTタイプは、Iceberg Catalogのテーブルにのみサポートされています。"
---

# VARIANT

:::important
VARIANT型はIceberg Catalogのテーブルでのみサポートされています。StarRocksネイティブテーブルではサポートされていません。
:::

v4.1以降、StarRocksはParquet形式のIcebergテーブルから半構造化データをクエリするためのVARIANTデータ型をサポートしています。この記事では、VARIANTの基本概念と、StarRocksがVARIANT型データをクエリしてVARIANT関数で処理する方法を紹介します。

## VARIANTとは

VARIANTは、プリミティブ型（整数、浮動小数点数、文字列、ブール値、日付、タイムスタンプ）や複合型（構造体、マップ、配列）を含む、さまざまなデータ型の値を格納できる半構造化データ型です。VARIANTデータは、効率的なストレージとクエリのためにバイナリ形式でエンコードされます。

VARIANT型は、バリアントエンコーディングを使用したParquet形式のApache Icebergテーブルのデータを扱う際に特に有用であり、柔軟なスキーマ進化と異種データの効率的なストレージを可能にします。

Parquetバリアントエンコーディング形式の詳細については、[Parquet Variant Encoding仕様](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)を参照してください。

## VARIANTデータの使用

### IcebergテーブルからのVARIANT型データのクエリ

StarRocksは、Parquet形式で保存されたIcebergテーブルからVARIANT型データのクエリをサポートしています。VARIANTカラムは、ParquetのバリアントエンコーディングをするIcebergテーブルをクエリする際に自動的に認識されます。

VARIANTカラムを持つIcebergテーブルのクエリ例：

```SQL
-- VARIANTカラムを持つテーブルのクエリ
SELECT
    id,
    variant_col
FROM iceberg_catalog.db.table_with_variants;
```

### VARIANTデータからの値の抽出

StarRocksは、VARIANTデータから型付きの値を抽出するためのいくつかの関数を提供しています。

例1：型付きゲッター関数を使用してプリミティブ値を抽出する。

```SQL
SELECT
    get_variant_int(variant_col, '$') AS int_value,
    get_variant_string(variant_col, '$') AS string_value,
    get_variant_bool(variant_col, '$') AS bool_value,
    get_variant_double(variant_col, '$') AS double_value
FROM iceberg_catalog.db.table_with_variants;
```

例2：JSONパス式を使用してネストされた構造をナビゲートする。

```SQL
SELECT
    get_variant_string(variant_col, '$.user.name') AS user_name,
    get_variant_int(variant_col, '$.user.age') AS user_age,
    get_variant_string(variant_col, '$.address.city') AS city
FROM iceberg_catalog.db.table_with_variants;
```

例3：配列要素にアクセスする。

```SQL
SELECT
    get_variant_int(variant_col, '$.scores[0]') AS first_score,
    get_variant_int(variant_col, '$.scores[1]') AS second_score
FROM iceberg_catalog.db.table_with_variants;
```

例4：ネストされたバリアントデータをクエリしてVARIANT型を返す。

```SQL
SELECT
    variant_query(variant_col, '$.metadata') AS metadata,
    variant_query(variant_col, '$.items[0]') AS first_item
FROM iceberg_catalog.db.table_with_variants;
```

例5：VARIANT値の型を確認する。

```SQL
SELECT
    variant_typeof(variant_col) AS root_type,
    variant_typeof(variant_query(variant_col, '$.data')) AS data_type
FROM iceberg_catalog.db.table_with_variants;
```

### JSONからVARIANTへのキャスト

StarRocksはJSON値をVARIANTにキャストすることをサポートしています。入力がSTRINGの場合は、まずJSONに変換してください。

```SQL
SELECT
    CAST(parse_json('{"id": 1, "flags": {"active": true}, "scores": [1.5, null]}') AS VARIANT) AS variant_value;
```

```SQL
SELECT
    CAST(json_col AS VARIANT) AS variant_value
FROM db.table_with_json;
```

### VARIANTデータのSQL型へのキャスト

CAST関数を使用してVARIANTデータを標準SQL型に変換できます：

```SQL
SELECT
    CAST(variant_query(variant_col, '$.count') AS INT) AS count,
    CAST(variant_query(variant_col, '$.price') AS DECIMAL(10, 2)) AS price,
    CAST(variant_query(variant_col, '$.active') AS BOOLEAN) AS is_active,
    CAST(variant_query(variant_col, '$.name') AS STRING) AS name
FROM iceberg_catalog.db.table_with_variants;
```

複合型もVARIANTからキャストできます：

```SQL
SELECT
    CAST(variant_col AS STRUCT<id INT, name STRING>) AS user_struct,
    CAST(variant_col AS MAP<STRING, INT>) AS config_map,
    CAST(variant_col AS ARRAY<DOUBLE>) AS values_array
FROM iceberg_catalog.db.table_with_variants;
```

### SQL型からVARIANTへのキャスト

SQL値をVARIANTにキャストできます。サポートされている入力型には、BOOLEAN、整数型、FLOAT/DOUBLE、DECIMAL、STRING/CHAR/VARCHAR、JSON、DATE/DATETIME/TIME、および複合型（ARRAY、MAP、STRUCT）が含まれます。MAPの場合、キーはエンコード時に文字列にキャストされます。HLL、BITMAP、PERCENTILE、VARBINARYなどの型はサポートされていません。

```SQL
SELECT
    CAST(123 AS VARIANT) AS v_int,
    CAST(3.14 AS VARIANT) AS v_double,
    CAST(CAST('12.34' AS DECIMAL(10,2)) AS VARIANT) AS v_decimal,
    CAST('hello' AS VARIANT) AS v_string,
    CAST(PARSE_JSON('{"k":1}') AS VARIANT) AS v_json;
```

## VARIANT関数

VARIANT関数は、VARIANTカラムからデータをクエリおよび抽出するために使用できます。詳細については、各関数のドキュメントを参照してください：

- [variant_query](../../sql-functions/variant-functions/variant_query.md)：VARIANT値のパスをクエリしてVARIANTを返す
- [get_variant](../../sql-functions/variant-functions/get_variant.md)：VARIANTから型付きの値（int、bool、double、string）を抽出する
- [variant_typeof](../../sql-functions/variant-functions/variant_typeof.md)：VARIANT値の型名を返す

## VARIANTパス式

VARIANT関数はJSONパス式を使用してデータ構造をナビゲートします。パス構文はJSONパスに似ています：

- `$` はVARIANT値のルートを表します
- `.` はオブジェクトフィールドへのアクセスに使用されます
- `[index]` は配列要素へのアクセスに使用されます（0ベースのインデックス）
- 特殊文字（ドットなど）を含むフィールド名はクォートできます: `$."field.name"`

パス式の例:

```plaintext
$                      -- Root element
$.field                -- Object field access
$.nested.field         -- Nested field access
$."field.with.dots"    -- Quoted field name
$[0]                   -- First array element
$.array[1]             -- Second element of array field
$.users[0].name        -- Nested array access
$.config["key"]        -- Map-style access
```

## データ型変換

バリアントエンコーディングを使用したParquetファイルからデータを読み取る場合、以下の型変換がサポートされています:

| Parquet バリアント型 | StarRocks VARIANT 型 |
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

## 制限事項と注意点

- VARIANTは、バリアントエンコーディングを使用したParquet形式のIcebergテーブルからのデータ読み取り、およびStarRocksファイルライター（非シュレッドバリアントエンコーディング）を使用したParquetファイルへの書き込みに対応しています。
- VARIANT値のサイズは16 MBに制限されています。
- 現在、読み取りと書き込みの両方において、非シュレッドバリアント値のみがサポートされています。
- VARIANTは、JSON値またはサポートされているSQLタイプ（ARRAY、MAP、STRUCTを含む）からのキャストによって作成できます。
- ネスト構造の最大深度は、基となるParquetファイルの構造に依存します。
