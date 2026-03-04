---
displayed_sidebar: docs
---

# VARIANT

:::important
VARIANT 型は Iceberg Catalog のテーブルでのみサポートされています。StarRocks ネイティブテーブルではサポートされていません。
:::

v4.1 以降、StarRocks は Parquet 形式の Iceberg テーブルから半構造化データをクエリするための VARIANT データ型をサポートしています。本記事では、VARIANT の基本概念、StarRocks における VARIANT 型データのクエリ方法、および VARIANT 関数による処理方法について説明します。

## VARIANT とは

VARIANT は半構造化データ型であり、複数の異なるデータ型の値を格納できます。これには、プリミティブ型（整数、浮動小数点数、文字列、ブール値、日付、タイムスタンプ）および複雑型（STRUCT、MAP、ARRAY）が含まれます。VARIANT データは効率的な保存およびクエリ処理のためにバイナリ形式でエンコードされます。

VARIANT 型は、Variant エンコーディングを使用する Parquet 形式の Apache Iceberg テーブルのデータを扱う場合に特に有用です。このエンコーディングにより、柔軟なスキーマ進化と異種データの効率的な保存が可能になります。

Parquet の Variant エンコーディング形式の詳細については、[Parquet Variant Encoding 仕様](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)を参照してください。

## VARIANT データの使用

### Iceberg テーブルからの VARIANT 型データのクエリ

StarRocks は、Parquet 形式で保存された Iceberg テーブルの VARIANT 型データのクエリをサポートしています。Parquet の Variant エンコーディングを使用する Iceberg テーブルをクエリする際、VARIANT 列は自動的に認識されます。

Iceberg テーブルの VARIANT 列をクエリする例：

```SQL
-- VARIANT 列を持つテーブルをクエリする
SELECT
    id,
    variant_col
FROM iceberg_catalog.db.table_with_variants;
```

### VARIANT データからの値の抽出

StarRocks は、VARIANT データから型付きの値を抽出するための複数の関数を提供しています。

例 1: 型付き getter 関数を使用してプリミティブ値を抽出する。

```SQL
SELECT
    get_variant_int(variant_col, '$') AS int_value,
    get_variant_string(variant_col, '$') AS string_value,
    get_variant_bool(variant_col, '$') AS bool_value,
    get_variant_double(variant_col, '$') AS double_value
FROM iceberg_catalog.db.table_with_variants;
```

例 2: JSON パス式を使用してネストされた構造をナビゲートする。

```SQL
SELECT
    get_variant_string(variant_col, '$.user.name') AS user_name,
    get_variant_int(variant_col, '$.user.age') AS user_age,
    get_variant_string(variant_col, '$.address.city') AS city
FROM iceberg_catalog.db.table_with_variants;
```

例 3: 配列要素にアクセスする。

```SQL
SELECT
    get_variant_int(variant_col, '$.scores[0]') AS first_score,
    get_variant_int(variant_col, '$.scores[1]') AS second_score
FROM iceberg_catalog.db.table_with_variants;
```

例 4: ネストされた VARIANT データをクエリし、VARIANT 型として返す。

```SQL
SELECT
    variant_query(variant_col, '$.metadata') AS metadata,
    variant_query(variant_col, '$.items[0]') AS first_item
FROM iceberg_catalog.db.table_with_variants;
```

例 5: VARIANT 値の型を確認する。

```SQL
SELECT
    variant_typeof(variant_col) AS root_type,
    variant_typeof(variant_query(variant_col, '$.data')) AS data_type
FROM iceberg_catalog.db.table_with_variants;
```

### JSON から VARIANT へのキャスト

StarRocks は JSON 値を VARIANT にキャストすることをサポートしています。入力が STRING 型の場合は、まず JSON に変換してから VARIANT にキャストしてください。

```SQL
SELECT
    CAST(parse_json('{"id": 1, "flags": {"active": true}, "scores": [1.5, null]}') AS VARIANT) AS variant_value;
```

```SQL
SELECT
    CAST(json_col AS VARIANT) AS variant_value
FROM db.table_with_json;
```

### VARIANT から SQL 型へのキャスト

CAST 関数を使用して、VARIANT データを標準的な SQL 型に変換できます。

```SQL
SELECT
    CAST(variant_query(variant_col, '$.count') AS INT) AS count,
    CAST(variant_query(variant_col, '$.price') AS DECIMAL(10, 2)) AS price,
    CAST(variant_query(variant_col, '$.active') AS BOOLEAN) AS is_active,
    CAST(variant_query(variant_col, '$.name') AS STRING) AS name
FROM iceberg_catalog.db.table_with_variants;
```

複雑型も VARIANT からキャストできます。

```SQL
SELECT
    CAST(variant_col AS STRUCT<id INT, name STRING>) AS user_struct,
    CAST(variant_col AS MAP<STRING, INT>) AS config_map,
    CAST(variant_col AS ARRAY<DOUBLE>) AS values_array
FROM iceberg_catalog.db.table_with_variants;
```

### SQL 型から VARIANT へのキャスト

SQL値をVARIANT型にキャストできます。サポートされる入力型には、BOOLEAN、整数型、FLOAT/DOUBLE、DECIMAL、STRING/CHAR/VARCHAR、JSON、DATE/DATETIME/TIME、および複合型（ARRAY、MAP、STRUCT）が含まれます。MAP 型の場合、エンコード時にキーは文字列へキャストされます。HLL、BITMAP、PERCENTILE、VARBINARY などの型はサポートされていません。

```SQL
SELECT
    CAST(123 AS VARIANT) AS v_int,
    CAST(3.14 AS VARIANT) AS v_double,
    CAST(DECIMAL(10, 2) '12.34' AS VARIANT) AS v_decimal,
    CAST('hello' AS VARIANT) AS v_string,
    CAST(PARSE_JSON('{"k":1}') AS VARIANT) AS v_json;
```

## VARIANT 関数

VARIANT 関数は、VARIANT 列からデータをクエリおよび抽出するために使用されます。詳細については、各関数のドキュメントを参照してください。

- [variant_query](../../sql-functions/variant-functions/variant_query.md): VARIANT 値内の指定パスをクエリし、VARIANT を返します
- [get_variant](../../sql-functions/variant-functions/get_variant.md): VARIANT から型付きの値（int、bool、double、string）を抽出します
- [variant_typeof](../../sql-functions/variant-functions/variant_typeof.md): VARIANT 値の型名を返します

## VARIANT パス式

VARIANT 関数では、データ構造をナビゲートするために JSON パス式を使用します。パス構文は JSON パスと類似しています。

- `$` は VARIANT 値のルートを表します
- `.` はオブジェクトのフィールドにアクセスするために使用します
- `[index]` は配列要素（0 始まり）にアクセスするために使用します
- 特殊文字（例: ドット）を含むフィールド名は引用符で囲むことができます（例: `$."field.name"`）

パス式の例：

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

Variant エンコーディングを使用した Parquet ファイルからデータを読み取る際、以下の型変換がサポートされています。

| Parquet Variant 型 | StarRocks VARIANT 型 |
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

## 制限事項および注意点

- VARIANT は、variant エンコーディングを使用した Parquet 形式の Iceberg テーブルからの読み取り、および StarRocks のファイルライターによる Parquet ファイル（unshredded variant encoding）の書き込みでサポートされます。
- VARIANT 値のサイズは最大 16 MB です。
- 現在、読み取りおよび書き込みの両方で unshredded variant 値のみサポートされています。
- VARIANT は、JSON 値またはサポートされている SQL 型（ARRAY、MAP、STRUCT を含む）からのキャストによって作成できます。
- ネスト構造の最大深度は、基盤となる Parquet ファイルの構造に依存します。
