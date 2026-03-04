---
displayed_sidebar: docs
---

# variant_typeof

VARIANT 値の型名を文字列として返します。

この関数は VARIANT 値の実際のデータ型を検査し、その型名を文字列として返します。VARIANT データの構造を把握したい場合や、データ型に応じて条件分岐処理を行う場合に有用です。

## 構文

```Haskell
VARCHAR variant_typeof(variant_expr)
```

## パラメータ

- `variant_expr`: VARIANT オブジェクトを表す式。通常は Iceberg テーブルの VARIANT 列、または `variant_query` が返す VARIANT 値を指定します。

## 戻り値

型名を表す VARCHAR 値を返します。

主な戻り値は次のとおりです。
- `"Null"` - NULL 値
- `"Boolean(true)"` - true のブール値
- `"Boolean(false)"` - false のブール値
- `"Int8"`, `"Int16"`, `"Int32"`, `"Int64"` - 整数値
- `"Float"`, `"Double"` - 浮動小数点値
- `"Decimal4"`, `"Decimal8"`, `"Decimal16"` - 異なる精度を持つ DECIMAL 値
- `"String"` - 文字列値
- `"Binary"` - バイナリデータ
- `"Date"` - 日付値
- `"TimestampTz"`, `"TimestampNtz"` - タイムゾーンあり／なしのタイムスタンプ値
- `"TimestampTzNanos"`, `"TimestampNtzNanos"` - ナノ秒精度のタイムスタンプ値
- `"TimeNtz"` - タイムゾーンなしの時刻値
- `"Uuid"` - UUID 値
- `"Object"` - 構造体（struct）またはマップ値
- `"Array"` - 配列値

## 例

例 1: ルートレベルの VARIANT 値の型を取得します。

```SQL
SELECT variant_typeof(data) AS type
FROM bluesky
LIMIT 1;
```

```plaintext
+--------+
| type   |
+--------+
| Object |
+--------+
```

例 2: ネストされたフィールドの型を取得します。

```SQL
SELECT
    variant_typeof(variant_query(data, '$.kind')) AS kind_type,
    variant_typeof(variant_query(data, '$.did')) AS did_type,
    variant_typeof(variant_query(data, '$.commit')) AS commit_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-----------+----------+-------------+
| kind_type | did_type | commit_type |
+-----------+----------+-------------+
| String    | String   | Object      |
+-----------+----------+-------------+
```

例 3: 複数フィールドの型を確認します。

```SQL
SELECT
    variant_typeof(variant_query(data, '$.commit')) AS commit_type,
    variant_typeof(variant_query(data, '$.time_us')) AS time_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------+-----------+
| commit_type | time_type |
+-------------+-----------+
| Object      | Int64     |
+-------------+-----------+
```

例 4: 条件分岐の中で使用し、型に応じた処理を行います。

```SQL
SELECT
    CASE variant_typeof(variant_query(data, '$.time_us'))
        WHEN 'Int64' THEN get_variant_int(data, '$.time_us')
        ELSE NULL
    END AS timestamp_value
FROM bluesky
LIMIT 3;
```

```plaintext
+------------------+
| timestamp_value  |
+------------------+
| 1733267476040329 |
| 1733267476040803 |
| 1733267476041472 |
+------------------+
```

例 5: 型に基づいて行をフィルタリングします。

```SQL
SELECT COUNT(*) AS object_count
FROM bluesky
WHERE variant_typeof(variant_query(data, '$.commit')) = 'Object';
```

```plaintext
+--------------+
| object_count |
+--------------+
| 9960624      |
+--------------+
```

## Keywords

VARIANT_TYPEOF,VARIANT
