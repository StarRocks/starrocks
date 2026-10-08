---
displayed_sidebar: docs
description: "VARIANT 値が VARIANT null（JSON null など）かどうかを返します。"
---

# is_variant_null

VARIANT 値が VARIANT null（JSON null など）かどうかを返します。

VARIANT null は値です。フィールドが存在し、その値が `null` であることを表します。パスが存在しない場合に `variant_query` が返す SQL NULL とは異なります。この関数を使うと、この 2 つのケースを区別できます。

## 構文

```Haskell
BOOLEAN is_variant_null(variant_expr)
```

## パラメータ

- `variant_expr`: VARIANT オブジェクトを表す式。通常は Iceberg テーブルの VARIANT 列、または `variant_query` が返す VARIANT 値を指定します。

## 戻り値

BOOLEAN 値を返します。

- 値が VARIANT null の場合は `true` を返します。
- それ以外の値、および SQL NULL の場合は `false` を返します。この関数は NULL を返しません。

| パス上の値         | `variant_query(v, path)` | `variant_typeof(...)` | `is_variant_null(...)` |
| ------------------ | ------------------------ | --------------------- | ---------------------- |
| `null`             | VARIANT null             | `Null`                | `true`                 |
| その他の値         | VARIANT 値               | 型名                  | `false`                |
| パスが存在しない   | SQL NULL                 | NULL                  | `false`                |

## 例

例 1: 値が `null` のフィールドと、存在しないフィールドを区別します。

```SQL
SELECT
    is_variant_null(variant_query(PARSE_JSON('{"a": null}'), '$.a')) AS json_null,
    is_variant_null(variant_query(PARSE_JSON('{"a": 1}'), '$.a')) AS has_value,
    is_variant_null(variant_query(PARSE_JSON('{"b": 1}'), '$.a')) AS missing;
```

```plaintext
+-----------+-----------+---------+
| json_null | has_value | missing |
+-----------+-----------+---------+
|         1 |         0 |       0 |
+-----------+-----------+---------+
```

例 2: フィールド `a` が存在し、値が `null` である行を数えます。

```SQL
SELECT COUNT(*)
FROM t
WHERE is_variant_null(variant_query(v, '$.a'));
```

## keyword

IS_VARIANT_NULL,VARIANT
