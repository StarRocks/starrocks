---
displayed_sidebar: docs
---

# element_at

指定された位置（インデックス）から配列の要素を返します。パラメータが NULL の場合や、指定された位置が存在しない場合、結果は NULL になります。

この関数は添字演算子 `[]` の別名です。v3.0 以降でサポートされています。

マップ内のキーと値のペアから値を取得したい場合は、[element_at](../map-functions/element_at.md) を参照してください。

## 構文

```Haskell
element_at(any_array, position)
```

## パラメータ

- `any_array`: 要素を取得する配列式。
- `position`: 配列内の要素の位置。正の整数でなければなりません。値の範囲: [1, 配列の長さ]。`position` が存在しない場合、NULL が返されます。

## 例

```plain text
mysql> select element_at([2,3,11],3);
+---------------+
| [2, 3, 11][3] |
+---------------+
|            11 |
+---------------+

mysql> select element_at(["a","b","c"],1);
+--------------------+
| ['a', 'b', 'c'][1] |
+--------------------+
| a                  |
+--------------------+
```

## キーワード

ELEMENT_AT, ARRAY