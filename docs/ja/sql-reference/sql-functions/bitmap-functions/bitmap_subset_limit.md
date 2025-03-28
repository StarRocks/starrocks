---
displayed_sidebar: docs
---

# bitmap_subset_limit

`start range` から始まる要素値を持つ BITMAP 値から指定された数の要素を抽出します。出力される要素は `src` の部分集合です。

この関数は、主にページネーションされたクエリなどのシナリオで使用されます。v3.1 からサポートされています。

この関数は [sub_bitmap](./sub_bitmap.md) に似ていますが、要素値 (`start_range`) から要素を抽出する点が異なります。sub_bitmap はオフセットから要素を抽出します。

## 構文

```Haskell
BITMAP bitmap_subset_limit(BITMAP src, BIGINT start_range, BIGINT limit)
```

## パラメータ

- `src`: 要素を取得する元の BITMAP 値。
- `start_range`: 要素を抽出する開始範囲。BIGINT 値でなければなりません。指定された開始範囲が BITMAP 値の最大要素を超え、`limit` が正の場合、NULL が返されます。例 4 を参照してください。
- `limit`: `start_range` から取得する要素の数。負のリミットは右から左にカウントされます。マッチする要素の数が `limit` の値より少ない場合、すべてのマッチする要素が返されます。

## 戻り値

BITMAP 型の値を返します。入力パラメータが無効な場合は NULL が返されます。

## 使用上の注意

- 部分集合の要素には `start range` が含まれます。
- 負のリミットは右から左にカウントされます。例 3 を参照してください。

## 例

以下の例では、bitmap_subset_limit() の入力は [bitmap_from_string](./bitmap_from_string.md) の出力です。例えば、`bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` は `1, 3, 5, 7, 9` を返します。bitmap_subset_limit() はこの BITMAP 値を入力として受け取ります。

例 1: 要素値が 1 から始まる BITMAP 値から 4 つの要素を取得します。

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 4)) value;
+---------+
|  value  |
+---------+
| 1,3,5,7 |
+---------+
```

例 2: 要素値が 1 から始まる BITMAP 値から 100 個の要素を取得します。リミットが BITMAP 値の長さを超えており、すべてのマッチする要素が返されます。

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

例 3: 要素値が 5 から始まる BITMAP 値から -2 個の要素を取得します（右から左にカウント）。

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 5, -2)) value;
+-----------+
| value     |
+-----------+
| 3,5       |
+-----------+
```

例 4: 開始範囲 10 が BITMAP 値 `1,3,5,7,9` の最大要素を超えており、リミットが正です。NULL が返されます。

```Plain
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 10, 15)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```