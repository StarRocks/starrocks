---
displayed_sidebar: docs
---

# array_to_bitmap

## 説明

配列を BITMAP 値に変換します。この関数は v2.3 からサポートされています。

## 構文

```Haskell
BITMAP array_to_bitmap(array)
```

## パラメータ

`array`: 配列内の要素は BIGINT、INT、TINYINT、または SMALLINT 型である必要があります。

## 戻り値

BITMAP 型の値を返します。

## 使用上の注意

- 入力配列の要素のデータ型が STRING や DECIMAL のように無効な場合、エラーが返されます。

- 空の配列が入力された場合、空の BITMAP 値が返されます。

- `NULL` が入力された場合、`NULL` が返されます。

## 例

例 1: 配列を BITMAP 値に変換します。この関数は BITMAP 値を表示できないため、`bitmap_to_array` にネストする必要があります。

```Plain
MySQL > select bitmap_to_array(array_to_bitmap([1,2,3]));
+-------------------------------------------+
| bitmap_to_array(array_to_bitmap([1,2,3])) |
+-------------------------------------------+
| [1,2,3]                                   |
+-------------------------------------------+
```

例 2: 空の配列を入力し、空の配列が返されます。

```Plain
MySQL > select bitmap_to_array(array_to_bitmap([]));
+--------------------------------------+
| bitmap_to_array(array_to_bitmap([])) |
+--------------------------------------+
| []                                   |
+--------------------------------------+
```

例 3: `NULL` を入力し、`NULL` が返されます。

```Plain
MySQL > select array_to_bitmap(NULL);
+-----------------------+
| array_to_bitmap(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```
