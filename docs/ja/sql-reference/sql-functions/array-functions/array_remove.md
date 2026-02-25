---
displayed_sidebar: docs
---

# array_remove

配列から要素を削除します。

## 構文

```Haskell
array_remove(any_array, any_element)
```

## パラメータ

- `any_array`: 検索対象の配列。
- `any_element`: 配列内の要素に一致する式。

## 戻り値

指定された要素が削除された配列を返します。

## 例

```plaintext
mysql> select array_remove([1,2,3,null,3], 3);

+---------------------------------+

| array_remove([1,2,3,NULL,3], 3) |

+---------------------------------+

| [1,2,null]                      |

+---------------------------------+

1 row in set (0.01 sec)
```

## キーワード

ARRAY_REMOVE, ARRAY