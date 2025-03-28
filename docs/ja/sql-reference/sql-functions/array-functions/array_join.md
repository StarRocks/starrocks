---
displayed_sidebar: docs
---

# array_join

配列の要素を連結して文字列にします。

## Syntax

```Haskell
array_join(array, sep[, null_replace_str])
```

## Parameters

- `array`: 連結したい配列の要素。ARRAY データ型のみサポートされています。

- `sep`: 連結された配列要素を区切るために使用されるデリミタ。VARCHAR データ型のみサポートされています。

- `null_replace_str`: `NULL` 値を置き換えるために使用される文字列。VARCHAR データ型のみサポートされています。

## Return value

VARCHAR データ型の値を返します。

## Usage notes

- `array` パラメータの値は一次元配列でなければなりません。

- `array` パラメータは DECIMAL 値をサポートしていません。

- `sep` パラメータを `NULL` に設定すると、戻り値は `NULL` になります。

- `null_replace_str` パラメータを指定しない場合、`NULL` 値は無視されます。

- `null_replace_str` パラメータを `NULL` に設定すると、戻り値は `NULL` になります。

## Examples

例 1: 配列の要素を連結します。この例では、配列内の `NULL` 値は無視され、連結された配列要素はアンダースコア (`_`) で区切られます。

```plaintext
mysql> select array_join([1, 3, 5, null], '_');

+-------------------------------+

| array_join([1,3,5,NULL], '_') |

+-------------------------------+

| 1_3_5                         |

+-------------------------------+
```

例 2: 配列の要素を連結します。この例では、配列内の `NULL` 値は `NULL` 文字列で置き換えられ、連結された配列要素はアンダースコア (`_`) で区切られます。

```plaintext
mysql> select array_join([1, 3, 5, null], '_', 'NULL');

+---------------------------------------+

| array_join([1,3,5,NULL], '_', 'NULL') |

+---------------------------------------+

| 1_3_5_NULL                            |

+---------------------------------------+
```