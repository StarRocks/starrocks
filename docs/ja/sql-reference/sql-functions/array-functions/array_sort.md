---
displayed_sidebar: docs
---

# array_sort

## Description

配列の要素を昇順にソートします。

## Syntax

```Haskell
array_sort(array)
```

## Parameters

`array`: ソートしたい配列。ARRAY データ型のみサポートされています。

## Return value

配列を返します。

## Usage notes

- この関数は配列の要素を昇順にのみソートします。

- `NULL` 値は返される配列の先頭に配置されます。

- 配列の要素を降順にソートしたい場合は、 [reverse](./reverse.md) 関数を使用してください。

- 返される配列の要素は、入力配列の要素と同じデータ型です。

## Examples

以下のテーブルを例として使用します。

```plaintext
mysql> select * from test;

+------+--------------+

| c1   | c2           |

+------+--------------+

|    1 | [4,3,null,1] |

|    2 | NULL         |

|    3 | [null]       |

|    4 | [8,5,1,4]    |

+------+--------------+
```

列 `c2` の値を昇順にソートします。

```plaintext
mysql> select c1, array_sort(c2) from test;

+------+------------------+

| c1   | array_sort(`c2`) |

+------+------------------+

|    1 | [null,1,3,4]     |

|    2 | NULL             |

|    3 | [null]           |

|    4 | [1,4,5,8]        |

+------+------------------+
```