---
displayed_sidebar: docs
---

# array_agg

## 説明

`NULL` を含む列の値を配列に集約します。

## 構文

```Haskell
ARRAY_AGG(col)
```

## パラメータ

`col`: 集約したい列の値。サポートされているデータ型は BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、CHAR、DATETIME、および DATE です。

## 戻り値

ARRAY データ型の値を返します。

## 使用上の注意

- 配列内の要素の順序はランダムであり、列内の値の順序とは異なる場合があります。
- 返される配列内の要素のデータ型は、列内の値のデータ型と同じです。

## 例

以下のデータテーブルを例として考えます。

```plaintext
mysql> select * from test;

+------+------+

| c1   | c2   |

+------+------+

|    1 | a    |

|    1 | b    |

|    2 | c    |

|    2 | NULL |

|    3 | NULL |

+------+------+
```

例 1: 列 c1 でグループ化し、列 c2 の値を列 c1 のグループ化に基づいて配列に集約します。

```plaintext
mysql> select c1, array_agg(c2) from test group by c1;

+------+-----------------+

| c1   | array_agg(`c2`) |

+------+-----------------+

|    1 | ["a","b"]       |

|    2 | [null,"c"]      |

|    3 | [null]          |

+------+-----------------+
```

例 2: 列 c2 の値を配列に集約する際に WHERE 句を使用します。WHERE 句で指定された条件を満たす列 c2 のデータがない場合、`NULL` 値が返されます。

```plaintext
mysql> select array_agg(c2) from test where c1>4;

+-----------------+

| array_agg(`c2`) |

+-----------------+

| NULL            |

+-----------------+
```

## キーワード

ARRAY_AGG, ARRAY