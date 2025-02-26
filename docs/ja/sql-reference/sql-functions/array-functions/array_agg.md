---
displayed_sidebar: docs
---

# array_agg

列内の値（`NULL` を含む）を配列に集約し（複数行を1行に）、特定の列で要素を並べ替えることができます。バージョン 3.0 から、array_agg() は ORDER BY を使用して要素を並べ替えることをサポートしています。

## 構文

```Haskell
ARRAY_AGG([distinct] col [order by col0 [desc | asc] [nulls first | nulls last] ...])
```

## パラメータ

- `col`: 集約したい列の値。サポートされているデータ型は BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、CHAR、DATETIME、DATE、ARRAY（v3.1以降）、MAP（v3.1以降）、STRUCT（v3.1以降）です。

- `col0`: `col` の順序を決定する列。ORDER BY 列は複数指定することができます。

- `[desc | asc]`: `col0` の昇順（デフォルト）または降順で要素を並べ替えるかを指定します。

- `[nulls first | nulls last]`: NULL 値を最初または最後に配置するかを指定します。

## 戻り値

ARRAY 型の値を返し、`col0` によってオプションで並べ替えられます。

## 使用上の注意

- 配列内の要素の順序はランダムであり、ORDER BY 列が指定されていない場合や、並べ替えられていない場合、列内の値の順序と異なる可能性があります。
- 返される配列内の要素のデータ型は、列内の値のデータ型と同じです。
- 入力が空でグループ化列がない場合、`NULL` を返します。
- ARRAY_AGG_DISTINCT() は ARRAY_AGG(DISTINCT) のエイリアスです。

## 例

次のデータテーブルを例にとります：

```plaintext
mysql> select * from t;
+------+------+------+
| a    | name | pv   |
+------+------+------+
|   11 |      |   33 |
|    2 | NULL |  334 |
|    1 | fzh  |    3 |
|    1 | fff  |    4 |
|    1 | fff  |    5 |
+------+------+------+
```

例 1: 列 `a` の値をグループ化し、列 `pv` の値を `name` で並べ替えて配列に集約します。

```plaintext
mysql> select a, array_agg(pv order by name nulls first) from t group by a;
+------+---------------------------------+
| a    | array_agg(pv ORDER BY name ASC) |
+------+---------------------------------+
|    2 | [334]                           |
|   11 | [33]                            |
|    1 | [4,5,3]                         |
+------+---------------------------------+

-- 順序なしで値を集約します。
mysql> select a, array_agg(pv) from t group by a;
+------+---------------+
| a    | array_agg(pv) |
+------+---------------+
|   11 | [33]          |
|    2 | [334]         |
|    1 | [3,4,5]       |
+------+---------------+
3 rows in set (0.03 sec)
```

例 2: 列 `pv` の値を `name` で並べ替えて配列に集約します。

```plaintext
mysql> select array_agg(pv order by name desc nulls last) from t;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| [3,4,5,33,334]                   |
+----------------------------------+
1 row in set (0.02 sec)

-- 順序なしで値を集約します。
mysql> select array_agg(pv) from t;
+----------------+
| array_agg(pv)  |
+----------------+
| [3,4,5,33,334] |
+----------------+
1 row in set (0.03 sec)
```

例 3: WHERE 句を使用して列 `pv` の値を集約します。`pv` にフィルタ条件を満たすデータがない場合、`NULL` 値が返されます。

```plaintext
mysql> select array_agg(pv order by name desc nulls last) from t where a < 0;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| NULL                             |
+----------------------------------+
1 row in set (0.02 sec)

-- 順序なしで値を集約します。
mysql> select array_agg(pv) from t where a < 0;
+---------------+
| array_agg(pv) |
+---------------+
| NULL          |
+---------------+
1 row in set (0.03 sec)
```

## キーワード

ARRAY_AGG, ARRAY