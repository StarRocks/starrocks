---
displayed_sidebar: docs
sidebar_label: "OFFSET"
---

# OFFSET

OFFSET 句を使用すると、結果セットは最初の数行をスキップし、後続の結果を直接返します。

結果セットはデフォルトで行 0 から始まるため、OFFSET 0 と OFFSET なしは同じ結果を返します。

一般的に、OFFSET 句は ORDER BY 句および LIMIT 句と組み合わせて使用する必要があります。

## 例

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 3;

+----------------+
| varchar_column | 
+----------------+
|    beijing     | 
|    chongqing   | 
|    tianjin     | 
+----------------+

3 rows in set (0.02 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 0;

+----------------+
|varchar_column  |
+----------------+
|     beijing    |
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 1;

+----------------+
|varchar_column  |
+----------------+
|    chongqing   | 
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 2;

+----------------+
|varchar_column  |
+----------------+
|     tianjin    |     
+----------------+

1 row in set (0.02 sec)
```

## 注意事項

order by なしで offset 構文を使用することは許可されていますが、この時点では offset は意味がありません。

この場合、limit 値のみが取得され、offset 値は無視されます。したがって、order by は不要です。

Offset が結果セットの最大行数を超えても、結果は表示されます。ユーザーは order by とともに offset を使用することをお勧めします。
