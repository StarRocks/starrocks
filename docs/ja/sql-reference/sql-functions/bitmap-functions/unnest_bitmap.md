---
displayed_sidebar: docs
---

# unnest_bitmap

unnest_bitmap は、ビットマップを受け取り、そのビットマップ内の要素をテーブルの複数行に変換するテーブル関数です。

StarRocks の [Lateral Join](../../../using_starrocks/Lateral_join.md) は、unnest_bitmap 関数と組み合わせて、一般的な列から行へのロジックを実装することができます。

この関数は `unnest(bitmap_to_array(bitmap))` を置き換えるために使用できます。パフォーマンスが向上し、メモリリソースの消費が少なく、配列の長さに制限されません。

この関数は v3.1 からサポートされています。

## 構文

```Haskell
unnest_bitmap(bitmap)
```

## パラメータ

`bitmap`: 変換したいビットマップ。

## 戻り値

ビットマップから変換された複数の行を返します。戻り値の型は BIGINT です。

## 例

`c2` はテーブル `t1` のビットマップ列です。

```SQL
-- bitmap_to_string 関数を使用して、c2 列の値を文字列に変換します。
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

-- unnest_bitmap 関数を使用して、ビットマップ列を複数の行に展開します。
mysql> select c1, unnest_bitmap from t1, unnest_bitmap(c2);
+------+---------------+
| c1   | unnest_bitmap |
+------+---------------+
|    1 |             1 |
|    1 |             2 |
|    1 |             3 |
|    1 |             4 |
|    1 |             5 |
|    1 |             6 |
|    1 |             7 |
|    1 |             8 |
|    1 |             9 |
|    1 |            10 |
+------+---------------+
```