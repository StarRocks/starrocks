---
displayed_sidebar: docs
---

# get_query_dump

`get_query_dump(query)`
`get_query_dump(query, enable_mock)`

これらの関数は、デバッグ目的でクエリのダンプを返します。

## 引数

`query`: SQL クエリ文字列 (VARCHAR)。
`enable_mock`: (オプション) ダンプのモックデータを有効にするかどうかを示すブール値。デフォルトは `FALSE` です。

## 戻り値

クエリダンプを含む VARCHAR 文字列を返します。

## 例

例 1: モックデータなしでシンプルなクエリのダンプを取得する
```
mysql> select get_query_dump('select * from ss limit 1');
1 row in set (0.04 sec)
```

