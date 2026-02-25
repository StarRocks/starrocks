---
displayed_sidebar: docs
---

# lookup_string

`lookup_string(table_name, lookup_key, return_column)`

この関数は、プライマリキーテーブルから値をルックアップし、オプティマイザで評価します。

## 引数

`table_name`: ルックアップするテーブルの名前。プライマリキーテーブルである必要があります (VARCHAR)。
`lookup_key`: ルックアップするキー。文字列型である必要があります (VARCHAR)。
`return_column`: 返す列の名前 (VARCHAR)。

## 戻り値

ルックアップされた値を含む VARCHAR 文字列を返します。見つからない場合は `NULL` を返します。

## 例

例1: `t2` テーブルの主キー値が `1` に等しい `event_day` 列の値を返す:
```
mysql> select lookup_string('t2', '1', 'event_day');
+---------------------------------------+
| lookup_string('t2', '1', 'event_day') |
+---------------------------------------+
| 2020-01-14                            |
+---------------------------------------+
1 row in set (0.02 sec)

```

