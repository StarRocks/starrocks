---
displayed_sidebar: docs
---

# last_query_id

現在のセッションで最も最近実行されたクエリの ID を取得します。

## Syntax

```Haskell
VARCHAR last_query_id();
```

## Parameters

なし

## Return value

VARCHAR 型の値を返します。

## Examples

```Plain Text
mysql> select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| 7c1d8d68-bbec-11ec-af65-00163e1e238f |
+--------------------------------------+
1 row in set (0.00 sec)
```

## Keywords

LAST_QUERY_ID