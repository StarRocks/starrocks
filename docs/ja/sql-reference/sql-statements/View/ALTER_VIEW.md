---
displayed_sidebar: docs
---

# ALTER VIEW

## 説明

ビューの定義を変更します。

## 構文

```sql
ALTER VIEW
[db_name.]view_name
(column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt
```

注意:

1. ビューは論理的なものであり、データは物理的な媒体に保存されません。クエリされたとき、ビューはステートメント内のサブクエリとして使用されます。したがって、ビューの定義を変更することは、query_stmt を変更することと同等です。
2. query_stmt は任意の SQL をサポートします。

## 例

`example_db` の `example_view` を変更します。

```sql
ALTER VIEW example_db.example_view
(
c1 COMMENT "column 1",
c2 COMMENT "column 2",
c3 COMMENT "column 3"
)
AS SELECT k1, k2, SUM(v1) 
FROM example_table
GROUP BY k1, k2
```