---
displayed_sidebar: docs
description: "ALTER VIEW modifies the definition of a logical view."
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

```sql
ALTER VIEW [db_name.]view_name RENAME new_view_name
```

注意:

1. ビューは論理的なものであり、データは物理的な媒体に保存されません。クエリされたとき、ビューはステートメント内のサブクエリとして使用されます。したがって、ビューの定義を変更することは、query_stmt を変更することと同等です。
2. query_stmt は任意の SQL をサポートします。
3. RENAME はビューの名前のみを変更します。そのビューに付与された権限、コメント、および SQL SECURITY 特性は保持されます。

:::warning
ビューは名前で参照されます。ビューの名前を変更すると、古い名前を参照したままの他のビュー、マテリアライズドビュー、アプリケーションの SQL は、クエリ時に「テーブルが存在しない」エラーで失敗します。名前の変更時に警告は出力されません。名前を変更したビュー上に作成されたマテリアライズドビューは inactive になり、ビューの名前を元に戻すか、マテリアライズドビューを再作成するまで再度アクティブにできません。
:::

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

`example_db` の `example_view` の名前を `example_view_new_name` に変更します。

```sql
ALTER VIEW example_db.example_view RENAME example_view_new_name
```
