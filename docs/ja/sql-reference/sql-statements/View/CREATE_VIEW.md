---
displayed_sidebar: docs
---

# CREATE VIEW

## 説明

ビューを作成します。

ビュー、またはビューは、他の既存の物理テーブルに対するクエリからデータが派生する仮想テーブルです。したがって、ビューは物理ストレージを使用せず、ビューに対するすべてのクエリは、ビューを構築するために使用されるクエリステートメントのサブクエリと同等です。

StarRocks がサポートするマテリアライズドビューに関する情報は、 [Synchronous materialized views](../../../using_starrocks/Materialized_view-single_table.md) および [Asynchronous materialized views](../../../using_starrocks/async_mv/Materialized_view.md) を参照してください。

> **注意**
>
> 特定のデータベースに対して CREATE VIEW 権限を持つユーザーのみがこの操作を実行できます。

## 構文

```SQL
CREATE [OR REPLACE] VIEW [IF NOT EXISTS]
[<database>.]<view_name>
(
    <column_name>[ COMMENT 'column comment']
    [, <column_name>[ COMMENT 'column comment'], ...]
)
[COMMENT 'view comment']
AS <query_statement>
```

## パラメータ

| **パラメータ**  | **説明**                                                      |
| --------------- | ------------------------------------------------------------ |
| OR REPLACE      | 既存のビューを置き換えます。                                  |
| database        | ビューが存在するデータベースの名前。                           |
| view_name       | ビューの名前。                                                |
| column_name     | ビュー内の列の名前。ビュー内の列と `query_statement` でクエリされた列の数は一致している必要があります。 |
| COMMENT         | ビュー内の列またはビュー自体に対するコメント。                |
| query_statement | ビューを作成するために使用されるクエリステートメント。StarRocks がサポートする任意のクエリステートメントを使用できます。 |

## 使用上の注意

- ビューをクエリするには、ビューおよび対応するベーステーブルに対する SELECT 権限が必要です。
- ベーステーブルに対する schema change によりビューを構築するためのクエリステートメントが実行できない場合、StarRocks はビューをクエリする際にエラーを返します。

## 例

例 1: `example_table` に対する集計クエリを使用して、`example_db` に `example_view` という名前のビューを作成します。

```SQL
CREATE VIEW example_db.example_view (k1, k2, k3, v1)
AS
SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
WHERE k1 = 20160112 GROUP BY k1,k2,k3;
```

例 2: テーブル `example_table` に対する集計クエリを使用して、データベース `example_db` に `example_view` という名前のビューを作成し、ビューおよび各列にコメントを指定します。

```SQL
CREATE VIEW example_db.example_view
(
    k1 COMMENT 'first key',
    k2 COMMENT 'second key',
    k3 COMMENT 'third key',
    v1 COMMENT 'first value'
)
COMMENT 'my first view'
AS
SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
WHERE k1 = 20160112 GROUP BY k1,k2,k3;
```

## 関連する SQL

- [SHOW CREATE VIEW](SHOW_CREATE_VIEW.md)
- [ALTER VIEW](ALTER_VIEW.md)
- [DROP VIEW](DROP_VIEW.md)