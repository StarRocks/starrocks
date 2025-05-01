---
displayed_sidebar: docs
---

# SHOW ALTER TABLE

## 説明

進行中の ALTER TABLE タスクの実行状況を表示します。

## 構文

```sql
SHOW ALTER TABLE {COLUMN | ROLLUP} [FROM <db_name>]
```

## パラメータ

- COLUMN | ROLLUP

  - COLUMN が指定された場合、このステートメントはカラムを変更するタスクを表示します。WHERE 句をネストする必要がある場合、サポートされている構文は `[WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]` です。

  - ROLLUP が指定された場合、このステートメントは ROLLUP インデックスを作成または削除するタスクを表示します。

- `db_name`: 任意。`db_name` が指定されていない場合、デフォルトで現在のデータベースが使用されます。

## 例

例 1: 現在のデータベースでのカラム変更タスクを表示します。

```sql
SHOW ALTER TABLE COLUMN;
```

例 2: テーブルの最新のカラム変更タスクを表示します。

```sql
SHOW ALTER TABLE COLUMN WHERE TableName = "table1"
ORDER BY CreateTime DESC LIMIT 1;
 ```

例 3: 指定されたデータベースでの ROLLUP インデックスの作成または削除タスクを表示します。

```sql
SHOW ALTER TABLE ROLLUP FROM example_db;
```

## 参照

- [CREATE TABLE](../data-definition/CREATE_TABLE.md)
- [ALTER TABLE](../data-definition/ALTER_TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
- [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)