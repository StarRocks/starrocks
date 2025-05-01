---
displayed_sidebar: docs
---

# CANCEL ALTER TABLE

## 説明

指定されたテーブルに対して ALTER TABLE ステートメントで実行された次の操作をキャンセルします。

- テーブルスキーマ: 列の追加と削除、列の並べ替え、列のデータ型の変更。
- ロールアップインデックス: ロールアップインデックスの作成と削除。

このステートメントは同期操作であり、テーブルに対する `ALTER_PRIV` 権限が必要です。

## 構文

- スキーマ変更をキャンセルします。

    ```SQL
    CANCEL ALTER TABLE COLUMN FROM [db_name.]table_name
    ```

- ロールアップインデックスの変更をキャンセルします。

    ```SQL
    CANCEL ALTER TABLE ROLLUP FROM [db_name.]table_name
    ```

## パラメータ

| **パラメータ** | **必須** | **説明** |
| ------------- | -------- | -------- |
| db_name       | いいえ   | テーブルが属するデータベースの名前。このパラメータが指定されていない場合、現在のデータベースがデフォルトで使用されます。 |
| table_name    | はい     | テーブル名。 |

## 例

例 1: `example_db` データベース内の `example_table` に対するスキーマ変更をキャンセルします。

```SQL
CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
```

例 2: 現在のデータベース内の `example_table` に対するロールアップインデックスの変更をキャンセルします。

```SQL
CANCEL ALTER TABLE ROLLUP FROM example_table;
```