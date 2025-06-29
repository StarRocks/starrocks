---
displayed_sidebar: docs
---

# SHOW ALTER TABLE

## 説明

進行中の ALTER TABLE 操作の実行を表示します。これには以下が含まれます。

- カラムの変更。
- テーブルスキーマの最適化 (v3.2 から)、バケット方式やバケット数の変更を含む。
- ロールアップインデックスの作成と削除。

## 構文

- カラムの変更やテーブルスキーマの最適化操作の実行を表示します。

    ```sql
    SHOW ALTER TABLE { COLUMN | OPTIMIZE } [FROM db_name] [WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
    ```

- ロールアップインデックスの追加または削除操作の実行を表示します。

    ```sql
    SHOW ALTER TABLE ROLLUP [FROM db_name]
    ```

## パラメータ

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`:

  - `COLUMN` が指定された場合、このステートメントはカラムの変更操作を表示します。
  - `OPTIMIZE` が指定された場合、このステートメントはテーブル構造の最適化操作を表示します。
  - `ROLLUP` が指定された場合、このステートメントはロールアップインデックスの追加または削除操作を表示します。

- `db_name`: 任意。`db_name` が指定されていない場合、デフォルトで現在のデータベースが使用されます。

## 例

1. 現在のデータベースでのカラムの変更、テーブルスキーマの最適化、およびロールアップインデックスの作成または削除操作の実行を表示します。

    ```sql
    SHOW ALTER TABLE COLUMN;
    SHOW ALTER TABLE OPTIMIZE;
    SHOW ALTER TABLE ROLLUP;
    ```

2. 指定されたデータベースでのカラムの変更、テーブルスキーマの最適化、およびロールアップインデックスの作成または削除操作の実行を表示します。

    ```sql
    SHOW ALTER TABLE COLUMN FROM example_db;
    SHOW ALTER TABLE OPTIMIZE FROM example_db;
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ```

3. 指定されたテーブルでのカラムの変更またはテーブルスキーマの最適化の最新の操作の実行を表示します。

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
    SHOW ALTER TABLE OPTIMIZE WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1; 
    ```

## 参照

- [CREATE TABLE](CREATE_TABLE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)