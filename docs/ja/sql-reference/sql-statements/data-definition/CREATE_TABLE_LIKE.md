---
displayed_sidebar: docs
---

# CREATE TABLE LIKE

## 説明

別のテーブルの定義に基づいて、同一の空のテーブルを作成します。定義には、カラム定義、パーティション、およびテーブルプロパティが含まれます。

## 構文

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name
```

> **注意**

1. 元のテーブルに対する `SELECT` 権限を持っている必要があります。
2. MySQL のような 外部テーブル をコピーすることができます。

## 例

1. test1 データベースの下で、table1 と同じテーブル構造を持つ空のテーブルを作成し、table2 と名付けます。

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1
    ```

2. test2 データベースの下で、test1.table1 と同じテーブル構造を持つ空のテーブルを作成し、table2 と名付けます。

    ```sql
    CREATE TABLE test2.table2 LIKE test1.table1
    ```

3. test1 データベースの下で、MySQL 外部テーブル と同じテーブル構造を持つ空のテーブルを作成し、table2 と名付けます。

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1
    ```