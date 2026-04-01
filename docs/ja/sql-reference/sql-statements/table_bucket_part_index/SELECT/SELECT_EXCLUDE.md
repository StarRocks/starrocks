---
displayed_sidebar: docs
sidebar_label: "EXCLUDE"
---

# EXCLUDE

この機能はバージョン 4.0 以降でサポートされています。

EXCLUDE キーワードは、クエリ結果から指定されたカラムを除外するために使用され、特定カラムを無視できる場合に SQL ステートメントを簡素化します。これは、多数のカラムを含むテーブルを操作する際に特に便利で、保持するすべてのカラムを明示的にリストする必要がなくなります。

## 構文

```sql  
SELECT  
  * EXCLUDE (<column_name> [, <column_name> ...])  
  | <table_alias>.* EXCLUDE (<column_name> [, <column_name> ...])  
FROM ...  
```

## パラメータ

- **`* EXCLUDE`**  
  ワイルドカード `*` を使用してすべてのカラムを選択し、その後に `EXCLUDE` と除外するカラム名のリストを指定します。
- **`<table_alias>.* EXCLUDE`**  
  テーブルエイリアスが存在する場合、この構文を使用すると、そのテーブルから特定カラムを除外できます（エイリアスとともに使用する必要があります）。
- **`<column_name>`**  
  除外するカラム名。複数のカラムはカンマで区切ります。カラムはテーブルに存在する必要があります。存在しない場合、エラーが返されます。

## 例

- 基本的な使用例：

```sql  
-- Create test_table.
CREATE TABLE test_table (  
  id INT,  
  name VARCHAR(50),  
  age INT,  
  email VARCHAR(100)  
) DUPLICATE KEY(id);  

-- Exclude a single column (age).
SELECT * EXCLUDE (age) FROM test_table;  
-- Above is equivalent to:  
SELECT id, name, email FROM test_table;  

-- Exclude multiple columns (name, email).
SELECT * EXCLUDE (name, email) FROM test_table;  
-- Above is equivalent to:  
SELECT id, age FROM test_table;  

-- Exclude columns using a table alias.
SELECT test_table.* EXCLUDE (email) FROM test_table;  
-- Above is equivalent to:  
SELECT id, name, age FROM test_table;  
```
