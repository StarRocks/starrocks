---
displayed_sidebar: docs
sidebar_label: "EXCLUDE"
---

# EXCLUDE

该功能从 4.0 版本开始支持。

`EXCLUDE` 关键字用于从查询结果中排除指定的列，从而简化 SQL 语句，尤其适用于处理包含大量列的表，避免了显式列出所有要保留的列。

## 语法

```sql  
SELECT  
  * EXCLUDE (<column_name> [, <column_name> ...])  
  | <table_alias>.* EXCLUDE (<column_name> [, <column_name> ...])  
FROM ...  
```

## 参数

- **`* EXCLUDE`**  
  选择所有列，使用通配符 `*`，后跟 `EXCLUDE` 和要排除的列名列表。
- **`<table_alias>.* EXCLUDE`**  
  当存在表别名时，允许从该表中排除特定列（必须与别名一起使用）。
- **`<column_name>`**  
  要排除的列名。多个列名用逗号分隔。列必须存在于表中；否则，将返回错误。

## 示例

- 基本用法：

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
