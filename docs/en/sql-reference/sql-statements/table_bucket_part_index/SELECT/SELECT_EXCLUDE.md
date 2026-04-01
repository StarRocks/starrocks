---
displayed_sidebar: docs
sidebar_label: "EXCLUDE"
---

# EXCLUDE

This feature is supported starting from version 4.0.  

The EXCLUDE keyword is used to exclude specified columns from query results, simplifying SQL statements when certain columns can be ignored. It is particularly convenient when working with tables containing a large number of columns, avoiding the need to explicitly list all columns to be retained.

## Syntax

```sql  
SELECT  
  * EXCLUDE (<column_name> [, <column_name> ...])  
  | <table_alias>.* EXCLUDE (<column_name> [, <column_name> ...])  
FROM ...  
```  

## Parameters

- **`* EXCLUDE`**  
  Selects all columns with the wildcard `*`, followed by `EXCLUDE` and a list of column names to exclude.  
- **`<table_alias>.* EXCLUDE`**  
  When a table alias exists, this allows excluding specific columns from that table (must be used with the alias).  
- **`<column_name>`**  
  The column name(s) to exclude. Multiple columns are separated by commas. Columns must exist in the table; otherwise, an error will be returned.  

## Examples

- Basic Usage:

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
