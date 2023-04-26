# SHOW ALTER TABLE

## Description

This statement is used to show the execution of various modification tasks currently in progress

## Syntax

```sql
SHOW ALTER TABLE {COLUMN | ROLLUP} [FROM <db_name>]
```

## Parameters

- TABLE COLUMN: Show ALTER tasks to modify columns
Supports Syntax [WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
- TABLE ROLLUP: Shows tasks for creating or deleting ROLLUP indexes
If db_is not specified Name, using the current default DB

## Examples

Example 1: Task execution showing all modified columns of default db

```sql
SHOW ALTER TABLE COLUMN;
```

Example 2: Shows task execution for a table that last modified columns

```sql
SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 
 ```

Example 3: Shows task execution for creating or deleting ROLLUP index for specified db

```sql
SHOW ALTER TABLE ROLLUP FROM example_db;
````
