---
displayed_sidebar: "English"
---

# CREATE VIEW

## Description

Creates a view.

A view, or a logical view, is a virtual table whose data is derived from a query against other existing physical tables. Therefore, a view uses no physical storage, and all queries against the view are equivalent to sub-queries of the query statement used to build the view.

For information about materialized views supported by StarRocks, see [Synchronous materialized views](../../../using_starrocks/Materialized_view-single_table.md) and [Asynchronous materialized views](../../../using_starrocks/Materialized_view.md).

> **CAUTION**
>
> Only users with the CREATE VIEW privilege on a specific database can perform this operation.

## Syntax

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

## Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| OR REPLACE      | Replace an existing view.                                    |
| database        | The name of the database where the view resides.             |
| view_name       | The name of the view.                                        |
| column_name     | The name of the column(s) in the view. Note that the columns in the view and the columns queried in the `query_statement` must agree in number. |
| COMMENT         | The comment on the column in the view or the view itself.    |
| query_statement | The query statement used to create the view. It can be any query statement supported by StarRocks. |

## Usage notes

- Querying a view requires the SELECT privilege on the view and on its corresponding base tables.
- If the query statement used to build a view cannot be executed due to the Schema Change on the base tables, StarRocks returns an error when you query the view.

## Examples

Example 1: Create a view named `example_view` in `example_db` with an aggregate query against `example_table`.

```SQL
CREATE VIEW example_db.example_view (k1, k2, k3, v1)
AS
SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
WHERE k1 = 20160112 GROUP BY k1,k2,k3;
```

Example 2: Create a view named `example_view` in the database `example_db` with an aggregate query against the table `example_table`, and specify comments for the view and each column in it.

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

## Relevant SQLs

- [SHOW CREATE VIEW](../data-manipulation/SHOW_CREATE_VIEW.md)
- [ALTER VIEW](./ALTER_VIEW.md)
- [DROP VIEW](./DROP_VIEW.md)
