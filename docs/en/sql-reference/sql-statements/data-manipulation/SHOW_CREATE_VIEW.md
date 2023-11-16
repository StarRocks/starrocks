---
displayed_sidebar: "English"
---

# SHOW CREATE VIEW

Returns the CREATE statement that was used to create a given view. The CREATE VIEW statement helps you understand how the view is defined and provides a reference for you to modify or reconstruct the view. Note that the SHOW CREATE VIEW statement requires you to have the `SELECT_PRIV` privilege on the view and the table based on which the view is created.

From v2.5.4 onwards, you can use SHOW CREATE VIEW to query the statement that is used to create a **materialized view**.

## Syntax

```SQL
SHOW CREATE VIEW [db_name.]view_name
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | No           | The database name. If this parameter is not specified, the CREATE VIEW statement of a given view in your current database is returned by default. |
| view_name     | Yes          | The view name.                                               |

## Output

```SQL
+---------+--------------+----------------------+----------------------+
| View    | Create View  | character_set_client | collation_connection |
+---------+--------------+----------------------+----------------------+
```

The following table describes the parameters returned by this statement.

| **Parameter**        | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| View                 | The view name.                                               |
| Create View          | The CREATE VIEW statement of the view.                       |
| character_set_client | The character set the client uses to send statements to StarRocks. |
| collation_connection | The rules for comparing characters in a character set.       |

## Examples

Create a table named `example_table`.

```SQL
CREATE TABLE example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1) BUCKETS 10;
```

Create a view named `example_view` based on `example_table`.

```SQL
CREATE VIEW example_view (k1, k2, k3, v1)
AS SELECT k1, k2, k3, v1 FROM example_table;
```

Display the CREATE VIEW statement of `example_view`.

```Plain
SHOW CREATE VIEW example_db.example_view;

+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
| View         | Create View                                                                                                                                                                                                                                                                                                                     | character_set_client | collation_connection |
+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
| example_view | CREATE VIEW `example_view` (k1, k2, k3, v1) COMMENT "VIEW" AS SELECT `default_cluster:db1`.`example_table`.`k1` AS `k1`, `default_cluster:db1`.`example_table`.`k2` AS `k2`, `default_cluster:db1`.`example_table`.`k3` AS `k3`, `default_cluster:db1`.`example_table`.`v1` AS `v1` FROM `default_cluster:db1`.`example_table`; | utf8                 | utf8_general_ci      |
+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
```
