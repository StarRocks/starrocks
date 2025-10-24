# Test Coverage Document

This document provides a high-level overview of the test coverage for the StarRocks Python Client, organized by feature area.

## 1. DDL Compilation (`test/sql/`)

This suite verifies that Python schema definitions are correctly compiled into StarRocks-compliant DDL statements.

- **`test_compiler_table.py`**: Covers `CREATE TABLE` statements, including `ENGINE`, `KEY`, `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, and `PROPERTIES`.
- **`test_compiler_alter_table.py`**: Covers `ALTER TABLE` statements for properties, distribution, and order by clauses.
- **`test_compiler_view.py`**: Covers `CREATE`, `ALTER`, and `DROP VIEW` statements.
- **`test_compiler_mv.py`**: Covers `CREATE`, `ALTER`, and `DROP MATERIALIZED VIEW` statements.

## 2. Database Reflection (`test/integration/test_reflection_*.py`)

These integration tests ensure that the dialect can accurately inspect a live StarRocks database and reflect its schema into SQLAlchemy objects.

- **`test_reflection_tables.py`**: Verifies reflection of all table-level options (`ENGINE`, `KEY`, `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, `PROPERTIES`, `COMMENT`).
- **`test_reflection_columns.py`**: Verifies reflection of column attributes like `type`, `nullable`, `default`, and `comment`.
- **`test_reflection_agg.py`**: Focuses on reflecting StarRocks-specific column aggregation types (`SUM`, `MIN`, `MAX`, `REPLACE`, etc.).

## 3. Alembic `autogenerate` Comparison (`test_autogenerate_*.py`)

This is the core suite of integration tests, verifying that Alembic's `autogenerate` feature correctly detects differences between database state and metadata definitions, generating the appropriate migration operations.

- **`test_autogenerate_tables.py`**: Tests detection of changes for all major table attributes (`ENGINE`, `KEY`, `PARTITION BY`, etc.). It covers state transitions like `None` to a value, value to `None`, and value to a different value.
- **`test_autogenerate_columns.py`**: Tests detection of changes for column properties (`type`, `nullable`, `default`, `comment`, `agg_type`).
- **`test_autogenerate_views.py`**: Covers `CREATE`, `DROP`, and `ALTER` operations for Views.
- **`test_autogenerate_mvs.py`**: Covers `CREATE`, `DROP`, and `ALTER` operations for Materialized Views.

## 4. Alembic `autogenerate` Rendering (`test/test_render.py`)

This suite of unit tests verifies that Alembic operation objects (`Op`) are correctly ren
ered into Python code for the migration scripts.

- **Operations Covered**: `View` (Create, Drop, Alter), `Materialized View` (Create, Drop), and `Table` (Alter Properties, Distribution, Order By).
- **Boundary Cases**:
  - Optional arguments being `None` (e.g., `schema=None`).
  - Empty collections (e.g., an empty `properties` dictionary).
  - Correct string escaping for values containing special characters (e.g., quotes, backticks).

## 5. Missing Coverage and Future Work

The following features and scenarios are not yet covered by automated tests:

- **Materialized View Lifecycle**:

  - `CREATE MATERIALIZED VIEW` compilation and execution with extended clauses:
    - `PARTITION BY`
    - `DISTRIBUTED BY`
    - `ORDER BY`
    - `REFRESH ASYNC/MANUAL/INCREMENTAL`
  - `ALTER MATERIALIZED VIEW` compilation and execution for:
    - `RENAME TO`
    - `SET REFRESH ...`
    - `SET PROPERTIES (...)`
    - `SET ACTIVE / INACTIVE`
  - Reflection of all extended MV properties (`partition`, `distribution`, `order by`, `refresh_type`, `status`).
  - `autogenerate` detection for changes in all the properties listed above.

- **Bitmap Index Lifecycle**:

  - `CREATE INDEX ... USING BITMAP` compilation and execution.
  - `DROP INDEX` compilation and execution.
  - Reflection of Bitmap indexes via `inspector.get_indexes()`.
  - `autogenerate` detection for the creation and deletion of Bitmap indexes.

- **`AUTO_INCREMENT` Property**:

  - Full reflection of the `AUTO_INCREMENT` property on columns. Current tests are marked as `xfail`.
  - `autogenerate` comparison to produce an `ALTER` operation for adding/removing `AUTO_INCREMENT` (currently only a warning is logged).

- **Complex Scenario Tests**:

  - `ALTER` operations on columns with complex data types (e.g., `ARRAY`, `JSON`).
  - Migration scripts that combine multiple, different types of operations (e.g., adding a table, altering a column, and dropping a view in a single revision).

- **End-to-End Alembic Lifecycle Test**:
  - A comprehensive test case simulating the full user workflow:
    1. Define a schema in Python models (including tables, views, etc.).
    2. Run `autogenerate` to create a migration script.
    3. Run `upgrade` to apply the migration to the database.
    4. Verify the database schema correctly matches the models.
    5. Modify the Python models (e.g., alter a table property).
    6. Run `autogenerate` again to create a second migration script.
    7. Run `upgrade` to apply the changes.
    8. Verify the changes are reflected in the database.
    9. Run `downgrade` to revert the changes and verify the schema is restored.
    10. Run `downgrade` back to base to verify all objects are dropped.
