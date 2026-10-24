# Test Coverage Document

This document provides a high-level overview of the test coverage for the StarRocks Python Client, organized by feature area.

All tests run under SQLAlchemy's testing plugin against a live StarRocks cluster, together with SQLAlchemy's dialect compliance suite (`test/test_suite.py`) and Alembic's dialect suite (`test/test_suite_alembic.py`). `test/test_files_clause.py` covers the `FILES()` clause compilation.

## 1. DDL Compilation (`test/sql/`)

This suite verifies that Python schema definitions are correctly compiled into StarRocks-compliant DDL statements.

- **`test_compiler_table.py`**: Covers `CREATE TABLE` statements, including `ENGINE`, `KEY`, `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, and `PROPERTIES`.
- **`test_compiler_alter_table.py`**: Covers `ALTER TABLE` statements for properties, distribution, and order by clauses.
- **`test_compiler_alter_column.py`**: Covers `ALTER TABLE` statements for column modifications.
- **`test_compiler_view.py`**: Covers `CREATE`, `ALTER`, and `DROP VIEW` statements.
- **`test_compiler_mv.py`**: Covers `CREATE`, `ALTER`, and `DROP MATERIALIZED VIEW` statements.
- **`test_compiler_type.py`**: Covers data type compilation.

## 2. Database Reflection (`test/integration/test_reflection_*.py`)

These integration tests ensure that the dialect can accurately inspect a live StarRocks database and reflect its schema into SQLAlchemy objects.

- **`test_reflection_table.py`**: Verifies reflection of all table-level options (`ENGINE`, `KEY`, `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, `PROPERTIES`, `COMMENT`).
- **`test_reflection_columns.py`**: Verifies reflection of column attributes like `type`, `nullable`, `default`, and `comment`.
- **`test_reflection_data_types.py`**: Verifies reflection of various data types.
- **`test_reflection_view.py`**: Verifies reflection of views.
- **`test_reflection_mv.py`**: Verifies reflection of materialized views.
Other integration tests (`test/integration/`) cover reading ARRAY, MAP and STRUCT values (`test_result_processor.py`), the run mode (`test_run_mode.py`), the wait for schema changes (`test_schema_change_wait.py`), column-change coalescing (`test_combine_column_alters*.py`), and the testing plugin's hooks that create and drop each worker's databases (`test_provision.py`).

## 3. Alembic `autogenerate` Comparison (`test/integration/test_autogenerate_*.py`)

This suite of integration tests verifies that Alembic's `autogenerate` feature correctly detects differences between database state and metadata definitions, generating the appropriate migration operations.

- **`test_autogenerate_alter_table.py`**: Tests detection of changes for all major table attributes (`ENGINE`, `KEY`, `PARTITION BY`, etc.). It covers state transitions like `None` to a value, value to `None`, and value to a different value.
- **`test_autogenerate_columns.py`**: Tests detection of changes for column properties (`type`, `nullable`, `default`, `comment`, `agg_type`).
- **`test_autogenerate_views.py`**: Covers `CREATE`, `DROP`, and `ALTER` operations for Views.
- **`test_autogenerate_mvs.py`**: Covers `CREATE`, `DROP`, and `ALTER` operations for Materialized Views.

## 4. Unit Tests (`test/unit/`)

This suite of unit tests verifies individual components and functions in isolation.

### 4.1 Alembic Operation Rendering (`test_render*.py`)

Verifies that Alembic operation objects (`Op`) are correctly rendered into Python code for the migration scripts.

- **`test_render.py`**: Tests rendering of table-related operations (Alter Properties, Distribution, Order By).
- **`test_render_views.py`**: Tests rendering of view-related operations (Create, Drop, Alter).
- **Operations Covered**: `View` (Create, Drop, Alter), `Materialized View` (Create, Drop), and `Table` (Alter Properties, Distribution, Order By).
- **Boundary Cases**:
  - Optional arguments being `None` (e.g., `schema=None`).
  - Empty collections (e.g., an empty `properties` dictionary).
  - Correct string escaping for values containing special characters (e.g., quotes, backticks).

### 4.2 Comparison Logic (`test_compare_*.py`)

Tests the comparison logic that detects differences between database state and metadata.

- **`test_compare_tables.py`**: Tests table-level comparison logic.
- **`test_compare_columns.py`**: Tests column-level comparison logic.
- **`test_compare_column_type.py`**: Tests column type comparison logic, including complex types.
- **`test_compare_views.py`**: Tests view comparison logic.
- **`test_compare_mvs.py`**: Tests materialized view comparison logic.

### 4.3 Other Unit Tests

- **`test_alter_table_operations.py`**: Tests ALTER TABLE operation objects.
- **`test_defaults_and_regex.py`**: Tests default value handling and regex patterns.
- **`test_parser.py`**: Tests SQL parsing utilities.
- **`test_reflection_defaults.py`**: Tests default value reflection logic.
- **`test_sql_normalization.py`**: Tests SQL normalization utilities.
- **`test_view_schema.py`**: Tests view schema handling.
- **`test_requirements.py`**: Tests that every suite requirement the dialect opens or closes gives its reason in a docstring.

## 5. End-to-End System Tests (`test/system/`)

This is the most comprehensive test suite that simulates the complete Alembic lifecycle from schema definition to migration execution. These tests verify the entire workflow that users would experience in production.

The tests share `test/system/lifecycle.py`, which gives each test a database of its own and an Alembic environment in Alembic's staging directory, and drives Alembic's commands against it.

### 5.1 Table Lifecycle (`test_table_lifecycle.py`)

Tests the complete lifecycle of table operations through Alembic:

- **`test_create_table_simple`**: Tests creating a simple table with autogenerate, verifying the generated script, and applying it to the database.
- **`test_idempotency_comprehensive`**: Tests that a second autogenerate run produces no changes for a complex table with all features (various data types, keys, partitions, distribution, order by, properties).
- **`test_alter_table_columns_comprehensive`**: Tests adding, dropping and altering columns in one revision, and its downgrade.
- **`test_a_type_change_starrocks_cannot_undo_fails_its_downgrade`**: Tests that widening INT to BIGINT applies, and that its downgrade, which StarRocks refuses, fails and changes nothing.
- **`test_add_agg_table_key_column`**: Tests adding key columns to AGGREGATE KEY tables, including waiting for long-running ALTER operations.
- **`test_add_agg_table_value_column`**: Tests adding value columns with aggregation types (SUM, REPLACE) to AGGREGATE KEY tables.
- **`test_add_agg_table_key_and_value_columns`**: Tests adding a key column and a value column to an AGGREGATE KEY table in one revision.
- **`test_add_table_column_only`**: Tests adding multiple columns to PRIMARY KEY tables.
- **`test_drop_table_column_only`**: Tests dropping multiple columns.
- **`test_alter_table_column_only`**: Tests altering column type, nullability, and comment.
- **`test_alter_table_attributes_distribution`**: Tests altering table distribution with bucket changes.
- **`test_alter_table_order_by`**: Tests altering table ORDER BY clause.
- **`test_alter_table_properties_and_comment`**: Tests altering the properties (`storage_medium`, `replicated_storage`) and comment of a PRIMARY KEY table.
- **`test_alter_table_bucket_size_with_properties_and_comment`**: Tests altering `bucket_size` with other properties and the comment, on a RANDOM-distributed DUPLICATE KEY table.
- **`test_drop_table`**: Tests dropping tables and verifying downgrade restores them.
- **`test_drop_duplicate_key_table`**: Tests dropping DUPLICATE KEY tables.
- **`test_drop_aggregate_key_table`**: Tests dropping AGGREGATE KEY tables.
- **`test_drop_unique_key_table`**: Tests dropping UNIQUE KEY tables.

Each test follows the complete workflow:

1. Define schema in Python models
2. Run `autogenerate` to create migration script
3. Verify the generated script content
4. Run `upgrade` to apply migration
5. Verify database schema matches expectations
6. Run `downgrade` to revert changes
7. Verify schema is correctly restored

### 5.2 View Lifecycle (`test_view_lifecycle.py`)

Tests the complete lifecycle of view operations:

- **`test_create_view_basic`**: Tests creating a simple view.
- **`test_create_view_with_columns`**: Tests creating a view with explicit column definitions and comments.
- **`test_view_idempotency`**: Tests that no changes are detected when the view is in sync, with column qualifiers left out of the comparison (StarRocks 3.4.1 or later).
- **`test_alter_view_comment_and_security`**: Tests that a comment and security change generates `op.alter_view`, and that the upgrade and its downgrade apply both (StarRocks 3.4.1 or later).
- **`test_alter_view_definition`**: Tests altering a view's SQL definition.
- **`test_drop_view`**: Tests dropping a view.
- **`test_columns_change`**: Tests that a change to the view's columns alone generates an `op.alter_view` that redefines the view, and that a definition change with new columns does too.
- **`test_create_view_with_schema`**: Tests creating views in specific schemas.
- **`test_multiple_views_in_one_migration`**: Tests creating, altering, and dropping multiple views in a single migration.
- **`test_custom_include_object`**: Tests using custom filters to exclude specific views (e.g., tmp\_\* patterns).
- **`test_view_with_special_characters_in_definition`**: Tests quotes, backslashes and newlines in a view definition through the script, the database and reflection (StarRocks 4.0.7 or later).

`ViewOrderLifecycleTest`, in the same file, covers revisions whose views and materialized views read what the same revision creates, changes or removes:

- **`test_removing_a_table_and_what_reads_it_downgrades`**: Tests that removing a table drops its materialized view and view first, and that the downgrade restores them.
- **`test_dropping_a_column_a_view_stops_reading_downgrades`**: Tests that a view is dropped before a column it reads and created again after, and that the downgrade restores the column and the view.
- **`test_views_over_views_and_mvs_follow_what_they_read`**: Tests that views over a materialized view and over another view are created after what they read, and survive a downgrade to base and an upgrade back.

### 5.3 Materialized View Lifecycle (`test_mv_lifecycle.py`)

Tests the lifecycle of materialized view operations:

- **`test_create_mv_basic`**: Tests that autogenerate creates a `MANUAL` refresh materialized view, that the upgrade creates it and that the downgrade drops it.
- **`test_recreate_downgrade_restores_the_mv_columns_and_data`**: Tests that a change ALTER cannot make drops and creates the materialized view, and that the downgrade recreates the database's one, with its columns in the order of its SELECT and its data.
- **`test_create_mv_comprehensive`**: Tests creating a materialized view with a comment, distribution, ORDER BY, an `ASYNC EVERY` refresh and properties.
- **`test_alter_mv_lifecycle`**: Tests that refresh and property changes generate `op.alter_materialized_view`, and applying it.
- **`test_mixed_schema_lifecycle`**: Tests a revision that adds a column, alters a view, drops a view and creates a materialized view, leaves an unchanged one alone, and its downgrade.
- **`test_multi_schema_support`**: Tests autogenerate with `include_schemas` and an `include_name` filter that leaves a second schema out.

### 5.4 Type Comparison Integration (`test_type_comparison.py`)

Tests that verify the type comparison logic actually works correctly with real database operations:

- **`test_complex_type_comparison_actually_works`**: Tests that complex nested type changes (STRUCT, MAP, ARRAY) are properly detected.
- **`test_array_item_type_changes_detected`**: Tests that changes in ARRAY item types are detected.
- **`test_map_key_value_type_changes_detected`**: Tests that changes in MAP key/value types are detected.
- **`test_struct_field_changes_detected`**: Tests that changes in STRUCT field types are detected.
- **`test_special_equivalence_rules_work_correctly`**: Tests that equivalent types (BOOLEAN↔TINYINT(1), STRING↔VARCHAR(65533)) don't generate unnecessary migrations.

These tests are crucial because they verify that the comparison logic isn't just returning "no differences" for complex types due to incomplete implementation.

## 6. Missing Coverage and Future Work

The following features and scenarios are not yet fully covered by automated tests:

### 6.1 Materialized View Extended Features

The basic MV lifecycle tests are currently skipped. The following MV features need comprehensive testing:

- **Extended CREATE clauses**:
  - `PARTITION BY`
  - `DISTRIBUTED BY`
  - `ORDER BY`
  - `REFRESH ASYNC/MANUAL/INCREMENTAL`
- **ALTER operations**:
  - `RENAME TO`
  - `SET REFRESH ...`
  - `SET PROPERTIES (...)`
  - `SET ACTIVE / INACTIVE`
- **Reflection**: Extended MV properties (`partition`, `distribution`, `order by`, `refresh_type`, `status`).
- **Autogenerate**: Detection of changes in all the properties listed above.

### 6.2 Bitmap Index Lifecycle

- `CREATE INDEX ... USING BITMAP` compilation and execution.
- `DROP INDEX` compilation and execution.
- Reflection of Bitmap indexes via `inspector.get_indexes()`.
- `autogenerate` detection for the creation and deletion of Bitmap indexes.

### 6.3 `AUTO_INCREMENT` Property

- Full reflection of the `AUTO_INCREMENT` property on columns. Current tests are marked as `xfail`.
- `autogenerate` comparison to produce an `ALTER` operation for adding/removing `AUTO_INCREMENT` (currently only a warning is logged).

### 6.4 Complex Mixed-Operation Scenarios

While individual operations are well-tested, the following complex scenarios need coverage:

- Migration scripts that combine multiple, different types of operations in a single revision (e.g., adding a table, altering a column, dropping a view, and modifying MV properties all in one migration).
- Cross-object dependencies in migrations (e.g., dropping a table that a view depends on).
- Schema-qualified objects across multiple schemas in a single migration.

### 6.5 Performance and Scale Testing

- Large-scale migrations with many objects (e.g., 100+ tables).
- Migration performance benchmarks.
- Handling of very large table definitions with many columns and complex types.

## 7. Test Organization Summary

The test suite is organized in a pyramid structure, from unit tests to end-to-end system tests:

```text
                    ┌─────────────────────────┐
                    │   System Tests (E2E)    │  ← Full Alembic lifecycle
                    │   test/system/          │     with real database
                    └─────────────────────────┘
                  ┌───────────────────────────────┐
                  │   Integration Tests           │  ← Database operations
                  │   test/integration/           │     (reflection, autogenerate)
                  └───────────────────────────────┘
              ┌─────────────────────────────────────────┐
              │   Unit Tests                            │  ← Individual components
              │   test/unit/ & test/sql/                │     (comparison, rendering)
              └─────────────────────────────────────────┘
```

**Key Testing Principles:**

1. **Unit Tests** (`test/unit/`, `test/sql/`): Fast, isolated tests for individual functions and components.
2. **Integration Tests** (`test/integration/`): Test interactions with a real StarRocks database (reflection, autogenerate comparison).
3. **System Tests** (`test/system/`): End-to-end tests simulating the complete user workflow from schema definition through migration execution and rollback.

The **system tests** are particularly important as they validate the entire Alembic lifecycle that users will experience in production, ensuring that:

- Generated migration scripts are syntactically correct
- Migrations can be applied successfully to the database
- Database state matches expectations after migration
- Downgrade operations correctly restore previous state
- Multiple migration cycles work correctly
