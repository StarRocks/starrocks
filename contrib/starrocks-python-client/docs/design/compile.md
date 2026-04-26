# DDL Compilation Design

This document describes the DDL (Data Definition Language) compilation process in the `starrocks-sqlalchemy` dialect. Compilation is the process of converting SQLAlchemy's abstract schema objects (like `Table`, `Column`, `View`, and `Materialized View`) into StarRocks-specific SQL strings.

## `StarRocksDDLCompiler`

The core of this process is the `starrocks.dialect.StarRocksDDLCompiler`, which inherits from SQLAlchemy's `MySQLDDLCompiler` but overrides key methods to handle StarRocks-specific syntax and features.

The `ddl_compiler` is registered with the dialect and is automatically invoked by SQLAlchemy when DDL statements need to be generated.

### Key Overridden Methods

- **`visit_create_table(self, create)`**: This method is responsible for generating the `CREATE TABLE` statement. It is extended to compile StarRocks-specific table properties from `table.dialect_options['starrocks']`.

- **`get_column_specification(self, column)`**: This method generates the DDL for a single column definition. It has been customized to handle:

  - **Aggregate Types**: For `AGGREGATE KEY` tables, it appends the aggregate function (e.g., `SUM`, `REPLACE`) to the column definition based on the attributes in `column.dialect_options['starrocks']`.
  - **StarRocks Data Types**: It ensures that StarRocks-specific data types like `BITMAP` and `HLL` are compiled correctly.

- **`visit_create_view(self, create)`**: A custom visitor that compiles a `starrocks.schema.View` object into a `CREATE VIEW` statement.

- **`visit_create_materialized_view(self, create)`**: A custom visitor that compiles a `starrocks.schema.MaterializedView` object into a `CREATE MATERIALIZED VIEW` statement.

### Unified Table-Based Compilation for View / Materialized View

In line with the unified design described in `docs/design/view_and_mv.md`, **views and materialized views are compiled through the same Table-based pipeline**:

- At compile time, the element passed to the compiler is always a `Table`-like object. The real kind (`TABLE` / `VIEW` / `MATERIALIZED_VIEW`) is determined by `table.info['table_kind']`.
- `StarRocksDDLCompiler.visit_create_table()` can inspect `table.info['table_kind']` and, when it is `VIEW` or `MATERIALIZED_VIEW`, delegate to the corresponding `CREATE VIEW` / `CREATE MATERIALIZED VIEW` helpers. This ensures that whether a View/MV was created via the `View` / `MaterializedView` convenience classes or directly as a `Table` with `info['table_kind']` set, the generated DDL is consistent.

## How it Works

1.  **SQLAlchemy Core**: When a user performs an action that requires DDL (e.g., `metadata.create_all()` or an Alembic migration), SQLAlchemy invokes the appropriate "visitor" method on the dialect's DDL compiler.
2.  **Custom Compiler**: The `StarRocksDDLCompiler` intercepts these calls.
3.  **DDL Generation**: The overridden methods generate the SQL string, paying special attention to the `dialect_options` on both `Table` and `Column` objects to construct the correct StarRocks-specific DDL.
