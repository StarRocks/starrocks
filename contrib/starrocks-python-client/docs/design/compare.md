# Alembic Schema Comparison Design

This document outlines the design of the schema comparison (or "diffing") process used by Alembic's `autogenerate` feature, focusing on the dispatch mechanism and how the `sqlalchemy-starrocks` dialect extends it.

## Alembic's `autogenerate` Workflow

The `autogenerate` process can be broken down into these high-level steps:

1. **Database Reflection**: Alembic connects to the database and reflects its current schema state (the "connection" state).
2. **Metadata Loading**: It loads the schema defined in your application's SQLAlchemy `MetaData` objects (the "metadata" state).
3. **Schema Comparison**: Alembic iterates through the objects and compares the "connection" state to the "metadata" state. This is where the dispatch mechanism comes into play.
4. **Operation Generation**: For each difference, Alembic generates a migration operation (e.g., `AddColumnOp`).
5. **Code Rendering**: The generated operations are then rendered into a Python migration script.

## The Comparator Dispatch Mechanism

A key part of the "Schema Comparison" step is Alembic's comparator dispatch system. It is designed to be extensible, allowing individual dialects to provide their own comparison logic.

When Alembic needs to compare two objects (e.g., a table), it calls `comparators.dispatch("table", ...)`. The dispatch system then acts as a registry, invoking **all** functions that have been registered to handle the `"table"` type. This means that both Alembic's generic `compare_table` function and any dialect-specific comparators will be called.

This makes it critical for a dialect's comparator to first check if it's operating on the correct database type.

### The `comparators_dispatch_for_starrocks` Decorator

To solve this, the `sqlalchemy-starrocks` dialect uses a custom decorator, `comparators_dispatch_for_starrocks`. This is a wrapper around Alembic's standard `@comparators.dispatch_for` that adds a crucial check at the beginning of each comparator function:

```python
if autogen_context.dialect.name != "starrocks":
    return
```

This ensures that the StarRocks-specific comparison logic is only executed when `autogenerate` is running against a StarRocks database, making it safe to use in projects with multiple database backends.

### Visualizing the Dispatch Flow

Here is a flowchart illustrating the nested dispatch process:

```plain text
+-------------------------------------------+
| $ alembic revision --autogenerate         |
+-------------------------------------------+
                   |
                   v
+-------------------------------------------+
| Alembic Core: Compares DB vs. Metadata    |
+-------------------------------------------+
                   |
                   v
+-------------------------------------------+
| For each table, Alembic dispatches for    |
| `"table"` type, calling ALL registered    |
| comparators.                              |
+-------------------------------------------+
                   |
     /----------------------------------\
     |                                  |
     v                                  v
+---------------------------+      +-------------------------------------------+
| SR `compare_..._table`    |      | Alembic Generic `compare_table` is called.|
| is called.                |      | It handles generic diffs (add/drop col)   |
| (Checks dialect & handles |      | and iterates through each column.         |
| table-level SR options)   |      +-------------------------------------------+
+---------------------------+                     |
                                                  v
                                     +-----------------------------------------+
                                     | For each column, it dispatches for      |
                                     | `"column"` type, calling ALL registered |
                                     | column comparators.                     |
                                     +-----------------------------------------+
                                                  |
                                    /---------------------------\
                                    |                           |
                                    v                           v
                        +--------------------------+   +-----------------------+
                        | SR `compare_..._column`  |   | Alembic Generic       |
                        | is called.               |   | `compare_column` is   |
                        | (Checks dialect &        |   | called.               |
                        | compares agg types)      |   |                       |
                        +--------------------------+   +-----------------------+
```

## StarRocks Dialect Extensions

The `sqlalchemy-starrocks` dialect hooks into this dispatch process by registering its own set of comparators in the `starrocks.alembic.compare` module.

> To remove the impaction on other type of databases when there are several Alembic plugins be use, we use a custom decorator, which will only handle for StarRocks dialect.

### Accessing Dialect-Specific Options

A key implementation detail is how SQLAlchemy handles dialect-specific keyword arguments (like `starrocks_PARTITION_BY`). Whether a `Table` object is created by the user (e.g., `Table('my_table', metadata, starrocks_PARTITION_BY=...)`, or ORM style) or reflected from the database by the inspector, SQLAlchemy normalizes these options.

All `starrocks_` prefixed arguments are collected and stored in a dictionary accessible via `table.dialect_options['starrocks']`.

This provides the custom comparator functions with a single, consistent location to retrieve StarRocks-specific attributes for both the database-side `Table` object and the metadata-side `Table` object, greatly simplifying the comparison logic.

### Custom Comparator Functions

- **`compare_starrocks_table(...)`**: This is the main entry point for comparing StarRocks tables. It is responsible for diffing StarRocks-specific options that are not handled by Alembic's default comparators. This includes:

  - `ENGINE`
  - Table Type / Key (`PRIMARY KEY`, `AGGREGATE KEY`, etc.)
  - `PARTITION BY`
  - `DISTRIBUTED BY`
  - `ORDER BY`
  - `PROPERTIES`

  > **Note on Table Properties**: The comparison logic for `PROPERTIES` identifies properties that only affect future partitions, such as `replication_num` and `storage_medium`. When changes to these properties are detected, the generated `AlterTablePropertiesOp` will automatically prefix the property name with `default.` (e.g., `default.replication_num`) to ensure the modification applies only to new partitions.

  > **Note on Distribution Buckets**: The `DISTRIBUTED BY` comparison includes a special case: if the distribution method (e.g., `HASH(col)`) is the same between the database and the metadata, but the metadata does not specify a bucket count, the comparator will consider them equal. This is to accommodate scenarios where StarRocks automatically assigns a default bucket count, preventing `autogenerate` from creating unnecessary `ALTER TABLE` statements.
  >
  > **Note on Type Handling**: When this comparator runs, the attributes from the database (`conn_table`) may be structured objects (e.g., `ReflectionPartitionInfo`, `ReflectionDistributionInfo`), while the attributes from the user's code (`metadata_table`) are typically strings. The comparison logic is responsible for handling this duality by normalizing the structured objects into a comparable string representation before checking for differences.

- **`compare_view(...)`**: Registered for the `"view"` type. It compares the `SELECT` definition of views.
- **`compare_mv(...)`**: Registered for the `"materialized_view"` type.
- **`compare_starrocks_column(...)`**: Registered for the `"column"` type. It retrieves StarRocks-specific column-level attributes from the `column.dialect_options['starrocks']` dictionary, like `starrocks_AGG_TYPE` (aggregate type) for `AGGREGATE KEY` tables. Other column properties are still handled by Alembic's generic comparator.

By implementing these functions, the dialect ensures that StarRocks-specific schema features are correctly compared during the `autogenerate` process.
