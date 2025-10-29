# Defining StarRocks Tables with SQLAlchemy

This guide provides a comprehensive overview of how to define StarRocks tables using SQLAlchemy, which allows you to manage your database schema in Python code and integrate with tools like Alembic for migrations.

## General Syntax

The following example shows creating a table with both table-level attributes and column definitions using StarRocks data types (uppercase) imported from `starrocks`.

```python
from sqlalchemy import Table, MetaData, Column
from starrocks import *  # use StarRocks types like INTEGER, VARCHAR, DATETIME, ARRAY, MAP, STRUCT

metadata = MetaData()

my_table = Table(
    'my_table',
    metadata,

    # Columns (use StarRocks types)
    Column('id', INTEGER, primary_key=True, nullable=False),
    Column('dt', DATETIME, primary_key=True),
    Column('data', VARCHAR(255), comment="payload"),

    # Table-level attributes (optional)
    comment="my first sqlalchemy table",
    starrocks_ENGINE="OLAP",
    starrocks_PRIMARY_KEY="id, dt",
    starrocks_PARTITION_BY="date_trunc('day', dt)",
    starrocks_DISTRIBUTED_BY="HASH(id) BUCKETS 10",
    starrocks_ORDER_BY="dt, id",
    starrocks_PROPERTIES={"replication_num": "3"}
)
```

Note: Usage mirrors standard SQLAlchemy patterns (e.g., MySQL dialect), but you should use types from `starrocks` and keep them uppercase.

**Important**: If you specify a StarRocks table key via `starrocks_PRIMARY_KEY`, `starrocks_UNIQUE_KEY`, `starrocks_DUPLICATE_KEY`, or `starrocks_AGGREGATE_KEY`, all columns listed in that key **MUST** also be declared with `primary_key=True` on their `Column(...)` definitions. This ensures SQLAlchemy metadata and Alembic autogenerate behave correctly.

## Defining Table Attributes

When defining a StarRocks table using SQLAlchemy, you can specify both table-level and column-level attributes using keyword arguments prefixed with `starrocks_`.

### Table-Level Attributes (`starrocks_*` Prefixes)

StarRocks-specific physical attributes for a table are configured by passing special keyword arguments, prefixed with `starrocks_`, either directly to the `Table` constructor or within the `__table_args__` dictionary (in [ORM style](#defining-tables-with-the-orm-declarative-style)). See the [General Syntax](#general-syntax) section above for a complete example that combines columns and table-level attributes.

#### Available Table-Level Options

Here is a comprehensive list of the supported `starrocks_` prefixed arguments. The order of attributes in the documentation follows the recommended order in the `CREATE TABLE` DDL statement.

The prefix `starrocks_` should be **lower case**, the other part is recommended to be upper case for clearity.

##### 1. `starrocks_ENGINE`

Specifies the table engine. `OLAP` is the default and only supported engine.

- **Type**: `str`
- **Default**: `"OLAP"`

##### 2. Table Type (`starrocks_*_KEY`)

Defines the table's type (key) and the columns that constitute the key. You must choose **at most one** of the following options.

- **`starrocks_PRIMARY_KEY`**

  - **Description**: Defines a Primary Key type table. Data is sorted by the primary key, and each row is unique.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_PRIMARY_KEY="user_id, event_date"`

- **`starrocks_DUPLICATE_KEY`**

  - **Description**: Defines a Duplicate Key type table. This is the default type if no key is specified. It's suitable for storing raw, unchanged data.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_DUPLICATE_KEY="request_id, timestamp"`

- **`starrocks_AGGREGATE_KEY`**

  - **Description**: Defines an Aggregate Key type table. Rows with the same key are aggregated into a single row.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_AGGREGATE_KEY="site_id, visit_date"`

- **`starrocks_UNIQUE_KEY`**
  - **Description**: Defines a Unique Key type table, where all rows are unique. It functions like a primary key but with a different underlying implementation strategy. You could use it only when Primary Key type can't satisfy you.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_UNIQUE_KEY="device_id"`

> Although you **CAN'T only** specify the Primary Key type in Columns, such as `Column('id', Integer, primary_key=True)`, or by using `PrimaryKeyConstraint`, You still need to specify it as the SQLAlchemy's stardard primary key declaration, either via Columns or `PrimaryKeyConstraint`, to prevent errors.

##### 3. `COMMENT`

The table comment should be passed as the standard `comment` keyword argument to the `Table` constructor, not as a `starrocks_` prefix. Otherwise, it will show some uncertain behaviors.

##### 4. `starrocks_PARTITION_BY`

Defines the partitioning strategy.

- **Type**: `str`
- **Example**:

  ```Python
  starrocks_PARTITION_BY="""RANGE(event_date) (
      START ('2022-01-01') END ('2023-01-01') EVERY (INTERVAL 1 DAY)
  )"""
  ```

##### 5. `starrocks_DISTRIBUTED_BY`

Specifies the data distribution (including bucketing) strategy.

- **Type**: `str`
- **Default**: `RANDOM`
- **Example**: `starrocks_DISTRIBUTED_BY="HASH(user_id) BUCKETS 32"`

> **Note on Buckets**: If you specify a distribution method (e.g., `HASH(user_id)`) but omit the `BUCKETS` clause, StarRocks will automatically assign a bucket count. Alembic's `autogenerate` feature is designed to handle this: if the distribution method in your metadata matches the one in the database and you haven't specified a bucket count, no changes will be detected. This prevents unnecessary `ALTER TABLE` statements when the bucket count is automatically managed by the system.

##### 6. `starrocks_ORDER_BY`

Specifies the sorting columns.

- **Type**: `str` (comma-separated column names)
- **Example**: `starrocks_ORDER_BY="event_timestamp, event_type"`

##### 7. `starrocks_PROPERTIES`

A dictionary of additional table attributes.

- **Type**: `dict[str, str]`
- **Example**:

  ```python
  starrocks_PROPERTIES={
      "replication_num": "3",
      "storage_medium": "SSD",
      "enable_persistent_index": "true"
  }
  ```

> **Note on Future Partition Properties**: Certain properties, such as `replication_num` and `storage_medium`, can be modified to apply only to newly created partitions. When `alembic revision --autogenerate` detects changes to these specific properties, it will generate an `ALTER TABLE` statement that prefixes them with `default.` prefix (e.g., `SET ("default.replication_num" = "...")`). This ensures that the changes do not affect existing data.

### Column Attributes

#### Column Data Types

Use StarRocks data types (uppercase) from `starrocks` to declare columns. The common scalar types include `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `LARGEINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `BOOLEAN`, `CHAR`, `VARCHAR`, `STRING`, `BINARY`, `VARBINARY`, `DATE`, `DATETIME`, `JSON`, `BITMAP`, `HLL`.

```python
from starrocks import INTEGER, VARCHAR, DECIMAL, DATETIME

Column('name', VARCHAR(50))
Column('price', DECIMAL(10, 2))
Column('created_at', DATETIME)
```

Complex types are composed from these base types:

- ARRAY: `ARRAY(<item_type>)`
- MAP: `MAP(<key_type>, <value_type>)`
- STRUCT: `STRUCT(field1=<type>, field2=<type>, ...)` or `STRUCT(("field1", <type>), ("field2", <type>), ...)`

Examples (including nesting):

```python
from starrocks import STRING, INTEGER, VARCHAR, DECIMAL, ARRAY, MAP, STRUCT

Column('tags', ARRAY(STRING))
Column('attributes', MAP(STRING, INTEGER))
Column('profile', STRUCT(name=VARCHAR(50), age=INTEGER))
Column(
    'doc',
    STRUCT(
        id=INTEGER,
        tags=ARRAY(STRING),
        metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
    )
)
```

**Note**: Syntax and usage are analogous to SQLAlchemy's style (e.g., MySQL types), but always import and use the StarRocks types in uppercase from `starrocks`.

#### Column-Level Attributes (`starrocks_*` Prefixes)

For `AGGREGATE KEY` tables, you can specify attributes for each column by passing a `starrocks_` prefixed keyword argument directly to the `Column` constructor.

##### Available Column-Level Options

- **`starrocks_is_agg_key`**: A boolean that can be set to `True` to explicitly mark a column as a key in an `AGGREGATE KEY` table. This is optional but improves clarity.
- **`starrocks_agg_type`**: A string specifying the aggregate type for a value column. Supported values are:
  - `'SUM'`, `'REPLACE'`, `'REPLACE_IF_NOT_NULL'`, `'MAX'`, `'MIN'`, `'HLL_UNION'`, `'BITMAP_UNION'`

##### Notes on ALTER TABLE ADD/MODIFY COLUMN

- When creating, adding or modifying columns on an `AGGREGATE KEY` table, StarRocks requires that each column’s role be explicit:
  - Key columns can be marked with `starrocks_is_agg_key=True`.
  - Value columns must specify `starrocks_agg_type` (e.g., `'SUM'`, `'REPLACE'`, etc.).
- Column-level `starrocks_is_agg_key`/`starrocks_agg_type` attributes are only valid for `AGGREGATE KEY` tables.
- In `ALTER TABLE ... ADD COLUMN`/`MODIFY COLUMN` contexts, Alembic may not provide table-level attributes to the compiler. The dialect therefore allows specifying `starrocks_is_agg_key` or `starrocks_agg_type` directly on the column for these operations.

Examples:

```python
# Add a key column to an AGGREGATE KEY table
op.add_column(
    'aggregate_table',
    Column('site_id2', INTEGER, nullable=False, starrocks_is_agg_key=True),
)

# Add a value column with aggregation
op.add_column(
    'aggregate_table',
    Column('page_views2', INTEGER, nullable=False, starrocks_agg_type='SUM'),
)

# Modify a column’s definition (type/nullable/comment). Note: changing agg type is not supported
op.alter_column(
    'aggregate_table',
    'page_views',
    existing_type=INTEGER,
    nullable=False,
    # If needed for clarity, you may repeat the role marker (key or agg_type)
    # starrocks_agg_type='SUM',  # allowed for context but agg type change is not supported
)
```

### Example: Aggregate Key Table

Here is a complete example of an `AGGREGATE KEY` table that demonstrates both table-level and column-level attributes.

```python
from sqlalchemy import Table, MetaData, Column
from starrocks import INTEGER, DATE, BITMAP, HLL

metadata = MetaData()

aggregate_table = Table(
    'aggregate_table',
    metadata,
    # Key columns (explicitly marked for clarity)
    Column('event_date', DATE, primary_key=True, starrocks_is_agg_key=True),
    Column('site_id', INTEGER, primary_key=True, starrocks_is_agg_key=True),

    # Value columns with aggregate types
    Column('page_views', INTEGER, starrocks_agg_type='SUM'),
    Column('last_visit_time', DATE, starrocks_agg_type='REPLACE'),
    Column('user_ids', BITMAP, starrocks_agg_type='BITMAP_UNION'),
    Column('uv_estimate', HLL, starrocks_agg_type='HLL_UNION'),

    # Table-level attributes
    starrocks_AGGREGATE_KEY="event_date, site_id",
    starrocks_PARTITION_BY="date_trunc('day', event_date)",
    starrocks_DISTRIBUTED_BY="HASH(site_id)",
    starrocks_PROPERTIES={"replication_num": "1"}
)
```

## Examples

### Primary Key table with ORDER BY and DISTRIBUTION (two-column key)

```python
from sqlalchemy import Table, MetaData, Column
from starrocks import INTEGER, VARCHAR, DATETIME

metadata = MetaData()

orders = Table(
    'orders',
    metadata,
    Column('order_id', INTEGER, primary_key=True),
    Column('order_dt', DATETIME, primary_key=True),
    Column('customer', VARCHAR(100), nullable=False),

    starrocks_PRIMARY_KEY='order_id, order_dt',
    starrocks_ORDER_BY='order_dt, order_id',
    # With BUCKETS
    starrocks_DISTRIBUTED_BY='HASH(order_id) BUCKETS 16',
    starrocks_PROPERTIES={'replication_num': '1'},
)
```

Note: If you omit BUCKETS (e.g., `starrocks_DISTRIBUTED_BY='HASH(order_id)'`), StarRocks assigns a bucket count. Autogenerate won’t emit changes when the distribution method matches and buckets were omitted in both metadata and DB.

### Aggregate Key table with PARTITION and PROPERTIES

```python
from sqlalchemy import Table, MetaData, Column
from starrocks import INTEGER, DATE, HLL, BITMAP

metadata = MetaData()

daily_stats = Table(
    'daily_stats',
    metadata,
    Column('dt', DATE, primary_key=True),
    Column('site_id', INTEGER, primary_key=True, starrocks_is_agg_key=True),
    Column('pv', INTEGER, starrocks_agg_type='SUM'),
    Column('uv_est', HLL, starrocks_agg_type='HLL_UNION'),
    Column('user_ids', BITMAP, starrocks_agg_type='BITMAP_UNION'),

    starrocks_AGGREGATE_KEY='dt, site_id',
    starrocks_PARTITION_BY="date_trunc('day', dt)",
    starrocks_DISTRIBUTED_BY='HASH(dt)',
    starrocks_PROPERTIES={'replication_num': '1'},
)
```

### Duplicate Key table focused on complex types (ARRAY/MAP/STRUCT)

```python
from sqlalchemy import Table, MetaData, Column
from starrocks import INTEGER, VARCHAR, STRING, DECIMAL, ARRAY, MAP, STRUCT

metadata = MetaData()

events = Table(
    'events',
    metadata,
    Column('event_id', INTEGER, primary_key=True),
    Column('event_type', VARCHAR(50), primary_key=True),
    Column('tags', ARRAY(STRING)),
    Column('attributes', MAP(STRING, STRING)),
    Column('payload', STRUCT(
        id=INTEGER,
        labels=ARRAY(VARCHAR(20)),
        metrics=MAP(STRING, DECIMAL(10, 2))
    )),

    starrocks_DUPLICATE_KEY='event_id, event_type',
    starrocks_DISTRIBUTED_BY='RANDOM',
)
```

## Alembic Operations Examples (manual operations)

### Column changes with `op.alter_column`

```python
from alembic import op
from starrocks import INTEGER, VARCHAR

# Change type and nullable
op.alter_column('orders', 'customer',
    existing_type=VARCHAR(50),
    modify_type=VARCHAR(100),
    modify_nullable=False)

# Aggregate Key context: include role markers for clarity when adding/modifying
# (agg type changes are unsupported by StarRocks)
op.add_column('daily_stats', Column('pv2', INTEGER, nullable=False, starrocks_agg_type='SUM'))
```

### Table-level StarRocks attributes with `op.alter_table_*`

```python
from alembic import op

# Partition (method only; create partitions separately)
op.alter_table_partition('daily_stats', "date_trunc('month', dt)")

# Distribution (with or without buckets)
op.alter_table_distribution('orders', 'HASH(order_id)', buckets=32)

# Order by
op.alter_table_order('orders', 'order_dt, order_id')

# Properties (note: certain changes may be emitted as default.* for new partitions)
op.alter_table_properties('daily_stats', {'replication_num': '2'})
```

## Defining Tables with the ORM (Declarative Style)

When using SQLAlchemy's Declarative style, you define table-level attributes within the `__table_args__` dictionary. Column-level attributes are passed directly as keyword arguments to each `Column`.

### Example: ORM Aggregate Key Table

```python
from sqlalchemy import Column
from sqlalchemy.orm import declarative_base
from starrocks import INTEGER, STRING, DATE, BITMAP, HLL

Base = declarative_base()

class PageViewAggregates(Base):
    __tablename__ = 'page_view_aggregates'

    # -- Key Columns --
    page_id = Column(INTEGER, primary_key=True, starrocks_is_agg_key=True)
    visit_date = Column(DATE, starrocks_is_agg_key=True)

    # -- Value Columns --
    total_views = Column(INTEGER, starrocks_agg_type='SUM')
    last_user = Column(STRING, starrocks_agg_type='REPLACE')
    distinct_users = Column(BITMAP, starrocks_agg_type='BITMAP_UNION')
    uv_estimate = Column(HLL, starrocks_agg_type='HLL_UNION')

    # -- Table-Level Arguments --
    __table_args__ = {
        'starrocks_aggregate_key': 'page_id, visit_date',
        'starrocks_partition_by': 'date_trunc("day", visit_date)',
        'starrocks_distributed_by': 'HASH(page_id)',
        'starrocks_properties': {"replication_num": "3"}
    }
```

## Integration with Alembic

The `sqlalchemy-starrocks` dialect integrates with Alembic to support autogeneration of schema migrations. When you run `alembic revision --autogenerate`, it will compare both the table-level and column-level attributes (mainly for `starrocks_` prefixed attributes) against the database and generate the appropriate DDL.

**To ensure Alembic correctly recognizes StarRocks column types during autogeneration, you need to configure your `env.py` file and model definitions as follows:**

### 1. Configure `env.py` for Column Type Rendering

In your Alembic `env.py` file, within both the `run_migrations_offline` and `run_migrations_online` functions, you must add the `render_item=render.render_column_type` parameter to `context.configure()`. This enables Alembic to correctly interpret and generate DDL for StarRocks-specific column types.

First, ensure you have the necessary import:

```python
from starrocks.alembic import render
```

Then, modify `context.configure()`:

```python
# In run_migrations_offline() and run_migrations_online()
context.configure(
    # ... other parameters ...
    render_item=render.render_column_type,
)
```

### 2. Import StarRocks Data Types in Model Files

When defining your database models (e.g., in `models.py` or similar files), use StarRocks's column types. The simplest way to make all StarRocks data types available is to import them directly:

```python
from starrocks import *
```

Alternatively, you can import specific types as needed:

```python
from starrocks.common.types import TINYINT, VARCHAR
```

This ensures that your model definitions correctly map to StarRocks's native data types, allowing `alembic revision --autogenerate` to produce accurate migration scripts.

### Notes

- Changes to **non-alterable attributes** like `ENGINE`, `table type`, or `partitioning` will be detected, but will raise an error to prevent generating an unsupported migration.
- Changing a column’s **aggregation type** is not supported by StarRocks; autogenerate will detect differences and raise an error instead of producing DDL.

### Limitations

- **`AUTO_INCREMENT`**: Currently, the reflection process does not detect the `AUTO_INCREMENT` property on columns. This is because this information is not available in a structured way from `information_schema.columns` or `SHOW FULL COLUMNS`. While it is present in the output of `SHOW CREATE TABLE`, parsing this is not yet implemented. Therefore, Alembic's `autogenerate` will not be able to detect or generate migrations for `AUTO_INCREMENT` columns.

### Important Considerations for Autogenerate

#### Handling Time-Consuming ALTER TABLE Operations

StarRocks schema change operations (like `ALTER TABLE ... MODIFY COLUMN`) can be time-consuming. Because one table can have only one ongoing schema change operation at a time, StarRocks does not allow other schema change jobs to be submitted for the same table.

> Some other operations, such as rename, comment, partition, index and swap, are synchronous operations, a command return indicates that the execution is finished. Then, the next operation can be submitted.

Alembic's `autogenerate` feature may produce an `upgrade()` function that contains multiple consecutive `op.alter_column()` or other `ALTER TABLE` calls for a single table. For example:

```python
def upgrade():
    # Potentially problematic if schema changes are slow
    op.alter_column('my_table', 'col1', ...)
    op.alter_column('my_table', 'col2', ...)
```

If the first `alter_column` operation takes a long time to complete, the second one will fail when Alembic tries to execute it immediately after the first.

**Recommendation:**

For potentially slow `ALTER TABLE` operations, it is recommended to modify only **one column or one table property at a time**. After `autogenerate` creates a migration script, review it. If you see multiple `ALTER` operations for the same table that you suspect might be slow, you should split them into separate migration scripts.

However, operations that are typically fast, such as adding or dropping multiple columns, can often be executed together without issue.

#### Limitations on Modifying Complex Type Columns

While `alembic autogenerate` may generate migration scripts to modify columns with complex data types (`ARRAY`, `MAP`, `STRUCT`), StarRocks itself imposes strict limitations on what modifications are actually permissible. Attempting to run an unsupported `ALTER TABLE` operation will result in an error from the database.

It is crucial to be aware of the following StarRocks limitations:

- **STRUCT**: StarRocks only supports adding, dropping, or replacing sub-columns within a `STRUCT` type. The dialect does not yet support autogeneration for these changes.
- **ARRAY/MAP**: Modifying columns of type `ARRAY` or `MAP` is **not supported**.
- **Type Conversion**: Converting a column from a complex type to any other type (e.g., `ARRAY` to `STRING`) or vice-versa is **not supported**.

**Recommendation:**

Given these limitations, if you need to modify an `ARRAY`, `MAP`, or `STRUCT` column beyond what StarRocks supports, you should perform the migration manually. The recommended approach is to:

1. Create a new column with the desired definition.
2. Write a data migration script to copy and transform data from the old column to the new one.
3. Update your application code to use the new column.
4. Drop the old column in a separate migration.
