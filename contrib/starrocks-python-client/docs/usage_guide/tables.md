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
    starrocks_engine="OLAP",
    starrocks_primary_key="id, dt",
    starrocks_partition_by="date_trunc('day', dt)",
    starrocks_distributed_by="HASH(id) BUCKETS 10",
    starrocks_order_by="dt, id",
    starrocks_properties={"replication_num": "3"}
)
```

Note: Usage mirrors standard SQLAlchemy patterns (e.g., MySQL dialect), but you should use types from `starrocks` and keep them uppercase.

**Important**: If you specify a Primary, Unique or Aggregate Key via `starrocks_primary_key`, `starrocks_unique_key` or `starrocks_aggregate_key`, also declare all the key's columns with `primary_key=True` on their `Column(...)` definitions. Reflection reports those keys as the table's primary key, so the model then matches the database, and the ORM can map the table. A Duplicate Key table (`starrocks_duplicate_key`) has no primary key: mark its key columns `primary_key=True` only if the ORM must map the table, and then mark all of them. The dialect refuses `primary_key=True` columns that are not exactly the named key's columns. For a table that names no key, see Table Type below.

## Defining Table Attributes

When defining a StarRocks table using SQLAlchemy, you can specify both table-level and column-level attributes using keyword arguments prefixed with `starrocks_`.

### Table-Level Attributes (`starrocks_*` Prefixes)

StarRocks-specific physical attributes for a table are configured by passing special keyword arguments, prefixed with `starrocks_`, either directly to the `Table` constructor or within the `__table_args__` dictionary (in [ORM style](#defining-tables-with-the-orm-declarative-style)). See the [General Syntax](#general-syntax) section above for a complete example that combines columns and table-level attributes.

#### Available Table-Level Options

Here is a comprehensive list of the supported `starrocks_` prefixed arguments. The order of attributes in the documentation follows the recommended order in the `CREATE TABLE` DDL statement.

The prefix `starrocks_` should be **lower case**. The suffix is case-insensitive, but it is recommended to use lowercase in your code for consistency (all examples in this guide use lowercase).

##### 1. `starrocks_engine`

Specifies the table engine. `OLAP` is the default. Any other value is written as given in `ENGINE=...`, and a table of another engine, such as `mysql`, has no key, so `starrocks_infer_primary_key` gives it no key clause.

- **Type**: `str`
- **Default**: `"OLAP"`

##### 2. Table Type (`starrocks_*_key`)

Defines the table's type (key) and the columns that constitute the key. You must choose **at most one** of the following options.

- **`starrocks_primary_key`**

  - **Description**: Defines a Primary Key type table. Data is sorted by the primary key, and each row is unique.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_primary_key="user_id, event_date"`

- **`starrocks_duplicate_key`**

  - **Description**: Defines a Duplicate Key type table. This is the type StarRocks gives a table that names no key, unless `starrocks_infer_primary_key` makes it a Primary Key table (see below). It's suitable for storing raw, unchanged data.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_duplicate_key="request_id, timestamp"`

- **`starrocks_aggregate_key`**

  - **Description**: Defines an Aggregate Key type table. Rows with the same key are aggregated into a single row.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_aggregate_key="site_id, visit_date"`

- **`starrocks_unique_key`**
  - **Description**: Defines a Unique Key type table, where all rows are unique. It functions like a primary key but with a different underlying implementation strategy. You could use it only when Primary Key type can't satisfy you.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_unique_key="device_id"`

> **A primary key and no key model.** A table that names no `starrocks_*_key` but has primary key columns, from `Column('id', Integer, primary_key=True)` or a `PrimaryKeyConstraint`, gets no key clause, as in earlier versions of the dialect, so StarRocks makes its default Duplicate Key table. That table keeps every row loaded into it, rows with the same key or a NULL key included, and reflects no primary key.
>
> `create_engine(..., starrocks_infer_primary_key=True)` makes such a table a Primary Key table on its primary key columns, where StarRocks can hold one. The option also takes text, as `sqlalchemy.starrocks_infer_primary_key = true` in `alembic.ini` for `engine_from_config()`, and it can go in the URL's query string, as `starrocks://root@localhost:9030/mydatabase?starrocks_infer_primary_key=true`, where it takes the place of the keyword. Alembic's offline mode (`--sql`) and `create_mock_engine()` (for DDL dumps) make the dialect without reading the URL's query string, so give the option in `context.configure(dialect_opts=...)` for Alembic, and as a keyword, `create_mock_engine(url, dump, starrocks_infer_primary_key=True)`, for the mock engine. The Alembic form is `context.configure(dialect_opts={"paramstyle": "named", "starrocks_infer_primary_key": True})`. StarRocks holds the table as a Primary Key table when:
>
> - the key columns are the table's first columns, in key order. SQLAlchemy 2.0's declarative puts the columns of a mixin after the class's own, so an `id` from a mixin leads only with `mapped_column(primary_key=True, sort_order=-1)`; SQLAlchemy 1.4 puts a mixin's columns first;
> - each key column is BOOLEAN, an integer type (LARGEINT included), DATE, DATETIME or VARCHAR, and none is a generated column. So CHAR, and with it `sa.Uuid()`, which is CHAR(32), DECIMAL, FLOAT, DOUBLE, BINARY, JSON and TIME keys are not taken;
> - no key column is nullable, as `primary_key=True, nullable=True` or an ORM `Mapped[Optional[int]]` key makes it. StarRocks makes a Primary Key table's key columns NOT NULL and refuses each row whose key holds a NULL. An `autoincrement=True` column is NOT NULL anyway;
> - the key's declared width fits the 128 bytes StarRocks loads in a row's primary key (the BE's `primary_key_limit_size`), so the table loads every row a Duplicate Key table would, except a row whose VARCHAR key holds zero bytes, where StarRocks counts one more byte for each zero byte, which can push the key past the limit. A `VARCHAR(n)` counts `n` bytes, and 2 more when another key column follows it; BOOLEAN and TINYINT count 1, SMALLINT 2, INT and DATE 4, BIGINT and DATETIME 8, and LARGEINT 16, and an `autoincrement=True` column is the BIGINT the dialect makes it. So `String(100)` with an `Integer` makes a Primary Key table and `String(255)` doesn't. A STRING key, which `Text` and a `String()` with no length are, never does;
> - the table is an OLAP table, distributed by HASH of key columns or by default, not by RANDOM, and partitioned by key columns only;
> - its `starrocks_order_by` columns are of those types, or STRING;
> - it has none of the properties StarRocks refuses for a Primary Key table: `storage_cooldown_ttl`, `storage_cooldown_time`, `base_compaction_forbidden_time_ranges`, and `enable_persistent_index` set to false, which StarRocks refuses from 3.5. The dialect applies this rule on older servers too, so a model gives the same table on every version.
> - it has no `bucket_size` property unless it names a `starrocks_distributed_by`: StarRocks 4.1 allows `bucket_size` only with RANDOM distribution, and a Primary Key table with no `DISTRIBUTED BY` is distributed by HASH.
>
> The dialect finds the columns of `starrocks_partition_by`, `starrocks_distributed_by` and `starrocks_order_by` by the table's column names, backquoted or bare, outside string literals; the unit of an interval, as `day` in `time_slice(dt, INTERVAL 1 day)`, is not a name. When it finds none in such a clause, it can't check the clause. When the table can't be a Primary Key table, the dialect warns, saying why, and sends no key clause, and StarRocks makes its default Duplicate Key table. The warning says what to change: to name the key with `starrocks_duplicate_key`, or, for key columns that don't lead, to put them first, since StarRocks takes no key whose columns don't lead the table. It also quotes the `warnings.filterwarnings()` call that silences it. A table of another engine, such as `ENGINE=mysql`, has no key, so it gets no key clause and no warning.
>
> With the option on, autogenerate compares such a model's key only with a Primary Key table, and leaves a table with any other key type as it is. With it off, it leaves the key of every such table as it is, a Primary Key table's included, since CREATE TABLE would give the model no key.
>
> A Primary Key table behaves differently from the Duplicate Key table the dialect makes with the option off:
>
> - a row whose key the table already holds replaces that row, where a Duplicate Key table keeps both;
> - its key columns can't be altered as a Duplicate Key table's can. StarRocks takes a wider VARCHAR for one from 4.1.1, and a new comment from 4.1.3, and refuses every other change, such as `Integer` to `BigInteger`; before 4.1.1 it refuses any `MODIFY COLUMN` of one. So a migration that alters a key column runs on a table created with the option off and fails on one created with it on. Name `starrocks_duplicate_key` on a table whose key columns will change;
> - StarRocks refuses a synchronous materialized view on it, and before 4.0 a GIN index. Those are separate statements the dialect can't see from the table, so name `starrocks_duplicate_key` on a table that needs them.
>
> Reflection reports a primary key for Primary, Unique and Aggregate Key tables, which hold one row per key, and none for Duplicate Key tables. When you name one of those three key types, also mark its columns `primary_key=True`, so that the model matches the reflected table and the ORM can map it.

> **Upgrading from an earlier version of the dialect**
>
> - By default a table that names no `starrocks_*_key` still gets no key clause, so its `primary_key=True` columns make a Duplicate Key table, as before. With `starrocks_infer_primary_key=True`, a new database built from such a model, or from the same migration scripts, gets Primary Key tables where StarRocks can hold one, while the tables earlier versions created are Duplicate Key tables, which autogenerate leaves as they are. A Primary Key table replaces a row whose key it already holds, refuses NULL keys, and can't alter its key columns later, as described above, so one migration history can then run on an old database and fail on a new one.
> - To keep a table a Duplicate Key table with the option on, name its key as `SHOW CREATE TABLE` reports it, for example `starrocks_duplicate_key="id, name"`. The columns marked `primary_key=True` must then be exactly the key's columns, or none of them; a column that loses `primary_key=True` keeps `nullable=False` if it had it.
> - A Duplicate Key table no longer reflects a primary key, so `automap_base().prepare(autoload_with=engine)` skips it, and the ORM cannot map a reflected `Table` of it. Give the ORM the key yourself: reflect the table with an overriding `Column('id', INTEGER, primary_key=True)`, or set `__mapper_args__ = {"primary_key": [table.c.id]}` on the mapped class.

##### 3. `COMMENT`

The table comment should be passed as the standard `comment` keyword argument to the `Table` constructor, not as a `starrocks_` prefix. Otherwise, it will show some uncertain behaviors.

##### 4. `starrocks_partition_by`

Defines the partitioning strategy.

- **Type**: `str`
- **Example**:

  ```Python
  starrocks_partition_by="""RANGE(event_date) (
      START ('2022-01-01') END ('2023-01-01') EVERY (INTERVAL 1 DAY)
  )"""
  ```

##### 5. `starrocks_distributed_by`

Specifies the data distribution (including bucketing) strategy.

- **Type**: `str`
- **Default**: `RANDOM` for a Duplicate Key table, and `HASH` of the key columns for a Primary Key table. StarRocks has no default distribution for a Unique or Aggregate Key table, so name one.
- **Example**: `starrocks_distributed_by="HASH(user_id) BUCKETS 32"`

> **Note on Buckets**: If you specify a distribution method (e.g., `HASH(user_id)`) but omit the `BUCKETS` clause, StarRocks will automatically assign a bucket count. Alembic's `autogenerate` feature is designed to handle this: if the distribution method in your metadata matches the one in the database and you haven't specified a bucket count, no changes will be detected. This prevents unnecessary `ALTER TABLE` statements when the bucket count is automatically managed by the system.

##### 6. `starrocks_order_by`

Specifies the sorting columns.

- **Type**: `str` (comma-separated column names), or a list of column names
- **Example**: `starrocks_order_by="event_timestamp, event_type"`, or `starrocks_order_by=["event_timestamp", "event_type"]`

The string is sent as written. In a list, each bare column name is quoted as StarRocks needs, so a name it reserves, such as `text`, can be listed as it is. An item holding a space, a parenthesis or a backquote is sent as written, so a name with a space goes in backquotes, as in ``["`event type`", "event_id"]``.

##### 7. `starrocks_properties`

A dictionary of additional table attributes.

- **Type**: `dict[str, str]`
- **Example**:

  ```python
  starrocks_properties={
      "replication_num": "3",
      "storage_medium": "SSD",
      "enable_persistent_index": "true"
  }
  ```

> **Note on Future Partition Properties**: Certain properties, such as `replication_num` and `storage_medium`, can be modified to apply only to newly created partitions. When `alembic revision --autogenerate` detects changes to these specific properties, it will generate an `ALTER TABLE` statement that prefixes them with `default.` prefix (e.g., `SET ("default.replication_num" = "...")`). This ensures that the changes do not affect existing data.

> **Note on `%` in SQL text**: write a single `%`, as StarRocks should read it, in the text of `starrocks_partition_by`, `starrocks_distributed_by`, `starrocks_order_by` and `starrocks_properties`, in a view's or materialized view's `definition` and `starrocks_refresh`, and in the `op.alter_table_*` and `op.alter_materialized_view` operations, for example `starrocks_partition_by="RANGE(str2date(s, '%Y-%m-%d')) (...)"`. The dialect escapes it for the driver.
>
> **Upgrading**: earlier versions of the dialect raised on a single `%` when connected, so models and migration scripts written for them hold `%%`. That `%%` now reaches StarRocks as `%%`, which changes what a `date_format` or `str2date` pattern means without an error. Replace it with `%`. The dialect warns, once for each such text, when it finds `%%`. It can't tell such a `%%` from one you mean, as in the string literal `'a%%b'`, which StarRocks stores as written. To keep a `%%` you mean without the warning, filter it: `warnings.filterwarnings("ignore", message="DDL text .* holds '%%'")`.

### Secondary Indexes

Declare StarRocks' BITMAP, NGRAMBF, GIN and VECTOR indexes with SQLAlchemy's `Index`. `starrocks_using` names the type, `starrocks_properties` its properties and `starrocks_comment` the comment:

```python
from sqlalchemy import Index

Index("ix_city", events.c.city, starrocks_using="BITMAP")
Index(
    "ix_name",
    events.c.name,
    starrocks_using="NGRAMBF",
    starrocks_properties={"gram_num": "4", "bloom_filter_fpp": "0.05"},
    starrocks_comment="n-grams of the name",
)
```

Each compiles to `CREATE INDEX ix_name ON events (name) USING NGRAMBF ("gram_num" = "4", "bloom_filter_fpp" = "0.05") COMMENT '...'`. StarRocks builds the index in an asynchronous schema-change job, and lists it only once the job has ended. The dialect waits for the job after each `CREATE INDEX` and `DROP INDEX`, so `create_all` can create several indexes on one table, and an inspector or an autogenerate run straight after sees them. GIN indexes need the FE configuration `enable_experimental_gin`.

Indexes are reflected with their type, comment and properties, including the ones StarRocks filled in, so autogenerate creates and drops them like any other index. Dropping a column drops its index as well, so in a migration, a `drop_index` for an index that a `drop_column` earlier in the same run took with it does nothing. Index names are matched without regard to case, as StarRocks matches them. `drop_index(..., if_exists=True)` drops the index only if it exists, and any other `drop_index` of a missing index fails with StarRocks' error.

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

Bind a Python list to an ARRAY column, and a dict to a MAP or STRUCT column. An ARRAY column also takes text, as StarRocks writes an array. JSON text such as `"[1, 2, 3]"` is read as JSON, and other text is cast by StarRocks, so `"[a,b]"` is stored as `["a", "b"]`, and text that isn't an array becomes NULL. An ARRAY of MAP or STRUCT values takes JSON text only, since StarRocks can't cast text to it.

A `cast()` to ARRAY, MAP or STRUCT is left out for three kinds of operand: text, an expression with no type, and JSON where the connected StarRocks can't cast JSON to the type. StarRocks casts JSON to MAP from 3.4.3 and to STRUCT from 3.5.0, and never to an ARRAY element, MAP value or STRUCT field of a DATE, DATETIME, DECIMAL or binary type, or to an ARRAY of ARRAYs or MAPs. Every other operand keeps the CAST: an ARRAY, MAP or STRUCT and NULL, which StarRocks casts, and any other type, such as a number, which StarRocks may refuse.

A left-out CAST changes nothing on the server, which computes on the operand's own type. Only where the dialect reads the value, in a column of the statement's result or a subquery column that the query selects as it is, is the operand's text read as the type. Each element is then converted as StarRocks' cast of JSON converts it, so a statement reads as it does where the server runs the CAST: a number or a JSON object in a VARCHAR element reads as its text, a BOOLEAN reads only from `1`, `0`, `true` or `false`, or their text, a STRUCT keeps only its declared fields, each by its exact name, and any other value that isn't of its element's type raises `ValueError`. Array text that StarRocks' cast reads but that isn't JSON, such as `[a,b]` or `[NULL,1]`, is returned as the text, as 1.3 returned it. Anywhere else, such as in a comparison, ORDER BY, GROUP BY, DISTINCT or a function's argument, the dialect warns that StarRocks computes on the operand's type. A view's or materialized view's definition compiles with no server version, so it leaves out, on every server, each JSON CAST that some supported StarRocks lacks, and each CAST it leaves out warns, since the definition's column then has the operand's type. A `func` given no `type_` has no type, so give a function that returns a collection its type, as in `func.split(c, ",", type_=ARRAY(VARCHAR(10)))`. Its result is then read as a list, and a CAST of it reaches StarRocks.

**Note**: Syntax and usage are analogous to SQLAlchemy's style (e.g., MySQL types), but always import and use the StarRocks types in uppercase from `starrocks`.

#### Column-Level Attributes (`starrocks_*` Prefixes)

For `AGGREGATE KEY` tables, you can specify attributes for each column by passing a `starrocks_` prefixed keyword argument directly to the `Column` constructor.

##### Available Column-Level Options

- **`starrocks_is_agg_key`**: A boolean that can be set to `True` to explicitly mark a column as a key in an `AGGREGATE KEY` table. This is optional but improves clarity.
- **`starrocks_agg_type`**: A string specifying the aggregate type for a value column. Supported values are:
  - `'SUM'`, `'REPLACE'`, `'REPLACE_IF_NOT_NULL'`, `'MAX'`, `'MIN'`, `'HLL_UNION'`, `'BITMAP_UNION'`, `'PERCENTILE_UNION'`
  - An aggregate state function, written as StarRocks prints it, such as `'sum(BIGINT)'`

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
    starrocks_aggregate_key="event_date, site_id",
    starrocks_partition_by="date_trunc('day', event_date)",
    starrocks_distributed_by="HASH(site_id)",
    starrocks_properties={"replication_num": "1"}
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

    starrocks_primary_key='order_id, order_dt',
    starrocks_order_by='order_dt, order_id',
    # With BUCKETS
    starrocks_distributed_by='HASH(order_id) BUCKETS 16',
    starrocks_properties={'replication_num': '1'},
)
```

Note: If you omit BUCKETS (e.g., `starrocks_distributed_by='HASH(order_id)'`), StarRocks assigns a bucket count. Autogenerate won’t emit changes when the distribution method matches and buckets were omitted in both metadata and DB.

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

    starrocks_aggregate_key='dt, site_id',
    starrocks_partition_by="date_trunc('day', dt)",
    starrocks_distributed_by='HASH(dt)',
    starrocks_properties={'replication_num': '1'},
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

    starrocks_duplicate_key='event_id, event_type',
    starrocks_distributed_by='RANDOM',
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
    visit_date = Column(DATE, primary_key=True, starrocks_is_agg_key=True)

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

The `starrocks-sqlalchemy` dialect integrates with Alembic to support autogeneration of schema migrations. When you run `alembic revision --autogenerate`, it will compare both the table-level and column-level attributes (mainly for `starrocks_` prefixed attributes) against the database and generate the appropriate DDL.

**To ensure Alembic correctly recognizes StarRocks column types and handles Views/Materialized Views during autogeneration, configure your `env.py` and model definitions as follows:**

### 1. Configure `env.py` for Column Type Rendering

In your Alembic `env.py` file, within both the `run_migrations_offline` and `run_migrations_online` functions, add:

- `render_item=render_column_type` so Alembic correctly interprets StarRocks-specific column types
- `include_object=include_object_for_view_mv` so autogenerate properly handles Views and Materialized Views

Imports:

```python
from starrocks.alembic import render_column_type, include_object_for_view_mv
```

Then, modify `context.configure()` in both functions:

```python
# In run_migrations_offline() and run_migrations_online()
context.configure(
    # ... other parameters ...
    render_item=render_column_type,
    include_object=include_object_for_view_mv,
)
```

### 2. Import StarRocks Data Types in Model Files

When defining your database models (e.g., in `models.py` or similar files), use StarRocks's column types. The simplest way to make all StarRocks data types available is to import them directly:

```python
from starrocks import *
```

Alternatively, you can import specific types as needed:

```python
from starrocks import TINYINT, VARCHAR
```

This ensures that your model definitions correctly map to StarRocks's native data types, allowing `alembic revision --autogenerate` to produce accurate migration scripts.

### Notes

- Changes to **non-alterable attributes** like `ENGINE`, `table type`, or `partitioning` are detected and reported as a warning. Autogenerate generates no operation for them, and still generates the rest of the revision; apply such a change by hand, usually by recreating the table.
- Changing a column’s **aggregation type** is not supported by StarRocks; autogenerate warns about the difference and generates no operation for it.

### Limitations

- **`AUTO_INCREMENT`**: Reflection reads a column's `AUTO_INCREMENT` from `SHOW CREATE TABLE`, since `information_schema.columns` and `SHOW FULL COLUMNS` don't show it, and autogenerate compares it with the model's `autoincrement`. StarRocks can't add `AUTO_INCREMENT` to a column or take it away, so autogenerate warns about a difference, and `op.alter_column` refuses one; recreate the table to change it. A `MODIFY COLUMN` of an `AUTO_INCREMENT` column, which `op.alter_column` writes without the `AUTO_INCREMENT`, is refused by StarRocks before 4.1.3, and from 4.1.3 taken only when it changes nothing but the comment.

### Important Considerations for Autogenerate

#### Handling Time-Consuming ALTER TABLE Operations

StarRocks schema change operations (like `ALTER TABLE ... MODIFY COLUMN`, building a secondary index, or changing the distribution) run as asynchronous jobs and can be time-consuming. Because one table can have only one ongoing schema change operation at a time, StarRocks rejects any other change to the table, with "A schema change operation is in progress", until the job has ended.

> Rename, comment, partition and swap operations are synchronous: when the statement returns, the change is done.

Alembic's `autogenerate` feature may produce an `upgrade()` function that contains multiple consecutive `op.alter_column()` or other `ALTER TABLE` calls for a single table. For example:

```python
def upgrade():
    op.alter_column('my_table', 'col1', ...)
    op.alter_column('my_table', 'col2', ...)
```

The dialect holds back each `ALTER TABLE`, `CREATE INDEX` and `DROP INDEX` that it compiles from a construct naming its table until the job running on that table has ended, so these run one after the other, and `metadata.create_all()` can create several indexes on one table. The constructs are SQLAlchemy's index and table-comment statements, as `create_all()`, `Index.create()` and `Index.drop()` send them, Alembic's operations, and the dialect's own `ALTER TABLE` constructs. A statement sent as text doesn't wait: through `exec_driver_sql()`, `text()` or `op.execute()`, or as SQLAlchemy's `DDL()`, such as a `DDL("ALTER TABLE ...")` listening for `after_create`. The wait covers the `COLUMN`, `OPTIMIZE` and `ROLLUP` jobs that `SHOW ALTER TABLE` lists. A `ROLLUP` job builds an `ALTER TABLE ... ADD ROLLUP` or a synchronous materialized view, and while it runs StarRocks refuses other changes to the table with "A materialized view or rollup index is being created on the table". StarRocks shows a `ROLLUP` job as `FINISHED` up to about 60 ms before it frees the table, so a statement it refuses that way after the wait saw a `ROLLUP` job end is sent again, after 50 ms and then after pauses that double up to a second, within the wait's `starrocks_schema_change_timeout`. After a `CREATE INDEX` or `DROP INDEX` it also waits for the index's own job. The `starrocks.dialect` logger shows each poll at INFO, and at WARNING after about five seconds of waiting and once a minute after that. Two `create_engine()` options control the wait:

```python
engine = create_engine(
    "starrocks://root@localhost:9030/mydatabase",
    starrocks_schema_change_timeout=300,    # seconds; None or 0 waits without a limit (default: 300)
    starrocks_wait_for_schema_change=True,  # False sends each statement at once (default: True)
)
```

When the timeout passes before a statement, the dialect raises `starrocks.dialect.SchemaChangeTimeoutError`, which names the job and its state, and doesn't send the statement. After a `CREATE INDEX` or `DROP INDEX` it gives a warning instead, since the statement has run and its job goes on. If StarRocks cancels the job of a `CREATE INDEX` or `DROP INDEX`, the dialect logs a warning naming the index, the table and the job, because the index was then not built, or not dropped. A job in `WAITING_TXN` waits for the transactions that write its table, which can include a `READ COMMITTED` transaction that your own program holds open on another connection. Without the timeout, that would wait for ever.

Both options also take text from a configuration file, such as `sqlalchemy.starrocks_schema_change_timeout = 600` in `alembic.ini` for `engine_from_config()`, and they can go in the URL's query string, such as `starrocks://root@localhost:9030/mydatabase?starrocks_schema_change_timeout=600`, where they take the place of the keywords. They apply to the whole engine: `execution_options()` doesn't take them. They are separate from the Alembic options of the same names in `context.configure()`, which make a migration wait for the job of each of its schema-change statements.

**Recommendation:**

Each slow schema change rewrites the table's data, so on a large table prefer one statement over several where StarRocks allows it: the `combine_column_alters` rewriter in the [Alembic guide](alembic.md#44-coalescing-column-changes-into-a-single-alter-table) folds a table's added and dropped columns into one `ALTER TABLE`.

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
