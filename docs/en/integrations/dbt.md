---
sidebar_position: 110
displayed_sidebar: docs
description: "How to use dbt-starrocks to transform data in StarRocks using dbt modeling patterns and best practices."
---

import Experimental from '../_assets/commonMarkdown/_experimental.mdx'

# dbt

`dbt-starrocks` enables the use of `dbt` to transform data in StarRocks using dbt's modeling patterns and best practices.

`dbt-starrocks` [GitHub repo](https://github.com/StarRocks/dbt-starrocks).

<Experimental />

## Supported features

| StarRocks >= 3.1 | StarRocks >= 3.4 |              Feature              |
|:----------------:|:----------------:|:---------------------------------:|
|        ✅        |        ✅         |       Table materialization       |
|        ✅        |        ✅         |       View materialization        |
|        ✅        |        ✅         | Materialized View materialization |
|        ✅        |        ✅         |    Incremental materialization    |
|        ✅        |        ✅         |         Primary Key Model         |
|        ✅        |        ✅         |              Sources              |
|        ✅        |        ✅         | Data tests (generic and singular) |
|        ✅        |        ✅         |            Unit tests             |
|        ✅        |        ✅         |      Storing test failures        |
|        ✅        |        ✅         |  Source freshness (`loaded_at_field`) |
|        ❌        |        ❌         |  Metadata-based source freshness  |
|        ✅        |        ✅         |           Docs generate           |
|        ❌        |        ❌         |          `persist_docs`           |
|        ✅        |        ✅         |       Expression Partition        |
|        ❌        |        ❌         |               Kafka               |
|        ❌        |        ✅         |         Dynamic Overwrite         |
|        `*`       |        ✅         |            Submit task            |
|        ✅        |        ✅         |  Microbatch (Insert Overwrite)   |
|        ❌        |        ✅         | Microbatch (Dynamic Overwrite)   |

`*` Verify the specific `submit task` support for your version, see [SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md)

## Installation

Install the StarRocks DBT adapter using pip:

```sh
pip install dbt-starrocks
```

## Verify Installation

Verify the installation by checking the version:

```sh
dbt --version
```

This should list `starrocks` under plugins.

## Configuration

### Profiles

Create or update `profiles.yml` with StarRocks-specific settings.

```yaml
starrocks_project:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: your-starrocks-host.com
      port: 9030
      schema: your_database
      username: your_username
      password: your_password
      catalog: test_catalog
```

### Parameters

#### `type`

**Description**: The specific adapter to use, this must be set to `starrocks`\
**Required\?**: Required\
**Example**: `starrocks`

#### `host`

**Description**: The hostname to connect to\
**Required\?**: Required\
**Example**: `192.168.100.28`

#### `port`

**Description**: The port to use\
**Required\?**: Required\
**Example**: `9030`

#### `catalog`

**Description**: Specify the catalog to build models into\
**Required\?**: Optional\
**Example**: `default_catalog`

#### `schema`

**Description**: Specify the schema (database in StarRocks) to build models into\
**Required\?**: Required\
**Example**: `analytics`

#### `username`

**Description**: The username to use to connect to the server\
**Required\?**: Required\
**Example**: `dbt_admin`

#### `password`

**Description**: The password to use for authenticating to the server\
**Required\?**: Required\
**Example**: `correct-horse-battery-staple` 

#### `version`

**Description**: Let Plugin try to go to a compatible starrocks version\
**Required\?**: Optional\
**Example**: `3.1.0`

#### `use_pure`

**Description**: set to "true" to use C extensions\
**Required\?**: Optional\
**Example**: `true`

#### `is_async`

**Description**: "true" to submit suitable tasks as etl tasks.\
**Required\?**: Optional\
**Example**: `true`

#### `async_query_timeout`

**Description**: Sets the `query_timeout` value when submitting a task to StarRocks\
**Required\?**: Optional\
**Example**: `300`


### Sources

Create or update `sources.yml`

```yml
sources:
  - name: your_source
    database: your_sr_catalog
    schema: your_sr_database
    tables:
      - name: your_table
```

If the catalog is not specified in the schema, it will default to the catalog defined in the profile. Using the profile from earlier, if catalog is not defined, the model will assume the source is located at `test_catalog.your_sr_database`. 

## Materializations

### Table

Basic Table Configuration

```sql
{{ config(
    materialized='table',
    engine='OLAP',
    keys=['id', 'name', 'created_date'],
    table_type='PRIMARY',
    distributed_by=['id'],
    buckets=3,
    partition_by=['created_date'],
    properties=[
        {"replication_num": "1"}
    ]
) }}


SELECT 
    id,
    name,
    email,
    created_date,
    last_modified_date
FROM {{ source('your_source', 'users') }}
```

## Configuration Options

- engine: Storage engine (default: `OLAP`)
- keys: Columns that define the sort key
- table_type: Table model type 
  - `PRIMARY`: Primary key model (supports upserts and deletes)
  - `DUPLICATE`: Duplicate key model (allows duplicate rows)
  - `UNIQUE`: Unique key model (enforces uniqueness)
- `distributed_by`: Columns for hash distribution
- `buckets`: Number of buckets for data distribution (leave empty for auto bucketing)
- `partition_by`: Columns for table partitioning
- `partition_by_init`: Initial partition definitions
- `properties`: Additional StarRocks table properties

## Tables in External Catalogs

### Read from External into StarRocks

This example creates a materialized table in StarRocks containing aggregated data from an external Hive catalog.

:::tip
Configure the external catalog if it does not already exist:

```sql
CREATE EXTERNAL CATALOG `hive_external`
PROPERTIES (
    "hive.metastore.uris"  =  "thrift://127.0.0.1:8087",
    "type"="hive"
);
```
:::

```sql
{{ config(
    materialized='table',
    keys=['product_id', 'order_date'],
    distributed_by=['product_id'],
    partition_by=['order_date']
) }}

-- Aggregate data from Hive external catalog into StarRocks table
SELECT 
    h.product_id,
    h.order_date,
    COUNT(*) as order_count,
    SUM(h.amount) as total_amount,
    MAX(h.last_updated) as last_updated
FROM {{ source('hive_external', 'orders') }} h
GROUP BY 
    h.product_id,
    h.order_date
```

### Write to External

```sql
{{
  config(
    materialized='table',
    on_table_exists = 'replace',
    partition_by=['order_date'],
    properties={},
    catalog='external_catalog',
    database='test_db'
  )
}}

SELECT * FROM {{ source('iceberg_external', 'orders') }}
```

The configuration for materialization to external catalogs supports fewer options. `on_table_exist`s, `partition_by`, and `properties` are supported. If `catalog` and `database` are not set, the defaults from the profile will be used. 

### Incremental

Incremental materializations are supported in StarRocks as well:

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    table_type='PRIMARY',
    keys=['id'],
    distributed_by=['id'],
    incremental_strategy='default'
) }}

SELECT
    id,
    user_id,
    event_name,
    event_timestamp,
    properties
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

#### Incremental Strategies

`dbt-starrocks` supports multiple incremental strategies:

1. `append` (default): Simply appends new records without deduplication
2. `insert_overwrite`: Overwrites table partitions with insertion
3. `dynamic_overwrite`: Overwrites, creates, and writes table partitions

For more information about which overwrite strategy to use, see the [INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md) documentation. 

:::note
Currently, incremental merge is not supported. 
:::

## Testing

`dbt-starrocks` supports every dbt test type. No adapter-specific configuration is required — the tests compile to standard SQL that StarRocks executes directly.

### Data tests

The four built-in generic tests (`not_null`, `unique`, `accepted_values`, and `relationships`) are supported, as are custom generic tests defined in `macros/` and singular tests defined as `.sql` files under `test-paths`.

```yml
models:
  - name: stg_customers
    columns:
      - name: id
        data_tests: [not_null, unique]
      - name: region
        data_tests:
          - accepted_values:
              values: ['us', 'eu']
          - relationships:
              to: ref('dim_customers')
              field: region
```

Test severity settings (`severity`, `error_if`, `warn_if`, `fail_calc`, and `limit`) are handled by dbt before any SQL reaches StarRocks and behave as they do on other adapters.

### Unit tests

Unit tests are supported. dbt builds the fixture rows into the compiled query, so no data is written to StarRocks:

```yml
unit_tests:
  - name: test_dim_customers_counts
    model: dim_customers
    given:
      - input: ref('stg_customers')
        rows:
          - {id: 1, name: 'a', region: 'us'}
          - {id: 2, name: 'b', region: 'us'}
    expect:
      rows:
        - {region: 'us', n: 2}
```

Run them with:

```sh
dbt test --select test_type:unit
```

### Storing test failures

`dbt test --store-failures`, the `store_failures` config, and `store_failures_as` (`table` or `view`) are all supported. Failing rows are written to a separate schema named `<schema>_dbt_test__audit`, which the adapter creates if it does not exist:

```sql
SELECT * FROM `analytics_dbt_test__audit`.`not_null_stg_customers_name`;
```

Failure tables are created with `CREATE TABLE AS`, so they accept the same configuration options as table models. Use this to control how the failure table is stored:

```yml
      - name: name
        data_tests:
          - not_null:
              config:
                store_failures: true
                alias: nn_name_failures
                distributed_by: ['id']
                buckets: 3
```

Without a `distributed_by` value, the failure table is created with random distribution.

:::tip
`store_failures_as: view` stores the failing rows as a view instead of a table, which avoids creating and reloading a table on every test run.
:::

### Source freshness

`dbt source freshness` requires each source table to declare a `loaded_at_field`:

```yml
sources:
  - name: raw
    tables:
      - name: events
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 24, period: hour}
```

:::warning
Metadata-based freshness is not supported. Every source that needs a freshness check must expose a timestamp column.
:::

## Generating documentation

`dbt docs generate` builds the project catalog by querying `information_schema.tables` and `information_schema.columns`. Models, seeds, views, materialized views, and sources are all included, each with its columns, ordinal positions, and data types. `dbt docs generate --static` and `dbt docs serve` work as well.

The descriptions you write in your `.yml` files are read from the dbt manifest, so they appear in the documentation site as usual. However, the following metadata is not collected from StarRocks:

- Table and column comments stored in the database.
- Table owner.
- Table statistics, such as row counts and sizes.

Two further details affect how relations are described:

- Column types are reported without precision. A `VARCHAR(64)` column appears as `varchar`, and a `DECIMAL(18,4)` column appears as `decimal`. Use [SHOW CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) when the exact type matters.
- Materialized views are documented as views. This affects the documentation site only, not `dbt run`.

:::warning
`persist_docs` is not supported. Do not set `persist_docs` in `dbt_project.yml`, where it would apply to every model in the project.
:::

## Troubleshooting

- Before using external catalogs in dbt, you must create them in StarRocks.[Catalog overview](../data_source/catalog/catalog_overview.md).
- External sources should be accessed using the `{{ source('external_source_name', 'table_name' }}` macro. 
- `dbt seed` was not tested for external catalogs and is not currently supported.
- In order for `dbt` to create models in external databases that do not currently exist, the location of the models must be set through properties. 
- External models need to define the location they are stored at. This location will be defined if the destination database exists and sets the location property. Otherwise, the location needs to be set. 
  - We will currently only support creating external models in databases that already exist. 
