---
displayed_sidebar: docs
---

import Experimental from '../_assets/commonMarkdown/_experimental.mdx'

# dbt

`dbt-starrocks` enables the use of `dbt` to transform data in StarRocks using dbt's modeling patterns and best practices.

`dbt-starrocks` [GitHub repo](https://github.com/StarRocks/dbt-starrocks).

<Experimental />

## Supported features

## Supported features

| StarRocks >= 3.1 | StarRocks >= 3.4 |              Feature              |
|:----------------:|:----------------:|:---------------------------------:|
|        ✅        |        ✅         |       Table materialization       |
|        ✅        |        ✅         |       View materialization        |
|        ✅        |        ✅         | Materialized View materialization |
|        ✅        |        ✅         |    Incremental materialization    |
|        ✅        |        ✅         |         Primary Key Model         |
|        ✅        |        ✅         |              Sources              |
|        ✅        |        ✅         |         Custom data tests         |
|        ✅        |        ✅         |           Docs generate           |
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

**Description**:  The specific adapter to use, this must be set to `starrocks`\
**Required\?**:  Required\
**Example**:  `starrocks`                    

#### `host`

**Description**:  The hostname to connect to\
**Required\?**:  Required\
**Example**:  `192.168.100.28`               

#### `port`

**Description**:  The port to use\
**Required\?**:  Required\
**Example**:  `9030`                         

#### `catalog`

**Description**:  Specify the catalog to build models into\
**Required\?**:  Optional\
**Example**:  `default_catalog`       

#### `schema`

**Description**:  Specify the schema (database in StarRocks) to build models into\
**Required\?**:  Required\
**Example**:  `analytics`                    

#### `username`

**Description**:  The username to use to connect to the server\
**Required\?**:  Required\
**Example**:  `dbt_admin`                    

#### `password`

**Description**:  The password to use for authenticating to the server\
**Required\?**:  Required\
**Example**:  `correct-horse-battery-staple` 

#### `version`

**Description**:  Let Plugin try to go to a compatible starrocks version\
**Required\?**:  Optional\
**Example**:  `3.1.0`                        

#### `use_pure`

**Description**:  set to "true" to use C extensions\
**Required\?**:  Optional  \
**Example**:  `true`                         

#### `is_async`

**Description**:  "true" to submit suitable tasks as etl tasks.\
**Required\?**:  Optional  \
**Example**:  `true`                         

#### `async_query_timeout`

**Description**:  Sets the `query_timeout` value when submitting a task to StarRocks\
**Required\?**:  Optional  \
**Example**:  `300`                          


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

## Troubleshooting

- Before using external catalogs in dbt, you must create them in StarRocks. There is documentation on that [here](../data_source/catalog/catalog_overview.md).
- Ensure external sources are properly defined with the `schema: <catalog>.<database>` format. 
- External sources should be accessed using the `{{ source('external_source_name', 'table_name' }}` macro. 
- `dbt seed` was not tested for external catalogs and is not currently supported.
- In order for `dbt` to create models in external databases that do not currently exist, the location of the models must be set through properties. 
- External models need to define the location they are stored at. This location will be defined if the destination database exists and sets the location property. Otherwise, the location needs to be set. 
  - We will currently only support creating external models in databases that already exist. 
