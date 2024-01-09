# dbt-starrocks

![PyPI](https://img.shields.io/pypi/v/dbt-starrocks)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbt-starrocks)
![PyPI - Downloads](https://img.shields.io/pypi/dw/dbt-starrocks)

This project is **under development**.


The `dbt-starrocks` package contains all the code to enable [dbt](https://getdbt.com) to work with [StarRocks](https://www.starrocks.io).

This is an experimental plugin:
- We have not tested it extensively
- Requires StarRocks version 2.5.0 or higher  
  - version 3.1.x is recommended
  - StarRocks versions 2.4 and below are no longer supported


## Installation

This plugin can be installed via pip:

```shell
$ pip install dbt-starrocks
```

## Supported features
| Starrocks <= 2.5 | Starrocks 2.5 ~ 3.1 | Starrocks >= 3.1  |              Feature              |
|:----------------:|:-------------------:|:-----------------:|:---------------------------------:|
|        ✅         |          ✅          |         ✅         |       Table materialization       |
|        ✅         |          ✅          |         ✅         |       View materialization        |
|        ❌         |          ❌          |         ✅         | Materialized View materialization |
|        ❌         |          ✅          |         ✅         |    Incremental materialization    |
|        ❌         |          ✅          |         ✅         |         Primary Key Model         |
|        ✅         |          ✅          |         ✅         |              Sources              |
|        ✅         |          ✅          |         ✅         |         Custom data tests         |
|        ✅         |          ✅          |         ✅         |           Docs generate           |
|        ❌         |          ❌          |         ✅         |       Expression Partition        |
|        ❌         |          ❌          |         ❌         |               Kafka               |

### Notice
1. When StarRocks Version < 2.5, `Create table as` can only set engine='OLAP' and table_type='DUPLICATE'
2. When StarRocks Version >= 2.5, `Create table as` supports table_type='PRIMARY'
3. When StarRocks Version < 3.1 distributed_by is required

## Profile Configuration

**Example entry for profiles.yml:**

```
starrocks:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: localhost
      port: 9030
      schema: analytics
      username: your_starrocks_username
      password: your_starrocks_password
```

| Option   | Description                                            | Required? | Example                        |
|----------|--------------------------------------------------------|-----------|--------------------------------|
| type     | The specific adapter to use                            | Required  | `starrocks`                    |
| host     | The hostname to connect to                             | Required  | `192.168.100.28`               |
| port     | The port to use                                        | Required  | `9030`                         |
| schema   | Specify the schema (database) to build models into     | Required  | `analytics`                    |
| username | The username to use to connect to the server           | Required  | `dbt_admin`                    |
| password | The password to use for authenticating to the server   | Required  | `correct-horse-battery-staple` |
| version  | Let Plugin try to go to a compatible starrocks version | Optional  | `3.1.0`                        |


## Example

### dbt seed properties(yml):
#### Complete configuration:
```
models:
  materialized: table       // table or view or materialized_view
  engine: 'OLAP'
  keys: ['id', 'name', 'some_date']
  table_type: 'PRIMARY'     // PRIMARY or DUPLICATE or UNIQUE
  distributed_by: ['id']
  buckets: 3                // default 10
  partition_by: ['some_date']
  partition_by_init: ["PARTITION p1 VALUES [('1971-01-01 00:00:00'), ('1991-01-01 00:00:00')),PARTITION p1972 VALUES [('1991-01-01 00:00:00'), ('1999-01-01 00:00:00'))"]
  // RANGE, LIST, or Expr partition types should be used in conjunction with partition_by configuration
  // Expr partition type requires an expression (e.g., date_trunc) specified in partition_by
  partition_type: 'RANGE'   // RANGE or LIST or Expr Need to be used in combination with partition_by configuration
  properties: [{"replication_num":"1", "in_memory": "true"}]
  refresh_method: 'async' // only for materialized view default manual
```
  
### dbt run config:
#### Example configuration:
```
{{ config(materialized='view') }}
{{ config(materialized='table', engine='OLAP', buckets=32, distributed_by=['id']) }}
{{ config(materialized='table', partition_by=['date_trunc("day", first_order)'], partition_type='Expr') }}
{{ config(materialized='incremental', table_type='PRIMARY', engine='OLAP', buckets=32, distributed_by=['id']) }}
{{ config(materialized='materialized_view') }}
{{ config(materialized='materialized_view', properties={"storage_medium":"SSD"}) }}
{{ config(materialized='materialized_view', refresh_method="ASYNC START('2022-09-01 10:00:00') EVERY (interval 1 day)") }}
```
For materialized view only support partition_by、buckets、distributed_by、properties、refresh_method configuration.

## Read From Catalog
First you need to add this catalog to starrocks. The following is an example of hive.
```mysql
CREATE EXTERNAL CATALOG `hive_catalog`
PROPERTIES (
    "hive.metastore.uris"  =  "thrift://127.0.0.1:8087",
    "type"="hive"
);
```
How to add other types of catalogs can be found in the documentation.
https://docs.starrocks.io/en-us/latest/data_source/catalog/catalog_overview
Then write the sources.yaml file.
```yaml
sources:
  - name: external_example
    schema: hive_catalog.hive_db
    tables:
      - name: hive_table_name
```
Finally, you might use below marco quote 
```
{{ source('external_example', 'hive_table_name') }}
```


## Test Adapter
consult [the project](https://github.com/dbt-labs/dbt-adapter-tests)

## Contributing
We welcome you to contribute to dbt-starrocks. Please see the [Contributing Guide](https://github.com/StarRocks/starrocks/blob/main/CONTRIBUTING.md) for more information.
