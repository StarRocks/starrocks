# dbt-starrocks

This project is **under development**.


The `dbt-starrocks` package contains all of the code enabling dbt to work with a StarRocks database. For
more information on using dbt with StarRocks.

## Getting started
Configuration your envs:

- Python: 3.7.4
- StarRocks: 2.4.0+
- DBT: 1.1.0

Install the `dbt-starrocks` into the `plugin` directory, and
```
  pip install .
```

Create your project:
```
  dbt init
```

## Basic Example
### dbt seed properties(yml):
#### Minimum configuration:
```
config:
  distributed_by: ['id']
```

#### Complete configuration:
```
config:
  engine: 'OLAP'
  keys: ['id', 'name', 'some_date']
  table_type: 'PRIMARY'     //PRIMARY or DUPLICATE or UNIQUE
  distributed_by: ['id']
  buckets: 3                //default 10
  partition_by: ['some_date']
  partition_by_init: ["PARTITION p1 VALUES [('1971-01-01 00:00:00'), ('1991-01-01 00:00:00')),PARTITION p1972 VALUES [('1991-01-01 00:00:00'), ('1999-01-01 00:00:00'))"]
  properties: {"replication_num":"1", "in_memory": "true"}
```
  
### dbt run config(table/incremental):
#### Minimum configuration:
```
{{ config(materialized=var("materialized_var", "table"), distributed_by=['id'])}}
{{ config(materialized='incremental', distributed_by=['id']) }}
```

#### Complete configuration:
```
{{ config(materialized='table', engine='OLAP', buckets=32, distributed_by=['id'], properties={"in_memory": "true"}) }}
{{ config(materialized='incremental', engine='OLAP', buckets=32, distributed_by=['id'], properties={"in_memory": "true"}) }}
```

## Test Adapter
consult [the project](https://github.com/dbt-labs/dbt-adapter-tests)

## Notice
1. When StarRocks Version < 2.5, `Create table as` can only set engine='OLAP' and table_type='DUPLICATE'
2. When StarRocks Version >= 2.5, `Create table as` support table_type='PRIMARY'
3. distributed_by is must