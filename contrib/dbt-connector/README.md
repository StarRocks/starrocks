# dbt-starrocks

This project is **under development**.


The `dbt-starrocks` package contains all the code enabling [dbt](https://getdbt.com) to work with [StarRocks](https://www.starrocks.io).

This is an experimental plugin:
- We have not tested it extensively
- Requirements at least StarRocks version 2.5.0+  
  - version 3.1.x is recommended
  - Previous versions will no longer support


## Installation

This plugin can be installed via pip:

```shell
$ pip install dbt-starrocks
```

## Supported features
### Notice
1. When StarRocks Version < 2.5, `Create table as` can only set engine='OLAP' and table_type='DUPLICATE'
2. When StarRocks Version >= 2.5, `Create table as` support table_type='PRIMARY'
3. When StarRocks Version < 3.1 distributed_by is must

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
config:
  engine: 'OLAP'
  keys: ['id', 'name', 'some_date']
  table_type: 'PRIMARY'     //PRIMARY or DUPLICATE or UNIQUE
  distributed_by: ['id']
  buckets: 3                //default 10
  partition_by: ['some_date']
  partition_by_init: ["PARTITION p1 VALUES [('1971-01-01 00:00:00'), ('1991-01-01 00:00:00')),PARTITION p1972 VALUES [('1991-01-01 00:00:00'), ('1999-01-01 00:00:00'))"]
  properties: [{"replication_num":"1", "in_memory": "true"}]
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

## Contributing
Welcome to contribute for dbt-starrocks. See [Contributing Guide](https://github.com/StarRocks/starrocks/blob/main/CONTRIBUTING.md) for more information.