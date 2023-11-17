---
displayed_sidebar: "English"
---

# DataX writer

## Introduction

The StarRocksWriter plugin allows writing data to StarRocks' destination table. Specifically,StarRocksWriter imports data to StarRocks in CSV or JSON format via [Stream Load](./StreamLoad.md), and internally caches and bulk imports the data read by `reader` to StarRocks for better write performance. The overall data flow is `source -> Reader -> DataX channel -> Writer -> StarRocks`.

[Download the plugin](https://github.com/StarRocks/DataX/releases)

Please go to `https://github.com/alibaba/DataX` to download the full package of DataX, and put the starrockswriter plugin into the `datax/plugin/writer/` directory.

Use the following command to test:
`python datax.py --jvm="-Xms6G -Xmx6G" --loglevel=debug job.json`

## Function Description

### Sample Configuration

Here is a configuration file for reading data from MySQL and loading it to StarRocks.

```json
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "column": [ "k1", "k2", "v1", "v2" ],
                        "connection": [
                            {
                                "table": [ "table1", "table2" ],
                                "jdbcUrl": [
                                     "jdbc:mysql://127.0.0.1:3306/datax_test1"
                                ]
                            },
                            {
                                "table": [ "table3", "table4" ],
                                "jdbcUrl": [
                                     "jdbc:mysql://127.0.0.1:3306/datax_test2"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "starrockswriter",
                    "parameter": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "database": "xxxx",
                        "table": "xxxx",
                        "column": ["k1", "k2", "v1", "v2"],
                        "preSql": [],
                        "postSql": [], 
                        "jdbcUrl": "jdbc:mysql://172.28.17.100:9030/",
                        "loadUrl": ["172.28.17.100:8030", "172.28.17.100:8030"],
                        "loadProps": {}
                    }
                }
            }
        ]
    }
}

```

## Starrockswriter Parameter Description

* **username**

  * Description: The user name of the StarRocks database

  * Required: Yes

  * Default value: none

* **password**

  * Description: The password for the StarRocks database

  * Required: Yes

  * Default: None

* **database**

  * Description: The name of the database for the StarRocks table.

  * Required: Yes

  * Default: None

* **table**

  * Description: The name of the table for the StarRocks table.

  * Required: Yes

  * Default: None

* **loadUrl**

  * Description: The address of the StarRocks FE for stream load, can be multiple FE addresses, in the form of `fe_ip:fe_http_port`.

  * Required: yes

  * Default value: none

* **column**

  * Description: The fields of the destination table **that need to be written to the data**, with the columns separated by commas. Example: "column": ["id", "name", "age"].
    >**column configuration item must be specified and cannot be left blank.**
    >Note: We strongly discourage you from leaving it empty, because your job may run incorrectly or fail when you change the number of columns, type, etc. of the destination table. The configuration items      must be in the same order as the querySQL or column in the reader.

* Required: Yes

* Default value: No

* **preSql**

* Description: The standard statement will be executed before writing data to the destination table.

* Required: No

* Default: No

* **jdbcUrl**

  * Description: JDBC connection information of the destination database for executing `preSql` and `postSql`.
  
  * Required: No

* Default: No

* **loadProps**

  * Description: Request parameters for StreamLoad, refer to the StreamLoad introduction page for details.

  * Required: No

  * Default value: No

## Type conversion

By default, incoming data is converted to strings, with `t` as column separator and `n` as row separator, to form `csv` files for StreamLoad import     .
To change the column separator, configure `loadProps` properly.

```json
"loadProps": {
    "column_separator": "\\x01",
    "row_delimiter": "\\x02" 
}
```

To change the import format to `json`, configure `loadProps` properly.

```json
"loadProps": {
    "format": "json",
    "strip_outer_array": true
}
```

> The `json` format is for the writer to import data to StarRocks in JSON format.

## About time zone

If the source tp library is in another time zone, when executing datax.py, add the following parameter after the command line

```json
"-Duser.timezone=xx"
```

e.g. If DataX imports Postgrest data and the source library is in UTC time, add the parameter "-Duser.timezone=GMT+0" to startup.
