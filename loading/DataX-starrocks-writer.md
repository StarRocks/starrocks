# 使用 DataX 导入 StarRocks

本文介绍如何利用 DataX 基于 StarRocks 开发的 StarRocks Writer 插件将 MySQL、Oracle 等数据库中的数据导入至 StarRocks。该插件将数据转化为 CSV 或 JSON 格式并将其通过 [Stream Load](./StreamLoad.md) 方式批量导入至 StarRocks。

## 支持的数据源

* MySQL
* Oracle
* [更多](https://github.com/alibaba/DataX)

## 使用场景

Mysql、Oracle 等 DataX 支持读取的数据库**全量数据**通过 DataX 和 starrockswriter 导入到 StarRocks。

## 实现原理

StarRocksWriter 插件实现了写入数据到 StarRocks 的目的表的功能。在底层实现上，StarRocksWriter 通过Stream load以csv或 json 格式导入数据至StarRocks。内部将`reader`读取的数据进行缓存后批量导入至StarRocks，以提高写入性能。总体数据流是 `source -> Reader -> DataX channel -> Writer -> StarRocks`。

## 使用步骤

### 环境准备

[点击下载插件](https://github.com/StarRocks/DataX/releases)

[源码地址](https://github.com/StarRocks/DataX)

解压 starrockswriter 插件并放置在 datax/plugin/writer目录下。

### 配置样例

> 以下举例从 MySQL 读取数据后导入至 StarRocks 时 datax 的配置。

~~~json
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
                        "loadUrl": ["172.28.17.100:8030", "172.28.17.101:8030"],
                        "loadProps": {}
                    }
                }
            }
        ]
    }
}

~~~

**参数说明**

* **username**

  * 描述：StarRocks 数据库的用户名

  * 必选：是

  * 默认值：无

* **password**

  * 描述：StarRocks 数据库的密码

  * 必选：是

  * 默认值：无

* **database**

  * 描述：StarRocks 表的数据库名称。

  * 必选：是

  * 默认值：无

* **table**

  * 描述：StarRocks 表的表名称。

  * 必选：是

  * 默认值：无

* **loadUrl**

  * 描述：StarRocks FE的地址用于Streamload，可以为多个fe地址，形如`fe_ip:fe_http_port`。

  * 必选：是

  * 默认值：无

* **column**

  * 描述：目的表需要写入数据的字段，字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。

   **column配置项必须指定，不能留空！**

  > 如果希望导入所有字段，可以使用 ["*"]

  * 必选：是

  * 默认值：否

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。

  * 必选：否

  * 默认值：无

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。

  * 必选：否

  * 默认值：无

* **jdbcUrl**

  * 描述：目的数据库的 JDBC 连接信息，用于执行 `preSql` 及 `postSql`。

  * 必选：否

  * 默认值：无

* **maxBatchRows**

  * 描述：单次StreamLoad导入的最大行数

  * 必选：否

  * 默认值：500000 (50W)

* **maxBatchSize**

  * 描述：单次StreamLoad导入的最大字节数。

  * 必选：否

  * 默认值：104857600 (100M)

* **flushInterval**

  * 描述：上一次 StreamLoad 结束至下一次开始的时间间隔（单位：ms）。

  * 必选：否

  * 默认值：300000 (ms)

* **loadProps**

  * 描述：StreamLoad 的请求参数，详情参照 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md) 介绍页面。

  * 必选：否

  * 默认值：无

### 启动任务

>下面 job.json 文件是用于调试 DataX 环境，实际文件内容可根据需求命名即可，比如 job_starrocks.json，内容参考[配置样例](#配置样例)。

~~~bash
  python bin/datax.py --jvm="-Xms6G -Xmx6G" --loglevel=debug job/job.json
~~~

### 查看导入任务状态

DataX 导入是封装的 Stream Load 实现的，可以在 `datax/log/$date/` 目录下搜索对应的job日志，日志文件名字中包含上文命名的json文件名和任务启动的小时分钟秒，例如：t_datax_job_job_json-20_52_19.196.log，

* 日志中如果有 `http://$fe:${http_port}/api/$db/$tbl/_stream_load` 生成，表示成功触发了 Stream Load 任务，任务结果可参考 [Stream Load 任务状态](../loading/StreamLoad.md#创建导入作业)。

* 日志中如果没有上述信息，请参考报错提示排查，或者在 [DataX 社区问题](https://github.com/alibaba/DataX/issues)查找。

### 取消或停止导入任务

DataX 导入启动的是一个 python 进程，如果要取消或者停止导入任务，kill 掉进程即可。

## 注意事项

### 导入参数配置

默认传入的数据均会被转为字符串，并以`\t`作为列分隔符，`\n`作为行分隔符，组成`csv`文件进行StreamLoad导入操作。
如需更改列分隔符，则正确配置 `loadProps` 即可：

~~~json
"loadProps": {
    "column_separator": "\\x01",
    "row_delimiter": "\\x02"
}
~~~

如需更改导入格式为`json`，则正确配置 `loadProps` 即可：

~~~json
"loadProps": {
    "format": "json",
    "strip_outer_array": true
}
~~~

### 关于时区

源库与目标库时区不同时，执行datax.py，命令行后面需要加如下参数：

~~~json
"-Duser.timezone=xx时区"
~~~

例如，DataX导入PostgreSQL中的数据，源库是UTC时间，在dataX启动时加参数 "-Duser.timezone=GMT+0"。
