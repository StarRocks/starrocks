# 使用 DataX 导入

本文介绍如何利用 DataX 基于 StarRocks 开发的 StarRocks Writer 插件将 MySQL、Oracle 等数据库中的数据导入至 StarRocks。该插件将数据转化为 CSV 或 JSON 格式并将其通过 [Stream Load](./StreamLoad.md) 方式批量导入至 StarRocks。

DataX 导入支持多种数据源，您可以参考 [DataX - Support Data Channels](https://github.com/alibaba/DataX#support-data-channels) 了解详情。

## 前提条件

使用 DataX 导入数据前，请确保已安装以下依赖：

- [DataX](https://github.com/alibaba/DataX/releases)
- [Python](https://www.python.org/downloads/)

## 安装 StarRocks Writer

[下载](https://github.com/StarRocks/DataX/releases) StarRocks Writer 安装包，然后运行如下命令将安装包解压至 **datax/plugin/writer** 路径下。

```shell
tar -xzvf starrockswriter.tar.gz
```

您也可以编译 StarRocks Writer [源码](https://github.com/StarRocks/DataX)。

## 创建配置文件

为导入作业创建 JSON 格式配置文件。

以下示例模拟 MySQL 至 StarRocks 导入作业的配置文件。您可以根据实际使用场景修改相应参数。

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
                                     "jdbc:mysql://x.x.x.x:3306/datax_test1"
                                ]
                            },
                            {
                                "table": [ "table3", "table4" ],
                                "jdbcUrl": [
                                     "jdbc:mysql://x.x.x.x:3306/datax_test2"
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
                        "jdbcUrl": "jdbc:mysql://y.y.y.y:9030/",
                        "loadUrl": ["y.y.y.y:8030", "y.y.y.y:8030"],
                        "loadProps": {}
                    }
                }
            }
        ]
    }
}
```

您需要在 `writer` 部分配置以下参数：

| **参数**      | **说明**                                                     | **必选** | **默认值** |
| ------------- | ------------------------------------------------------------ | -------- | ---------- |
| username      | StarRocks 集群用户名。<br />导入操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。                                       | 是       | 无         |
| password      | StarRocks 集群用户密码。                                     | 是       | 无         |
| database      | StarRocks 目标数据库名称。                                   | 是       | 无         |
| table         | StarRocks 目标表名称。                                       | 是       | 无         |
| loadUrl       | StarRocks FE 节点的地址，格式为 `"fe_ip:fe_http_port"`。多个地址使用逗号（,）分隔。 | 是       | 无         |
| column        | 目标表需要写入数据的列，多列使用逗号（,）分隔。例如: `"column": ["id","name","age"]`。该参数的对应关系与列名无关，但与其顺序一一对应。建议与 reader 中的 column 一样。 | 是       | 无         |
| preSql        | 标准 SQL 语句，在数据写入到目标表**前**执行。                | 否       | 无         |
| postSql       | 标准 SQL 语句，在数据写入到目标表**后**执行。                | 否       | 无         |
| jdbcUrl       | 目标数据库的 JDBC 连接信息，用于执行 `preSql` 及 `postSql`。 | 否       | 无         |
| maxBatchRows  | 单次 Stream Load 导入的最大行数。导入大量数据时，StarRocks Writer 将根据 `maxBatchRows` 或 `maxBatchSize` 将数据分为多个 Stream Load 作业分批导入。 | 否       | 500000     |
| maxBatchSize  | 单次 Stream Load 导入的最大字节数，单位为 Byte。导入大量数据时，StarRocks Writer 将根据 `maxBatchRows` 或 `maxBatchSize` 将数据分为多个 Stream Load 作业分批导入。 | 否       | 104857600  |
| flushInterval | 上一次 Stream Load 结束至下一次开始的时间间隔，单位为 ms。   | 否       | 300000     |
| loadProps     | Stream Load 的导入参数，详情参考 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。 | 否       | 无         |

> 注意
>
> - `column` 参数**不能留空**。如果您希望导入所有列，可以配置该项为 `"*"`。
> - `writer` 部分中 `column` 的**数量**必须与 `reader` 部分一致。

### 配置特殊参数

- **设置分隔符**

默认设置下，数据会被转化为字符串，以 CSV 格式通过 Stream Load 导入至 StarRocks。字符串以 `\t` 作为列分隔符，`\n` 作为行分隔符。

您可以通过在参数 `loadProps` 中添加以下配置，以更改分隔符：

```json
"loadProps": {
    "column_separator": "\\x01",
    "row_delimiter": "\\x02"
}
```

其中，`\x` 代表 16 进制，额外的 `\` 做转义 `x` 用。

> 说明
>
> StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符。

- **设置导入格式**

当前导入方式的默认导入格式为 CSV。您可以通过在参数 `loadProps` 中添加以下配置更改数据格式为 JSON：

```json
"loadProps": {
    "format": "json",
    "strip_outer_array": true
}
```

- **导入衍生列**

如果您希望在向 StarRocks 表中导入原始数据的同时导入衍生列，则需要在 `loadProps` 里额外设置 `columns` 参数。

以下代码片段展示导入原始数据列 `a`、`b` 以及衍生列 `c`。

```json
"writer": {
    "column": ["a", "b"],
    "loadProps": {
        "columns": "a,b,c=murmur_hash3_32(a)"
    }
    ...
}
```

其中，您需要在 `column` 项中填写原始数据中的列名，并在 `columns` 项中额外填写衍生列名和其对应的数据处理方式。

## 启动导入任务

完成配置文件设定后，您需要通过命令行启动导入任务。

以下示例基于前述步骤中的配置文件 **job.json** 启动导入任务，并设置了 JVM 调优参数（`--jvm="-Xms6G -Xmx6G"`）以及日志等级（`--loglevel=debug`）。您可以根据实际使用场景修改相应参数。

```shell
python datax/bin/datax.py --jvm="-Xms6G -Xmx6G" --loglevel=debug datax/job/job.json
```

如果源数据库与目标数据库时区不同，您需要命令行中添加 `-Duser.timezone=GMTxxx` 选项设置源数据库的时区信息。

例如，源库使用 UTC 时区，则启动任务时需添加参数 `-Duser.timezone=GMT+0`。

## 查看导入任务状态

由于 DataX 导入是基于封装的 Stream Load 实现，您可以在 `datax/log/$date/` 目录下搜索对应的导入作业日志，日志文件名字中包含导入使用的 JSON 文件名和任务启动时间（小时、分钟、秒），例如：**t_datax_job_job_json-20_52_19.196.log**。

- 如果日志中有 "http://fe_ip:fe_http_port/api/db_name/tbl_name/_stream_load" 记录生成，则表示 Stream Load 作业已成功触发。作业成功触发后，您可参考 [Stream Load 返回值](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#返回值) 查看任务情况。
- 日志中如果没有上述信息，请根据报错信息排查问题，或者在 [DataX 社区问题](https://github.com/alibaba/DataX/issues)中寻找解决方案。

> 注意
>
> 使用 DataX 导入时，当存在不符合目标表格式的数据，若 StarRocks 中该字段允许 NULL 值，不合法的数据将转为 NULL 后正常导入。若字段不允许 NULL 值，且任务中未配置容错率（max_filter_ratio），则任务报错，导入作业中断。

## 取消或停止导入任务

您可以通过终止 DataX 的 Python 进程停止导入任务。

```shell
pkill datax
```
