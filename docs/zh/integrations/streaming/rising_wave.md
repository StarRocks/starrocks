---
displayed_sidebar: docs
---

# 从 RisingWave 持续导入

[RisingWave](https://docs.risingwave.com/) 是 Apache 2.0 协议开源的分布式流数据库。旨在让用户以操作传统数据库的方式来处理流数据。通过创建实时物化视图，RisingWave 可以让用户轻松编写流计算逻辑。用户可以访问物化视图来对流计算结果进行及时、一致的查询。想快速上手 RisingWave 可参考[此文档](https://docs.risingwave.com/docs/current/get-started/)。

RisingWave 提供了直连的 StarRocks Sink 功能，可用于将数据导入至 StarRocks 表，不依赖第三方组件。该功能支持 StarRocks 全部 4 种表类型：明细表、聚合表、更新表和主键表。

## 前提条件

- 请确保您的 RisingWave 集群升级至 v1.7 或以上。v1.7 版本对 StarRocks Sink 进行了优化。
- 推荐使用 StarRocks v2.5 及以上版本。更早版本未经过充分测试，如遇到稳定性问题可以通过 [Github Issues](https://github.com/risingwavelabs/risingwave/issues) 进行反馈。
- 使用 RisingWave connector 导入数据至 StarRocks 需要目标表的 SELECT 和 INSERT 权限。如果您的用户账号没有这些权限，请参考 [GRANT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/GRANT/) 给用户赋权。

:::tip
RisingWave 目前对 StarRocks Sink 仅支持 At-least-once 语义，这意味着在故障发生时，数据可能被重复写入。使用 [StarRocks 主键表](https://docs.starrocks.io/zh/docs/table_design/table_types/primary_key_table/)可对数据进行去重，实现端到端的幂等写。

:::

## 参数说明

下表解释了 RisingWave 向 StarRocks Sink 数据时需要配置的参数。若未特殊标识，所有的参数都默认为必填。

| 参数名                                                       | 描述                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| connector                                                    | 固定设置为 starrocks 。                                      |
| starrocks.host                                               | StarRocks FE 节点的 IP 地址。                                |
| starrocks.query_port                                         | FE 节点的 MySQL 接口所用端口。                               |
| starrocks.http_port                                          | FE 节点的 HTTP 接口所用端口。                                |
| starrocks.user                                               | 用于访问 StarRocks 数据库的用户名。                          |
| starrocks.password                                           | 用户名所对应的密码。                                         |
| starrocks.database                                           | StarRocks 目标表所在的数据库名。                             |
| starrocks.table                                              | StarRocks 目标表。                                           |
| starrocks.partial_update                                     | （选填）是否启用部分列更新的优化。在更新列较少时，启用该选项可以提高导入性能。 |
| type                                                         | 输出数据类型：<ul><li>`append-only`: 只会输出 Insert 操作。</li><li>`upsert`: 输出变更操作流。在此类型下，StarRocks 目标表必须为主键表。</li></ul>            |
| force_append_only                                            | （选填）当输出类型为 append-only 而 Sink 实际有可能输出 upsert/delete 变更时，将 upsert 和 delete 数据丢弃，强行让 Sink 输出 append-only 流。 |
| primary_key                                                  | （选填）StarRocks 表的主键。当 `type` 为 `upsert` 时，需要填入该项。 |

## 数据类型映射

| RisingWave 类型                                       | StarRocks 类型 |
| ----------------------------------------------------- | -------------- |
| BOOLEAN                                               | BOOLEAN        |
| SMALLINT                                              | SMALLINT       |
| INTEGER                                               | INT            |
| BIGINT                                                | BIGINT         |
| REAL                                                  | FLOAT          |
| DOUBLE                                                | DOUBLE         |
| DECIMAL                                               | DECIMAL        |
| DATE                                                  | DATE           |
| VARCHAR                                               | VARCHAR        |
| TIME（建议预先转类型为 VARCHAR）                         | 不支持         |
| TIMESTAMP                                             | DATETIME       |
| TIMESTAMP WITH TIME ZONE（建议预先转类型为 TIMESTAMP）     | 不支持         |
| INTERVAL（建议预先转类型为 VARCHAR）                      | 不支持         |
| STRUCT                                                | JSON           |
| ARRAY                                                 | ARRAY          |
| BYTEA（建议预先转类型为 VARCHAR）                     | 不支持         |
| JSONB                                                 | JSON           |
| SERIAL                                                | BIGINT         |

## 使用示例

1. 在 StarRocks 上创建数据库 `demo`，并创建主键表 `score_board`。

   ```sql
   CREATE DATABASE demo;
   USE demo;

   CREATE TABLE demo.score_board(
       id int(11) NOT NULL COMMENT "",
       name varchar(65533) NULL DEFAULT "" COMMENT "",
       score int(11) NOT NULL DEFAULT "0" COMMENT ""
   )
   PRIMARY KEY(id)
   DISTRIBUTED BY HASH(id);
   ```

2. 使用 RisingWave 向 StarRocks 表写入数据：

   ```sql
   -- 创建一张 RisingWave 表。
   CREATE TABLE score_board (
       id INT PRIMARY KEY,
       name VARCHAR,
       score INT
   );
   
   -- 向表中插入数据。
   INSERT INTO score_board VALUES (1, 'starrocks', 100), (2, 'risingwave', 100);

   -- 将表中的数据 Sink 到 StarRocks 表中。
   CREATE SINK score_board_sink
   FROM score_board WITH (
       connector = 'starrocks',
       type = 'upsert',
       starrocks.host = 'starrocks-fe',
       starrocks.mysqlport = '9030',
       starrocks.httpport = '8030',
       starrocks.user = 'users',
       starrocks.password = '123456',
       starrocks.database = 'demo',
       starrocks.table = 'score_board',
         primary_key = 'id'
   );
   ```
