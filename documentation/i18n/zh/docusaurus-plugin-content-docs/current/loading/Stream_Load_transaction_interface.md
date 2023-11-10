---
displayed_sidebar: "Chinese"
---

# 使用 Stream Load 事务接口导入

为了支持和 Apache Flink®、Apache Kafka® 等其他系统之间实现跨系统的两阶段提交，并提升高并发 Stream Load 导入场景下的性能，StarRocks 自 2.4 版本起提供 Stream Load 事务接口。

本文介绍 Stream Load 事务接口、以及如何使用该事务接口把数据导入到 StarRocks 中。

> **注意**
>
> 导入操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。

## 接口说明

Stream Load 事务接口支持通过兼容 HTTP 协议的工具或语言发起接口请求。本文以 curl 工具为例介绍如何使用该接口。该接口提供事务管理、数据写入、事务预提交、事务去重和超时管理等功能。

### 事务管理

提供如下标准接口，用于管理事务：

- `/api/transaction/begin`：开启一个新事务。

- `/api/transaction/commit`：提交当前事务，持久化变更。

- `/api/transaction/rollback`：回滚当前事务，回滚变更。

### 事务预提交

提供 `/api/transaction/prepare` 接口，用于预提交当前事务，临时持久化变更。预提交一个事务后，您可以继续提交或者回滚该事务。这种机制下，如果在事务预提交成功以后 StarRocks 发生宕机，您仍然可以在系统恢复后继续执行提交。

> **说明**
>
> 在事务预提交以后，请勿继续写入数据。继续写入数据的话，写入请求会报错。

### 数据写入

提供 `/api/transaction/load` 接口，用于写入数据。您可以在同一个事务中多次调用该接口来写入数据。

### 事务去重

复用 StarRocks 现有的标签机制，通过标签绑定事务，实现事务的 “至多一次 (At-Most-Once)” 语义。

### 超时管理

支持通过 FE 配置中的 `stream_load_default_timeout_second` 参数设置默认的事务超时时间。

开启事务时，可以通过 HTTP 请求头中的 `timeout` 字段来指定当前事务的超时时间。

开启事务时，还可以通过 HTTP 请求头中的 `idle_transaction_timeout` 字段来指定空闲事务超时时间。当事务超过 `idle_transaction_timeout` 所设置的超时时间而没有数据写入时，事务将自动回滚。

## 接口优势

Stream Load 事务接口具有如下优势：

- **Exactly-Once 语义**

  通过“预提交事务”、“提交事务”，方便实现跨系统的两阶段提交。例如配合在 Flink 实现“精确一次 (Exactly-Once)”语义的导入。

- **提升导入性能**

  在通过程序提交 Stream Load 作业的场景中，Stream Load 事务接口允许在一个导入作业中按需合并发送多次小批量的数据后“提交事务”，从而能减少数据导入的版本，提升导入性能。

## 使用限制

事务接口当前具有如下使用限制：

- 只支持**单库单表**事务，未来将会支持**跨库多表**事务。

- 只支持**单客户端并发**数据写入，未来将会支持**多客户端并发**数据写入。

- 支持在单个事务中多次调用数据写入接口 `/api/transaction/load` 来写入数据，但是要求所有 `/api/transaction/load` 接口中的参数设置必须保持一致。

- 导入 CSV 格式的数据时，需要确保每行数据结尾都有行分隔符。

## 注意事项

- 使用 Stream Load 事务接口导入数据的过程中，注意 `/api/transaction/begin`、`/api/transaction/load`、`/api/transaction/prepare` 接口报错后，事务将失败并自动回滚。
- 在调用 `/api/transaction/begin` 接口开启事务时，您可以选择指定或者不指定标签 (Label)。如果您不指定标签，StarRocks 会自动为事务生成一个标签。其后的 `/api/transaction/load`、`/api/transaction/prepare`、`/api/transaction/commit` 三个接口中，必须使用与 `/api/transaction/begin` 接口中相同的标签。
- 重复调用标签相同的 `/api/transaction/begin` 接口，会导致前面使用相同标签已开启的事务失败并回滚。
- StarRocks支持导入的 CSV 格式数据默认的列分隔符是 `\t`，默认的行分隔符是 `\n`。如果源数据文件中的列分隔符和行分隔符不是 `\t` 和 `\n`，则在调用 `/api/transaction/load` 接口时必须通过 `"column_separator: <column_separator>"` 和 `"row_delimiter: <row_delimiter>"` 指定行分隔符和列分隔符。

## 基本操作

### 准备数据样例

这里以 CSV 格式的数据为例。

1. 在本地文件系统 `/home/disk1/` 路径下创建一个 CSV 格式的数据文件 `example1.csv`。文件一共包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

   ```Plain
   1,Lily,23
   2,Rose,23
   3,Alice,24
   4,Julia,25
   ```

2. 在数据库 `test_db` 中创建一张名为 `table1` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，主键为 `id` 列，如下所示：

   ```SQL
   CREATE TABLE `table1`
   (
       `id` int(11) NOT NULL COMMENT "用户 ID",
       `name` varchar(65533) NULL COMMENT "用户姓名",
       `score` int(11) NOT NULL COMMENT "用户得分"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

### 开始事务

#### 语法

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" -H "table:<table_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

#### 示例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" -H "table:table1" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

> **说明**
>
> 上述示例中，指定事务的标签为 `streamload_txn_example1_table1`。

#### 返回结果

- 如果事务开始成功，则返回如下结果：

  ```Bash
  {
      "Status": "OK",
      "Message": "",
      "Label": "streamload_txn_example1_table1",
      "TxnId": 9032,
      "BeginTxnTimeMs": 0
  }
  ```

- 如果事务的标签重复，则返回如下结果：

  ```Bash
  {
      "Status": "LABEL_ALREADY_EXISTS",
      "ExistingJobStatus": "RUNNING",
      "Message": "Label [streamload_txn_example1_table1] has already been used."
  }
  ```

- 如果发生标签重复以外的其他错误，则返回如下结果：

  ```Bash
  {
      "Status": "FAILED",
      "Message": ""
  }
  ```

### 写入数据

#### 语法

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" -H "table:<table_name>" \
    -T <file_path> \
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

> **说明**
>
> 调用 `/api/transaction/load` 接口时，必须通过 `-T <file_path>` 指定数据文件所在的路径。

#### 示例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" -H "table:table1" \
    -T /home/disk1/example1.csv \
    -H "column_separator: ," \
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

> **说明**
>
> 上述示例中，由于数据文件 `example1.csv` 中使用的列分隔符为逗号 (`,`)，而不是 StarRocks 默认的列分隔符 (`\t`)，因此在调用 `/api/transaction/load` 接口时必须通过 `"column_separator: <column_separator>"`  指定列分隔符为逗号 (`,`)。

#### 返回结果

- 如果数据写入成功，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Seq": 0,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "",
      "NumberTotalRows": 5265644,
      "NumberLoadedRows": 5265644,
      "NumberFilteredRows": 0,
      "NumberUnselectedRows": 0,
      "LoadBytes": 10737418067,
      "LoadTimeMs": 418778,
      "StreamLoadPutTimeMs": 68,
      "ReceivedDataTimeMs": 38964,
  }
  ```

- 如果事务被判定为未知，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "TXN_NOT_EXISTS"
  }
  ```

- 如果事务状态无效，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation State Invalid"
  }
  ```

- 如果发生事务未知和状态无效以外的其他错误，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### 预提交事务

#### 语法

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### 示例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### 返回结果

- 如果事务预提交成功，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "",
      "NumberTotalRows": 5265644,
      "NumberLoadedRows": 5265644,
      "NumberFilteredRows": 0,
      "NumberUnselectedRows": 0,
      "LoadBytes": 10737418067,
      "LoadTimeMs": 418778,
      "StreamLoadPutTimeMs": 68,
      "ReceivedDataTimeMs": 38964,
      "WriteDataTimeMs": 417851
      "CommitAndPublishTimeMs": 1393
  }
  ```

- 如果事务被判定为不存在，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 如果事务预提交超时，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- 如果发生事务不存在和预提交超时以外的其他错误，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "publish timeout"
  }
  ```

### 提交事务

#### 语法

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### 示例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### 返回结果

- 如果事务提交成功，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "",
      "NumberTotalRows": 5265644,
      "NumberLoadedRows": 5265644,
      "NumberFilteredRows": 0,
      "NumberUnselectedRows": 0,
      "LoadBytes": 10737418067,
      "LoadTimeMs": 418778,
      "StreamLoadPutTimeMs": 68,
      "ReceivedDataTimeMs": 38964,
      "WriteDataTimeMs": 417851
      "CommitAndPublishTimeMs": 1393
  }
  ```

- 如果事务已经提交过，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "Transaction already commited",
  }
  ```

- 如果事务被判定为不存在，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 如果事务提交超时，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- 如果数据发布超时，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "publish timeout",
      "CommitAndPublishTimeMs": 1393
  }
  ```

- 如果发生事务不存在和超时以外的其他错误，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### 回滚事务

#### 语法

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### 示例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### 返回结果

- 如果事务回滚成功，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": ""
  }
  ```

- 如果事务被判定为不存在，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 如果发生事务不存在以外的其他错误，则返回如下结果：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

## 相关文档

有关 Stream Load 适用的业务场景、支持的数据文件格式、基本原理等信息，参见[通过 HTTP PUT 从本地文件系统或流式数据源导入数据](../loading/StreamLoad.md)。

有关创建 Stream Load 作业的语法和参数，参见[STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。
