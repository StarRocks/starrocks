# 使用 Stream Load 事务接口导入

为了支持和 Apache Flink®、Apache Kafka® 等其他系统之间实现跨系统的两阶段提交，并提升高并发 Stream Load 导入场景下的性能，StarRocks 提供了 Stream Load 事务接口。

本文介绍 Stream Load 事务接口、以及如何使用该事务接口把数据导入到 StarRocks 中。

## 接口说明

Stream Load 事务接口支持通过兼容 HTTP 协议的工具或语言发起接口操作请求。本文以 curl 工具为例介绍如何使用该接口。该接口提供事务管理、数据写入、事务预提交、事务去重和超时管理等功能。

### 事务管理

提供如下标准接口操作，用于管理事务：

- `/api/transaction/begin`：开启一个新事务。

- `/api/transaction/commit`：提交当前事务，持久化变更。

- `/api/transaction/rollback`：回滚当前事务，回滚变更。

### 事务预提交

提供 `/api/transaction/prepare` 接口操作，用于预提交当前事务，临时持久化变更。预提交一个事务后，您可以继续提交或者回滚该事务。这种机制下，如果在事务预提交成功以后 StarRocks 发生宕机，您仍然可以在系统恢复后继续执行提交。

> **说明**
>
> 在事务预提交以后，请勿继续写入数据。继续写入数据的话，写入请求会报错。

### 数据写入

提供 `/api/transaction/load` 接口操作，用于写入数据。您可以在同一个事务中多次调用该接口来写入数据。

### 事务去重

复用 StarRocks 现有的标签机制，通过标签绑定事务，实现事务的 “至多一次 (At-Most-Once)” 语义。

### 超时管理

支持通过 FE 配置中的 `stream_load_default_timeout_second` 参数设置默认的事务超时时间。

开启事务时，可以通过 HTTP 请求头中的 `timeout` 字段来指定当前事务的超时时间。

开启事务时，还可以通过 HTTP 请求头中的 `idle_transaction_timeout` 字段来指定空闲事务超时时间。当事务超过 `idle_transaction_timeout` 所设置的超时时间而没有数据写入时，事务将自动回滚。

## 接口优势

Stream Load 事务接口具有如下优势：

- **精确一次语义**

  通过“预提交事务”、“提交事务”，方便实现跨系统的两阶段提交。例如配合在 Flink 实现“精确一次 (Exactly-Once)”语义的导入。

- **提升导入性能**

  在通过程序提交 Stream Load 作业的场景中，Stream Load 事务接口允许在一个导入作业中按需合并发送多次小批量的数据后“提交事务”，从而能减少数据导入的版本，提升导入性能。

## 使用限制

事务接口当前具有如下使用限制：

- 只支持**单库****单表**事务，未来将会支持**跨库****多表**事务。

- 只支持**单****客户端****并发**数据写入，未来将会支持**多****客户端****并发**数据写入。

- 支持在单个事务中多次调用数据写入接口 `/api/transaction/load` 来写入数据，但是要求所有 `/api/transaction/load` 操作中的参数设置必须保持一致。

## 基本操作

### 开始事务

#### 语法

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>" -H "table:<table_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

#### 返回结果

- 如果事务开始成功，则返回如下结果：

  ```PowerShell
  {
      "Status": "OK",
      "Message": "",
      "Label": "xxx",
      "TxnId": 9032,
      "BeginTxnTimeMs": 0
  }
  ```

- 如果事务的标签重复，则返回如下结果：

  ```PowerShell
  {
      "Status": "LABEL_ALREADY_EXISTS",
      "ExistingJobStatus": "RUNNING",
      "Message": "Label [xxx] has already been used."
  }
  ```

- 如果发生标签重复以外的其他错误，则返回如下结果：

  ```PowerShell
  {
      "Status": "FAILED",
      "Message": ""
  }
  ```

### 写入数据

#### 语法

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>" -H "table:<table_name>"
    -T /path/to/data.csv
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

#### 返回结果

- 如果数据写入成功，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Seq": 0,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
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

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "TXN_NOT_EXISTS"
  }
  ```

- 如果事务状态无效，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation State Invalid"
  }
  ```

- 如果发生事务未知和状态无效以外的其他错误，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### 预提交事务

#### 语法

```PowerShell
 curl -H "label:<label_name>" -H "db:<database_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### 返回结果

- 如果事务预提交成功，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
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

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 如果事务预提交超时，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- 如果发生事务不存在和预提交超时以外的其他错误，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "publish timeout"
  }
  ```

### 提交事务

#### 语法

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### 返回结果

- 如果事务提交成功，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
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

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "OK",
      "Message": "Transaction already commited",
  }
  ```

- 如果事务被判定为不存在，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 如果事务提交超时，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- 如果数据发布超时，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "publish timeout",
      "CommitAndPublishTimeMs": 1393
  }
  ```

- 如果发生事务不存在和超时以外的其他错误，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### 回滚事务

#### 语法

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### 返回结果

- 如果事务回滚成功，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "OK",
      "Message": ""
  }
  ```

- 如果事务被判定为不存在，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 如果发生事务不存在以外的其他错误，则返回如下结果：

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": ""
  }
  ```
  