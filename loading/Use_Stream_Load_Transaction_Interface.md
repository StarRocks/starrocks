# 使用 Stream Load 事务接口导入

为了支持和其他系统（如 Apache Flink® 和 Apache Kafka®）之间实现跨系统的两阶段提交，并提升高并发 Stream Load 导入场景下的性能，StarRocks 提供了 Stream Load 事务接口。您可以选择使用事务接口来执行 Stream Load 导入。

## 接口能力

### 事务去重

复用 StarRocks 现有的标签机制，通过标签绑定事务，实现事务的 “至多一次 (At-Most-Once)” 语义。

### 事务回滚

在写入数据失败时，事务将自动回滚。您也可以通过 `rollback` 接口手动回滚事务。

### 事务超时管理

使用 FE 配置中的 `stream_load_default_timeout_second` 参数设置默认的事务超时时间。

开启事务时，可以通过 HTTP 请求头中的 `timeout` 字段来指定当前事务的超时时间。

开启事务时，还可以通过 HTTP 请求头中的 `idle_transaction_timeout` 字段来指定空闲事务超时时间。当事务超过 `idle_transaction_timeout` 所设置的超时时间而没有数据写入时，事务将自动回滚。

## 接口优势

- **减少内存使用**
  通过分开“发送数据”和“提交事务”，客户端不再需要先缓存完整的一个批次数据后再提交事务，而是可以边接收上游数据边“发送数据”，在合适的时候“提交事务”以完成一个批次的导入。这样可以减少客户端的内存使用，在 Apache Flink® 实现“精确一次 (Exactly-Once)”语义的导入中尤为明显。

- **提升导入性能**
  在通过程序提交 Stream Load 作业的场景中，新接口允许一个导入作业中按需合并发送多个小文件数据后“提交事务”，从而能减少导入的版本数量，提升导入性能。

## 使用限制

事务接口当前只支持**单表**事务，未来将会支持多表事务。

## 基本操作

Stream Load 事务接口当前只支持 HTTP 协议。您可以使用该事务接口执行如下操作：

### 开始事务

```PowerShell
# 开始事务。
curl -H "label:${label}"
    -XPUT http://fe_host:http_port/api/{db}/transaction/begin
# 开始事务成功。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "OK",
    "BeginTxnTimeMs": 173
}

# 标签重复。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Label Already Exist",
    "Message": ""
}

# 其他失败错误。
{
    "Status": "Fail",
    "Message": ""
}
```

### 发送数据

```PowerShell
# 可发送多次。
# 发送数据。
curl -H "label:${label}" 
    -T /path/to/data.csv
    -XPUT http://fe_host:http_port/api/{db}/transaction/{table}/stream_load
 
# 发送数据成功。   
{
    "TxnId": 1,
    "Seq": 0,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 5265644,
    "NumberLoadedRows": 5265644,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 10737418067,
    "LoadTimeMs": 418778,
    "StreamLoadPutTimeMs": 68,
    "ReceivedDataTimeMs": 38964,
}

# 未知的事务。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation Not Exist",
    "Message": ""
}

# 事务状态无效。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation State Invalid",
    "Message": ""
}

# 其他失败错误。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Fail",
    "Message": ""
}
```

### 提交事务

```PowerShell
# 提交事务。  
curl -H "label:${label}"
    -XPUT http://fe_host:http_port/api/{db}/transaction/commit
# 提交事务成功。  
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "OK",
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

# 提交事务成功。   
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "Transaction already commited",
}

# 事务不存在。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation Not Exist",
    "Message": ""
}

# 提交事务超时。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Commit Timeout",
    "Message": "",
}

# 发布超时信息。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Publish Timeout",
    "Message": "",
    "CommitAndPublishTimeMs": 1393
}

# 其他失败错误。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Fail",
    "Message": ""
}
```

### 回滚事务

```PowerShell
# 终止事务。 
curl -H "label:${label}"
    -XPUT http://fe_host:http_port/api/{db}/transaction/rollback
    
# 终止事务成功。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "OK"
}

# 事务不存在。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation Not Exist",
    "Message": ""
}

# 其他失败错误。
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Fail",
    "Message": ""
}
```
