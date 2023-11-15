# Load data by using Stream Load transaction interface

StarRocks provides a Stream Load transaction interface to implement two-phase commit (2PC) of transactions that are run to stream data from external systems such as Apache Flink® and Apache Kafka®. The Stream Load transaction interface helps improve the performance of highly concurrent stream loads. You can optionally run stream load jobs by using the Stream Load transaction interface.

## Capabilities

### Transaction deduplication

The Stream Load transaction interface carries over the labeling mechanism of StarRocks. You can bind a unique label to each transaction to achieve at-most-once guarantees for transactions.

### Transaction rollback

If data writes within a transaction fail, the transaction automatically rolls back. You can also call the `rollback` operation to roll back the transaction.

### Transaction timeout management

You can use the `stream_load_default_timeout_second` parameter in the configuration file of each frontend (FE) to specify a default transaction timeout period for that FE.

When you create a transaction, you can use the `timeout` field in the HTTP request header to specify a timeout period for the transaction.

When you create a transaction, you can also use the `idle_transaction_timeout` field in the HTTP request header to specify a timeout period within which the transaction can stay idle. If no data is written within the timeout period, the transaction automatically rolls back.

## Benefits

The Stream Load transaction interface brings the following benefits:

- **Reduced memory usage**
  You can use the Stream Load transaction interface to send data and commit transactions as separate operations. As such, you no longer need to cache a complete batch of data on your client before you commit your transaction. Instead, you can keep receiving upstream data while sending each group of received data separately, and then commit your transaction at a proper time to load all received data as a single batch. This way, memory usage on your client is reduced. Memory usage reduction is especially significant when you run a load job exactly once to load data from Apache Flink®.

- **Improved load performance**
  When you invoke a program to run a stream load job, the Stream Load transaction interface allows you to send multiple small files at a time and then commit your transaction. This way, fewer data versions are generated, and load performance is improved.

## Limits

The Stream Load transaction interface supports only single-table transactions. Multi-table transactions are under development.

## Basic operations

The Stream Load transaction interface supports only the HTTP protocol. You can use the transaction interface to perform the following operations:

### Start a transaction

```PowerShell
# Start a transaction.
curl -H "label:${label}"
    -XPUT http://fe_host:http_port/api/{db}/transaction/begin
# The transaction is successfully started.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "OK",
    "BeginTxnTimeMs": 173
}

# The transaction is bound to duplicate labels.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Label Already Exist",
    "Message": ""
}

# The transaction cannot be started due to other errors.
{
    "Status": "Fail",
    "Message": ""
}
```

### Send data

```PowerShell
# You can send data multiple times.
# Send data.
curl -H "label:${label}" 
    -T /path/to/data.csv
    -XPUT http://fe_host:http_port/api/{db}/transaction/{table}/stream_load
 
# The data is successfully sent.   
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

# The transaction is unknown.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation Not Exist",
    "Message": ""
}

# The transaction is in an invalid state.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation State Invalid",
    "Message": ""
}

# Data cannot be sent in the transaction due to other errors.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Fail",
    "Message": ""
}
```

### Commit a transaction

```PowerShell
# Commit a transaction.
curl -H "label:${label}"
    -XPUT http://fe_host:http_port/api/{db}/transaction/commit
# The transaction is successfully committed.  
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

# The transaction is successfully committed.   
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "Transaction already commited",
}

# The transaction cannot be found.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation Not Exist",
    "Message": ""
}

# The commit of the transaction times out.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Commit Timeout",
    "Message": "",
}

# The publishing times out.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Publish Timeout",
    "Message": "",
    "CommitAndPublishTimeMs": 1393
}

# The transaction cannot be committed due to other errors.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Fail",
    "Message": ""
}
```

### Roll back a transaction

```PowerShell
# Abort a transaction.
curl -H "label:${label}"
    -XPUT http://fe_host:http_port/api/{db}/transaction/rollback
    
# The transaction is successfully aborted.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Success",
    "Message": "OK"
}

# The transaction cannot be found.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Transcation Not Exist",
    "Message": ""
}

# The transaction cannot be aborted due to other errors.
{
    "TxnId": 1,
    "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
    "Status": "Fail",
    "Message": ""
}
```
