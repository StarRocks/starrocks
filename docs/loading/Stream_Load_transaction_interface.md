# Load data using Stream Load transaction interface

StarRocks provides a Stream Load transaction interface to implement two-phase commit (2PC) for transactions that are run to load data from external systems such as Apache Flink® and Apache Kafka®. The Stream Load transaction interface helps improve the performance of highly concurrent stream loads.

This topic describes the Stream Load transaction interface and how to load data into StarRocks by using this interface.

## Description

The Stream Load transaction interface supports using an HTTP protocol-compatible tool or language to call API operations. This topic uses curl as an example to explain how to use this interface. This interface provides various features, such as transaction management, data write, transaction pre-commit, transaction deduplication, and transaction timeout management.

### Transaction management

The Stream Load transaction interface provides the following API operations, which are used to manage transactions:

- `/api/transaction/begin`: starts a new transaction.

- `/api/transaction/commit`: commits the current transaction to make data changes persistent.

- `/api/transaction/rollback`: rolls back the current transaction to abort data changes.

### Transaction pre-commit

The Stream Load transaction interface provides the `/api/transaction/prepare` operation, which is used to pre-commit the current transaction and make data changes temporarily persistent. After you pre-commit a transaction, you can proceed to commit or roll back the transaction. If your StarRocks cluster breaks down after a transaction is pre-committed, you can still proceed to commit the transaction after your StarRocks cluster is restored to normal.

> **NOTE**
>
> After the transaction is pre-committed, do not continue to write data using the transaction. If you continue to write data using the transaction, your write request returns errors.

### Data write

The Stream Load transaction interface provides the `/api/transaction/load` operation, which is used to write data. You can call this operation multiple times within one transaction.

### Transaction deduplication

The Stream Load transaction interface carries over the labeling mechanism of StarRocks. You can bind a unique label to each transaction to achieve at-most-once guarantees for transactions.

### Transaction timeout management

You can use the `stream_load_default_timeout_second` parameter in the configuration file of each FE to specify a default transaction timeout period for that FE.

When you create a transaction, you can use the `timeout` field in the HTTP request header to specify a timeout period for the transaction.

When you create a transaction, you can also use the `idle_transaction_timeout` field in the HTTP request header to specify a timeout period within which the transaction can stay idle. If no data is written within the timeout period, the transaction automatically rolls back.

## Benefits

The Stream Load transaction interface brings the following benefits:

- **Exactly-once semantics**

  A transaction is split into two phases, pre-commit and commit, which make it easy to load data across systems. For example, this interface can guarantee exactly-once semantics for data loads from Flink.

- **Improved load performance**

  If you run a load job by using a program, the Stream Load transaction interface allows you to merge multiple mini-batches of data on demand and then send them all at once within one transaction by calling the `/api/transaction/commit` operation. As such, fewer data versions need to be loaded, and load performance is improved.

## Limits

The Stream Load transaction interface has the following limits:

- Only **single-database single-table** transactions are supported. Support for **multi-database multi-table** transactions is in development.

- Only **concurrent data** **writes** **from one client** are supported. Support for **concurrent data writes from multiple clients** is in development.

- The `/api/transaction/load` operation can be called multiple times within one transaction. In this case, the parameter settings specified for all of the `/api/transaction/load` operations that are called must be the same.

- When you load CSV-formatted data by using the Stream Load transaction interface, make sure that each data record ends with a row delimiter.

## Basic operations

### Start a transaction

#### Syntax

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>" -H "table:<table_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

#### Return result

- If the transaction is successfully started, the following result is returned:

  ```PowerShell
  {
      "Status": "OK",
      "Message": "",
      "Label": "xxx",
      "TxnId": 9032,
      "BeginTxnTimeMs": 0
  }
  ```

- If the transaction is bound to a duplicate label, the following result is returned:

  ```PowerShell
  {
      "Status": "LABEL_ALREADY_EXISTS",
      "ExistingJobStatus": "RUNNING",
      "Message": "Label [xxx] has already been used."
  }
  ```

- If errors other than duplicate label occur, the following result is returned:

  ```PowerShell
  {
      "Status": "FAILED",
      "Message": ""
  }
  ```

### Write data

#### Syntax

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>" -H "table:<table_name>"
    -T /path/to/data.csv
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

#### Return result

- If the data write is successful, the following result is returned:

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

- If the transaction is considered unknown, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "TXN_NOT_EXISTS"
  }
  ```

- If the transaction is considered in an invalid state, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation State Invalid"
  }
  ```

- If errors other than unknown transaction and invalid status occur, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### Pre-commit a transaction

#### Syntax

```PowerShell
 curl -H "label:<label_name>" -H "db:<database_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### Return result

- If the pre-commit is successful, the following result is returned:

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

- If the transaction is considered not existent, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- If the pre-commit times out, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- If errors other than non-existent transaction and pre-commit timeout occur, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "publish timeout"
  }
  ```

### Commit a transaction

#### Syntax

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### Return result

- If the commit is successful, the following result is returned:

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

- If the transaction has already been committed, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "OK",
      "Message": "Transaction already commited",
  }
  ```

- If the transaction is considered not existent, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- If the commit times out, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- If the data publish times out, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "publish timeout",
      "CommitAndPublishTimeMs": 1393
  }
  ```

- If errors other than non-existent transaction and timeout occur, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### Roll back a transaction

#### Syntax

```PowerShell
curl -H "label:<label_name>" -H "db:<database_name>"
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### Return result

- If the rollback is successful, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "OK",
      "Message": ""
  }
  ```

- If the transaction is considered not existent, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- If errors other than not existent transaction occur, the following result is returned:

  ```PowerShell
  {
      "TxnId": 1,
      "Label": "a25eca8b-7b48-4c87-9ea7-0cbdd913e77d",
      "Status": "FAILED",
      "Message": ""
  }
  ```

## References

For information about the suitable application scenarios and supported data file formats of Stream Load and about how Stream Load works, see [Load data from a local file system or a streaming data source using HTTP PUT](../loading/StreamLoad.md).

For information about the syntax and parameters for creating Stream Load jobs, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md).
