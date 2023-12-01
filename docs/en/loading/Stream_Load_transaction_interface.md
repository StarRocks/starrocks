---
displayed_sidebar: "English"
---

# Load data using Stream Load transaction interface

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

From v2.4 onwards, StarRocks provides a Stream Load transaction interface to implement two-phase commit (2PC) for transactions that are run to load data from external systems such as Apache Flink® and Apache Kafka®. The Stream Load transaction interface helps improve the performance of highly concurrent stream loads.

This topic describes the Stream Load transaction interface and how to load data into StarRocks by using this interface.

<InsertPrivNote />

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

- Only **concurrent data writes from one client** are supported. Support for **concurrent data writes from multiple clients** is in development.

- The `/api/transaction/load` operation can be called multiple times within one transaction. In this case, the parameter settings specified for all of the `/api/transaction/load` operations that are called must be the same.

- When you load CSV-formatted data by using the Stream Load transaction interface, make sure that each data record in your data file ends with a row delimiter.

## Precautions

- If the `/api/transaction/begin`, `/api/transaction/load`, or `/api/transaction/prepare` operation that you have called returns errors, the transaction fails and is automatically rolled back.
- When calling the `/api/transaction/begin` operation to start a new transaction, you have the option to specify a label. If you do not specify a label, StarRocks will generate a label for the transaction. Note that the subsequent `/api/transaction/load`, `/api/transaction/prepare`, and `/api/transaction/commit` operations must use the same label as the `/api/transaction/begin` operation.
- If you the label of a previous transaction to call the `/api/transaction/begin` operation to start a new transaction, the previous transaction will fail and be rolled back.
- The default column separator and row delimiter that StarRocks supports for CSV-formatted data are `\t` and `\n`. If your data file does not use the default column separator or row delimiter, you must use `"column_separator: <column_separator>"` or `"row_delimiter: <row_delimiter>"` to specify the column separator or row delimiter that is actually used in your data file when calling the `/api/transaction/load` operation.

## Basic operations

### Prepare sample data

This topic uses CSV-formatted data as an example.

1. In the `/home/disk1/` path of your local file system, create a CSV file named `example1.csv`. The file consists of three columns, which represent the user ID, user name, and user score in sequence.

   ```Plain
   1,Lily,23
   2,Rose,23
   3,Alice,24
   4,Julia,25
   ```

2. In your StarRocks database `test_db`, create a Primary Key table named `table1`. The table consists of three columns: `id`, `name`, and `score`, of which `id` is the primary key.

   ```SQL
   CREATE TABLE `table1`
   (
       `id` int(11) NOT NULL COMMENT "user ID",
       `name` varchar(65533) NULL COMMENT "user name",
       `score` int(11) NOT NULL COMMENT "user score"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

### Start a transaction

#### Syntax

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" -H "table:<table_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

#### Example

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" -H "table:table1" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

> **NOTE**
>
> For this example, `streamload_txn_example1_table1` is specified as the label of the transaction.

#### Return result

- If the transaction is successfully started, the following result is returned:

  ```Bash
  {
      "Status": "OK",
      "Message": "",
      "Label": "streamload_txn_example1_table1",
      "TxnId": 9032,
      "BeginTxnTimeMs": 0
  }
  ```

- If the transaction is bound to a duplicate label, the following result is returned:

  ```Bash
  {
      "Status": "LABEL_ALREADY_EXISTS",
      "ExistingJobStatus": "RUNNING",
      "Message": "Label [streamload_txn_example1_table1] has already been used."
  }
  ```

- If errors other than duplicate label occur, the following result is returned:

  ```Bash
  {
      "Status": "FAILED",
      "Message": ""
  }
  ```

### Write data

#### Syntax

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" -H "table:<table_name>" \
    -T <file_path> \
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

> **NOTE**
>
> When calling the `/api/transaction/load` operation, you must use `<file_path>` to specify the save path of the data file you want to load.

#### Example

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" -H "table:table1" \
    -T /home/disk1/example1.csv \
    -H "column_separator: ," \
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

> **NOTE**
>
> For this example, the column separator used in the data file `example1.csv` is commas (`,`) instead of StarRocks‘s default column separator (`\t`). Therefore, when calling the `/api/transaction/load` operation, you must use `"column_separator: <column_separator>"` to specify commas (`,`) as the column separator.

#### Return result

- If the data write is successful, the following result is returned:

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

- If the transaction is considered unknown, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "TXN_NOT_EXISTS"
  }
  ```

- If the transaction is considered in an invalid state, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation State Invalid"
  }
  ```

- If errors other than unknown transaction and invalid status occur, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### Pre-commit a transaction

#### Syntax

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### Example

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### Return result

- If the pre-commit is successful, the following result is returned:

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

- If the transaction is considered not existent, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- If the pre-commit times out, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- If errors other than non-existent transaction and pre-commit timeout occur, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "publish timeout"
  }
  ```

### Commit a transaction

#### Syntax

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### Example

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### Return result

- If the commit is successful, the following result is returned:

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

- If the transaction has already been committed, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "Transaction already commited",
  }
  ```

- If the transaction is considered not existent, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- If the commit times out, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- If the data publish times out, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "publish timeout",
      "CommitAndPublishTimeMs": 1393
  }
  ```

- If errors other than non-existent transaction and timeout occur, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### Roll back a transaction

#### Syntax

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### Example

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### Return result

- If the rollback is successful, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": ""
  }
  ```

- If the transaction is considered not existent, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- If errors other than not existent transaction occur, the following result is returned:

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

## References

For information about the suitable application scenarios and supported data file formats of Stream Load and about how Stream Load works, see [Loading from a local file system via Stream Load](../loading/StreamLoad.md#loading-from-a-local-file-system-via-stream-load).

For information about the syntax and parameters for creating Stream Load jobs, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md).
