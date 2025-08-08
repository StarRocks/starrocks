---
displayed_sidebar: docs
---

# SHOW TRANSACTION

SHOW TRANSACTION is used to view the transaction details of the specified transaction id.

Syntax:

```sql
SHOW TRANSACTION
[FROM <db_name>]
WHERE id = transaction_id
```

Examples of returned results:

```plain text
TransactionId: 4005
Label: insert_8d807d5d-bcdd-46eb-be6d-3fa87aa4952d
Coordinator: FE: 10.74.167.16
TransactionStatus: VISIBLE
LoadJobSourceType: INSERT_STREAMING
PrepareTime: 2020-01-09 14:59:07
PreparedTime: 2020-01-09 14:59:08
CommitTime: 2020-01-09 14:59:09
FinishTime: 2020-01-09 14:59:09
Reason:
ErrorReplicasCount: 0
ListenerId: -1
TimeoutMs: 300000
PreparedTimeoutMs: 86400000
```

* TransactionId: transaction id
* Label: import the label corresponding to the task
* Coordinator: the node responsible for transaction coordination
* TransactionStatus: transaction status
* PREPARE: preparation stage
* COMMITTED: the transaction succeeded, but the data is not visible
* VISIBLE: the transaction is successful and the data is visible
* ABORTED: transaction failed
* LoadJobSourceType: type of import task.
* PrepareTime: transaction start time
* PreparedTime: the time when the transaction is successfully prepared (supported from v3.5.4)
* CommitTime: the time when the transaction is successfully committed
* FinishTime: the time when the data is visible
* Reason: error message
* ErrorReplicasCount: number of replicas with errors
* ListenerId: id of the related import job
* TimeoutMs: timeout for the transaction from `PREPARE` to `PREPARED` state, in milliseconds
* PreparedTimeoutMs: timeout for the transaction from `PREPARED` to `COMMITTED` state, in milliseconds (supported from v3.5.4)

## Examples

1. To view a transaction with id 4005:

    ```sql
    SHOW TRANSACTION WHERE ID=4005;
    ```

2. In the specified dB, view the transaction with id 4005:

    ```sql
    SHOW TRANSACTION FROM db WHERE ID=4005;
    ```
