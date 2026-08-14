---
displayed_sidebar: docs
description: "running_transactions lists the transactions that are currently running across all databases."
---

# running_transactions

`running_transactions` lists one row per transaction that is currently running (that is, in a non-final state) across all databases. A transaction leaves this view as soon as it reaches a final state (`VISIBLE` or `ABORTED`).

The rows cover every kind of running transaction, including regular load transactions, Routine Load tasks, and lake compaction transactions.

The following fields are provided in `running_transactions`:

| Field                 | Description                                                  |
| --------------------- | ------------------------------------------------------------ |
| TXN_ID                | The transaction ID.                                          |
| GLOBAL_TXN_ID         | The global transaction ID (GTID). `0` if the transaction has none. |
| LABEL                 | The transaction label.                                       |
| DATABASE_ID           | The ID of the database that the transaction belongs to.      |
| DATABASE_NAME         | The name of the database. |
| TABLE_IDS             | The IDs of the tables that the transaction touches, joined by commas. |
| TABLE_NAMES           | The names of the tables that the transaction touches, joined by commas. Best effort: an ID that cannot be resolved to a name appears as the raw ID. |
| STATE                 | The state of the transaction. Valid values:<ul><li>`PREPARE`: The transaction has begun.</li><li>`PREPARED`: The transaction has been pre-committed.</li><li>`COMMITTED`: The transaction has been committed and is pending publish to `VISIBLE`.</li></ul> |
| COORDINATOR           | The coordinator node of the transaction, for example, `FE: 127.0.0.1`. |
| SOURCE_TYPE           | The load source type of the transaction, for example, `BACKEND_STREAMING`, `INSERT_STREAMING`, `LAKE_COMPACTION`, `ROUTINE_LOAD_TASK`, or `FRONTEND`. |
| WAREHOUSE_ID          | The ID of the warehouse that the transaction belongs to.     |
| PREPARE_TIME          | The time at which the transaction began (entered `PREPARE`). `NULL` if unset. |
| PREPARED_TIME         | The time at which the transaction reached `PREPARED`. `NULL` if unset. |
| COMMIT_TIME           | The time at which the transaction was committed. `NULL` if not yet committed. |
| PUBLISH_TIME          | The time at which publish started. `NULL` if publish has not yet started. |
| FINISH_TIME           | The time at which the transaction finished. Always `NULL` for a running transaction, because a finished transaction leaves this view. |
| PENDING_PUBLISH_MS    | For a `COMMITTED` transaction, the time in milliseconds that it has waited to publish to `VISIBLE` (now minus commit time). `0` for any other state. This is the headline column for diagnosing publish stalls. |
| TIMEOUT_MS            | The transaction timeout, in milliseconds.                    |
| PREPARED_TIMEOUT_MS   | The `PREPARED`-state timeout, in milliseconds.               |
| ERROR_REPLICA_NUM     | The number of error replicas.                                |
| REASON                | The abort or failure reason text. May be empty.              |
| ERROR_MSG             | The error message text. May be empty.                        |
| IS_NO_OP_PUBLISH      | Whether the publish is a no-op.                              |
| NO_OP_PUBLISH_REASON  | The reason the publish is a no-op.                           |

## Usage notes

`running_transactions` is a diagnostic surface for publish stalls, which are most visible in shared-data (lake) mode. When version publish stalls, transactions pile up in the `COMMITTED` state, and `PENDING_PUBLISH_MS` shows how long each one has waited to publish to `VISIBLE`. Sorting `COMMITTED` transactions by `PENDING_PUBLISH_MS` surfaces the oldest stalled transactions first.

Because the set of running transactions is authoritative only on the FE leader, a scan of this view is always served from the leader FE.

`running_transactions` filters rows by the querying user's privileges: a transaction is shown only for a database on or within which the user holds some privilege (for example a privilege on the database itself, or `SELECT` on one of its tables). A user with cluster-wide access (for example the `root` user, or a user with a broad grant) sees all running transactions. A transaction whose database was dropped while it was still running cannot be authorized and is hidden from every user, including administrators.

:::note

Do not assume that `COUNT(*)` of this view equals the `txn_running` metric. The two are computed differently and may not match.

:::

## Example

Find the committed transactions that have waited the longest to publish:

```sql
SELECT TXN_ID, DATABASE_NAME, TABLE_NAMES, STATE, PENDING_PUBLISH_MS
FROM information_schema.running_transactions
WHERE STATE = 'COMMITTED'
ORDER BY PENDING_PUBLISH_MS DESC;
```
