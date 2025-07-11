---
displayed_sidebar: docs
---

# be_txns

`be_txns` provides information about transactions on each BE node.

The following fields are provided in `be_txns`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| BE_ID          | ID of the BE node.                                           |
| LOAD_ID        | ID of the load job.                                          |
| TXN_ID         | ID of the transaction.                                       |
| PARTITION_ID   | ID of the partition involved in the transaction.             |
| TABLET_ID      | ID of the tablet involved in the transaction.                |
| CREATE_TIME    | Creation time of the transaction (Unix timestamp in seconds). |
| COMMIT_TIME    | Commit time of the transaction (Unix timestamp in seconds).  |
| PUBLISH_TIME   | Publish time of the transaction (Unix timestamp in seconds). |
| ROWSET_ID      | ID of the rowset involved in the transaction.                |
| NUM_SEGMENT    | Number of segments in the rowset.                            |
| NUM_DELFILE    | Number of delete files in the rowset.                        |
| NUM_ROW        | Number of rows in the rowset.                                |
| DATA_SIZE      | Data size of the rowset (in bytes).                          |
| VERSION        | Version of the rowset.                                       |
