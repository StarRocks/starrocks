---
displayed_sidebar: docs
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# SQL Transaction

<Beta />

Start a simple SQL transaction to commit multiple INSERT statements in a batch.

## Overview

From v3.5.0, StarRocks supports SQL transactions to assure the atomicity of the updated tables when loading data into multiple tables.

A transaction consists of multiple SQL statements that are processed within the same atomic unit. The statements in the transaction are either applied or undone together, thus guaranteeing the ACID (atomicity, consistency, isolation, and durability) properties of the transaction.

Currently, the SQL transaction in StarRocks only supports loading operations via INSERT, and does not allow multiple INSERT statements against the same table. The ACID properties of the transaction are guaranteed only on the limited READ COMMITTED isolation level, that is:

- A statement operates only on data that was committed before the statement began. 
- Two successive statements within the same transaction may operate on different data if another transaction is committed between the execution of the first and the second statements.
- Data changes brought by preceding DML statements are invisible to subsequent statements within the same transaction.

A transaction is associated with a single session. Multiple sessions cannot share the same transaction.

## Usage

1. A transaction must be started by executing a START TRANSACTION statement. StarRocks also supports the synonym BEGIN.

   ```SQL
   { START TRANSACTION | BEGIN [ WORK ] }
   ```

2. After starting the transaction, you can define multiple INSERT statements in the transaction. For detailed information, see [Usage notes](#usage-notes).

3. A transaction must be ended explicitly by executing `COMMIT` or `ROLLBACK`.

   - To apply (commit) the transaction, use the following syntax:

     ```SQL
     COMMIT [ WORK ]
     ```

   - To undo (roll back) the transaction, use the following syntax:

     ```SQL
     ROLLBACK [ WORK ]
     ```

## Example

```SQL
BEGIN WORK;
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
COMMIT WORK;
```

## Usage notes

- Currently, StarRocks only supports INSERT and SELECT statements in SQL transactions.
- All target tables of the DML statements in a transaction must be within the same database.
- Multiple INSERT statements against the same table within a transaction are not allowed.
- Nesting transactions are not allowed. You cannot specify BEGIN WORK within a BEGIN-COMMIT/ROLLBACK pair.
- If the session where an on-going transaction belongs is terminated or closed, the transaction is automatically rolled back.
- StarRock only supports limited READ COMMITTED for Transaction Isolation Level as described above.
- Write conflict checks are not supported. When two transactions write to the same table simultaneously, both transactions can be committed successfully. The visibility (order) of the data changes depends on the execution order of the COMMIT WORK statements.
