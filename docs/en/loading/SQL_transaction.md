---
displayed_sidebar: docs
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# SQL Transaction

<Beta />

Start a simple SQL transaction to commit multiple DML statements in a batch.

## Overview

From v3.5.0, StarRocks supports SQL transactions to assure the atomicity of the updated tables when manipulating data within multiple tables.

A transaction consists of multiple SQL statements that are processed within the same atomic unit. The statements in the transaction are either applied or undone together, thus guaranteeing the ACID (atomicity, consistency, isolation, and durability) properties of the transaction.

Currently, the SQL transaction in StarRocks supports the following operations:
- INSERT INTO
- UPDATE
- DELETE

:::note

- INSERT OVERWRITE is not supported currently.
- Multiple INSERT statements against the same table within a transaction are supported only in shared-data clusters from v4.0 onwards.
- UPDATE and DELETE are supported only in shared-data clusters from v4.0 onwards.

:::

From v4.0 onwards, within one SQL transaction:
- **Multiple INSERT statements** against the one table are supported.
- **Only one UPDATE *OR* DELETE** statement against one table is allowed.
- **An UPDATE *OR* DELETE** statement **after** INSERT statements against the same table is **not allowed**.

The ACID properties of the transaction are guaranteed only on the limited READ COMMITTED isolation level, that is:
- A statement operates only on data that was committed before the statement began. 
- Two successive statements within the same transaction may operate on different data if another transaction is committed between the execution of the first and the second statements.
- Data changes brought by preceding DML statements are invisible to subsequent statements within the same transaction.

A transaction is associated with a single session. Multiple sessions cannot share the same transaction.

## Usage

1. A transaction must be started by executing a START TRANSACTION statement. StarRocks also supports the synonym BEGIN.

   ```SQL
   { START TRANSACTION | BEGIN [ WORK ] }
   ```

2. After starting the transaction, you can define multiple DML statements in the transaction. For detailed information, see [Usage notes](#usage-notes).

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

1. Create the demo table `desT` in a shared-data cluster, and load data into it.

    :::note
    If you want to try this example in a shared-nothing cluster, you must skip Step 3 and define only one INSERT statement in Step 4.
    :::

    ```SQL
    CREATE TABLE desT (
        k int,
        v int
    ) PRIMARY KEY(k);

    INSERT INTO desT VALUES
    (1,1),
    (2,2),
    (3,3);
    ```

2. Start a transaction.

    ```SQL
    START TRANSACTION;
    ```

    Or

    ```SQL
    BEGIN WORK;
    ```

3. Define an UPDATE or DELETE statement.

    ```SQL
    UPDATE desT SET v = v + 1 WHERE k = 1,
    ```

    Or

    ```SQL
    DELETE FROM desT WHERE k = 1;
    ```

4. Define multiple INSERT statements.

    ```SQL
    -- Insert data with specified values.
    INSERT INTO desT VALUES (4,4);
    -- Insert data from a native table to another.
    INSERT INTO desT SELECT * FROM srcT;
    -- Insert data from remote storage.
    INSERT INTO desT
        SELECT * FROM FILES(
            "path" = "s3://inserttest/parquet/srcT.parquet",
            "format" = "parquet",
            "aws.s3.access_key" = "XXXXXXXXXX",
            "aws.s3.secret_key" = "YYYYYYYYYY",
            "aws.s3.region" = "us-west-2"
    );
    ```

5. Apply or undo the transaction.

    - To apply the SQL statements in the transaction.

      ```SQL
      COMMIT WORK;
      ```

    - To undo the SQL statements in the transaction.

      ```SQL
      ROLLBACK WORK;
      ```

## Usage notes

- Currently, StarRocks supports SELECT, INSERT, UPDATE, and DELETE statements in SQL transactions. UPDATE and DELETE are supported only in shared-data clusters from v4.0 onwards.
- SELECT statements against the tables whose data have been changed in the same transaction are not allowed.
- Multiple INSERT statements against the same table within a transaction are supported only in shared-data clusters from v4.0 onwards.
- Within a transaction, you can only define one UPDATE or DELETE statement against each table, and it must precede the INSERT statements.
- Subsequent DML statements cannot read the uncommitted changes brought by preceding statements within the same transaction. For example, the target table of the preceding INSERT statement cannot be the source table of subsequent statements. Otherwise, the system returns an error.
- All target tables of the DML statements in a transaction must be within the same database. Cross-database operations are not allowed.
- Currently, INSERT OVERWRITE is not supported.
- Nesting transactions are not allowed. You cannot specify BEGIN WORK within a BEGIN-COMMIT/ROLLBACK pair.
- If the session where an on-going transaction belongs is terminated or closed, the transaction is automatically rolled back.
- StarRock only supports limited READ COMMITTED for Transaction Isolation Level as described above.
- Write conflict checks are not supported. When two transactions write to the same table simultaneously, both transactions can be committed successfully. The visibility (order) of the data changes depends on the execution order of the COMMIT WORK statements.
