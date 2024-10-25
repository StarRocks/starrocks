// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.listener;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.DmlType;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.TransactionState;

/**
 * Listener for / insert into / stream load/ routine load/ insert into/ … job finished
 */
public interface LoadJobListener {
    /**
     * Listener after `stream load` transaction is finished.
     * @param transactionState: finished transaction states
     */
    void onStreamLoadTransactionFinish(TransactionState transactionState);

    /**
     * Listener after `broker load/spark load/routine load` transaction is finished which is
     *  only triggered without an error.
     * @param transactionState finished transaction states
     */
    void onLoadJobTransactionFinish(TransactionState transactionState);

    /**
     * Listener after `Insert INTO` transaction is finished, which is only triggered without an error.
     *
     * @param transactionState finished transaction states
     * @param db               database of the target table
     * @param table            target table that has changed
     * @param dmlType
     */
    void onDMLStmtJobTransactionFinish(TransactionState transactionState, Database db, Table table, DmlType dmlType);

    /**
     * Listener after `Insert OVERWRITE` transaction is finished, which is only triggered without an error.
     *
     * @param db    database of the target table
     * @param table target table that has changed
     * @param stats
     */
    void onInsertOverwriteJobCommitFinish(Database db, Table table, InsertOverwriteJobStats stats);

    /**
     * Listener after `Delete` transaction is finished, which is only triggered without an error.
     * @param db database of the target table
     * @param table target table that has changed
     */
    void onDeleteJobTransactionFinish(Database db, Table table);
}
