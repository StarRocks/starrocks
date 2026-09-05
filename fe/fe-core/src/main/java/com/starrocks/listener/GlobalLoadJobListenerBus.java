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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.DmlType;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class GlobalLoadJobListenerBus {
    private static final Logger LOG = LogManager.getLogger(GlobalLoadJobListenerBus.class);

    private final List<LoadJobListener> listeners = ImmutableList.of(
            LoadJobStatsListener.INSTANCE,
            LoadJobMVListener.INSTANCE
    );

    public GlobalLoadJobListenerBus() {
    }

    /**
     * Do all callbacks after `broker load/spark load/routine load` transaction is finished which is
     *  only triggered without an error.
     * @param transactionState finished transaction states
     */
    public void onLoadJobTransactionFinish(TransactionState transactionState) {
        if (transactionState == null) {
            return;
        }
        listeners.stream().forEach(listener -> listener.onLoadJobTransactionFinish(transactionState));
    }

    /**
     * Do all callbacks after `Insert INTO` transaction is finished, which is only triggered without an error.
     * @param transactionState finished transaction states
     * @param db database of the target table
     * @param table target table that has changed
     */
    public void onDMLStmtJobTransactionFinish(TransactionState transactionState, Database db, Table table,
                                              DmlType dmlType) {
        if (transactionState == null) {
            return;
        }
        listeners.stream().forEach(listener -> listener.onDMLStmtJobTransactionFinish(transactionState, db, table,
                dmlType
        ));
    }

    /**
     * Do all callbacks after `Insert OVERWRITE` transaction is finished, which is only triggered without an error.
     * @param db database of the target table
     * @param table target table that has changed
     */
    public void onInsertOverwriteJobCommitFinish(Database db, Table table, InsertOverwriteJobStats stats) {
        if (db == null || table == null) {
            return;
        }
        listeners.stream().forEach(listener -> listener.onInsertOverwriteJobCommitFinish(db, table, stats));
    }

    /**
     * Do all callbacks after `stream load` transaction is finished.
     * @param transactionState: finished transaction states
     */
    public void onStreamJobTransactionFinish(TransactionState transactionState) {
        if (transactionState == null) {
            return;
        }
        // only trigger the stream load transaction
        TransactionState.LoadJobSourceType sourceType = transactionState.getSourceType();
        if (!TransactionState.LoadJobSourceType.FRONTEND_STREAMING.equals(sourceType)
                && !TransactionState.LoadJobSourceType.BACKEND_STREAMING.equals(sourceType)) {
            return;
        }
        listeners.stream().forEach(listener -> listener.onStreamLoadTransactionFinish(transactionState));
    }

    /**
     * Do all callbacks after `delete` transaction is finished.
     * @param db database of the target table
     * @param table table of the target table
     */
    public void onDeleteJobTransactionFinish(Database db, Table table) {
        if (db == null || table == null) {
            return;
        }
        listeners.stream().forEach(listener -> listener.onDeleteJobTransactionFinish(db, table));
    }
}
