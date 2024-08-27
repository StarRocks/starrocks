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
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Listener to trigger a statistics collection after a first load
 */
public class LoadJobStatsListener implements LoadJobListener {
    public static final LoadJobStatsListener INSTANCE = new LoadJobStatsListener();

    private static final Logger LOG = LogManager.getLogger(LoadJobStatsListener.class);

    @Override
    public void onStreamLoadTransactionFinish(TransactionState transactionState) {
        onTransactionFinish(transactionState, false);
    }

    @Override
    public void onLoadJobTransactionFinish(TransactionState transactionState) {
        // For compatibility reasons, broker load still uses async collect.
        onTransactionFinish(transactionState, false);
    }

    @Override
    public void onDMLStmtJobTransactionFinish(TransactionState transactionState, Database db, Table table) {
        StatisticUtils.triggerCollectionOnFirstLoad(transactionState, db, table, true);
    }

    @Override
    public void onInsertOverwriteJobCommitFinish(Database db, Table table) {
        // do nothing
    }

    private void onTransactionFinish(TransactionState transactionState, boolean sync) {
        if (!Config.enable_statistic_collect_on_first_load) {
            return;
        }

        long dbId = transactionState.getDbId();
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        if (localMetastore == null) {
            LOG.warn("local metastore is null when transaction finish.");
            return;
        }
        Database db = localMetastore.getDb(dbId);
        if (db == null) {
            LOG.warn("transaction db is null when transaction finish.");
            return;
        }

        try {
            List<Table> tables = transactionState.getIdToTableCommitInfos().values().stream()
                    .map(x -> x.getTableId())
                    .distinct()
                    .map(db::getTable)
                    .filter(Objects::nonNull)
                    .filter(t -> !t.isMaterializedView()) // skip mvs since its stats will be triggered after refresh
                    .collect(Collectors.toList());
            for (Table table : tables) {
                StatisticUtils.triggerCollectionOnFirstLoad(transactionState, db, table, sync);
            }
        } catch (Exception t) {
            LOG.warn("refresh mv after publish version failed:", DebugUtil.getStackTrace(t));
        }
    }

    @Override
    public void onDeleteJobTransactionFinish(Database db, Table table) {
        // do nothing
    }
}
