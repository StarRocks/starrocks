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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Auto trigger by job operation listener to refresh related materialized views.
 */
public class LoadJobMVListener implements LoadJobListener {
    private static final Logger LOG = LogManager.getLogger(LoadJobMVListener.class);

    public static final LoadJobMVListener INSTANCE = new LoadJobMVListener();

    private static boolean isTriggerOnTransactionFinish(TransactionState transactionState) {
        if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
            return false;
        }
        return true;
    }

    @Override
    public void onStreamLoadTransactionFinish(TransactionState transactionState) {
        if (!isTriggerOnTransactionFinish(transactionState)) {
            return;
        }
        // how to handle stream load transaction?
        triggerToRefreshRelatedMVs(transactionState, false);
    }

    @Override
    public void onLoadJobTransactionFinish(TransactionState transactionState) {
        if (!isTriggerOnTransactionFinish(transactionState)) {
            return;
        }
        triggerToRefreshRelatedMVs(transactionState, false);
    }

    @Override
    public void onDMLStmtJobTransactionFinish(TransactionState transactionState, Database db, Table table) {
        if (table != null && table.isMaterializedView()) {
            return;
        }
        if (!isTriggerOnTransactionFinish(transactionState)) {
            return;
        }
        triggerToRefreshRelatedMVs(db, table);
    }

    @Override
    public void onInsertOverwriteJobCommitFinish(Database db, Table table) {
        triggerToRefreshRelatedMVs(db, table);
    }

    /**
     * Refresh the materialized view if it should be triggered after base table was loaded.
     * @param transactionState version published transaction state
     * @param isTriggerIfBaseTableIsMV whether to trigger the refresh if the base table is a materialized view
     * @throws DdlException
     * @throws MetaNotFoundException
     */
    private void triggerToRefreshRelatedMVs(TransactionState transactionState, boolean isTriggerIfBaseTableIsMV) {
        // Refresh materialized view when base table update transaction has been visible
        long dbId = transactionState.getDbId();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            LOG.warn("failed to get Database when pending refresh, DBId: {}", dbId);
            return;
        }
        for (long tableId : transactionState.getTableIdList()) {
            Table table = db.getTable(tableId);
            if (table == null) {
                LOG.warn("failed to get transaction tableId {} when pending refresh.", tableId);
                return;
            }
            if (!isTriggerIfBaseTableIsMV && table.isMaterializedView()) {
                LOG.info("Skip to trigger refresh related materialized views in publish version phase because " +
                        "base table {} is a materialized view.", table.getName());
                continue;
            }
            triggerToRefreshRelatedMVs(db, table);
        }
    }

    private void triggerToRefreshRelatedMVs(Database db, Table table) {
        try {
            doTriggerToRefreshRelatedMVs(db, table);
        } catch (Exception e) {
            LOG.warn("refresh mv after publish version failed: {}", DebugUtil.getStackTrace(e));
        }
    }

    /**
     * Trigger to refresh related materialized views if necessary.
     * @param db base table's database
     * @param table base table to trigger the related materialized views
     * @throws DdlException
     * @throws MetaNotFoundException
     */
    private void doTriggerToRefreshRelatedMVs(Database db, Table table) throws DdlException, MetaNotFoundException {
        if (table == null) {
            return;
        }
        Set<MvId> relatedMvs = table.getRelatedMaterializedViews();
        if (relatedMvs == null || relatedMvs.isEmpty()) {
            return;
        }
        Iterator<MvId> mvIdIterator = relatedMvs.iterator();
        while (mvIdIterator.hasNext()) {
            MvId mvId = mvIdIterator.next();
            Database mvDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
            MaterializedView materializedView = (MaterializedView) mvDb.getTable(mvId.getId());
            if (materializedView == null) {
                LOG.warn("materialized view {} does not exists.", mvId.getId());
                mvIdIterator.remove();
                continue;
            }
            // It's fine to no lock here, since it's not a critical operation and can be retried.
            if (materializedView.shouldTriggeredRefreshBy(db.getFullName(), table.getName())) {
                LOG.info("Trigger auto materialized view refresh because of base table {} has changed, " +
                                "db:{}, mv:{}", table.getName(), mvDb.getFullName(),
                        materializedView.getName());
                GlobalStateMgr.getCurrentState().getLocalMetastore().refreshMaterializedView(
                        mvDb.getFullName(), mvDb.getTable(mvId.getId()).getName(), false, null,
                        Constants.TaskRunPriority.NORMAL.value(), true, false);
            }
        }
    }

    private List<PartitionCommitInfo> getPartitionCommitInfos(TransactionState txnState, long tableId) {
        TableCommitInfo tableCommitInfo = txnState.getTableCommitInfo(tableId);
        if (tableCommitInfo == null) {
            return Collections.emptyList();
        }
        if (tableCommitInfo.getIdToPartitionCommitInfo() == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(tableCommitInfo.getIdToPartitionCommitInfo().values());
    }

    @Override
    public void onDeleteJobTransactionFinish(Database db, Table table) {
        triggerToRefreshRelatedMVs(db, table);
    }
}