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

package com.starrocks.load.loadv2;

import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * The callback implementation for insert load transaction of materialized view with IVM refresh.
 * This will ensure the base table's tvr version range is updated after the transaction is committed.
 */
public class IVMInsertLoadTxnCallback implements InsertLoadTxnCallback {
    private static final Logger LOG = LogManager.getLogger(IVMInsertLoadTxnCallback.class);

    private long tableId;
    private long dbId;
    private MaterializedView mv;
    private final Map<BaseTableInfo, TvrVersionRange> baseTableInfoTvrDeltaMap = Maps.newConcurrentMap();

    public IVMInsertLoadTxnCallback(long dbId, long tableId) {
        this.dbId = dbId;
        this.tableId = tableId;
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (database == null) {
            throw new TransactionException("Database " + dbId + "has been deleted");
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
        if (table == null || !(table instanceof MaterializedView)) {
            throw new TransactionException("Failed to find table " + tableId + " in db " + dbId);
        }
        this.mv = (MaterializedView) table;
        final MaterializedView.MvRefreshScheme refreshScheme = mv.getRefreshScheme();
        final MaterializedView.AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
        final Map<BaseTableInfo, TvrVersionRange> tempBaseTableInfoTvrDeltaMap =
                asyncRefreshContext.getTempBaseTableInfoTvrDeltaMap();
        if (tempBaseTableInfoTvrDeltaMap == null) {
            LOG.warn("Materialized view {} has no temp base table info tvr version range map, " +
                    "skip update version range", mv.getName());
            return;
        }

        // apply the delta into baseTableInfoTvrDeltaMap
        Map<BaseTableInfo, TvrVersionRange> mvBaseTableInfoTvrDeltaMap =
                asyncRefreshContext.getBaseTableInfoTvrVersionRangeMap();
        for (Map.Entry<BaseTableInfo, TvrVersionRange> entry : tempBaseTableInfoTvrDeltaMap.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            BaseTableInfo baseTableInfo = entry.getKey();
            TvrVersionRange toCommitVersionRange = entry.getValue();
            TvrVersionRange committedVersionRange = mvBaseTableInfoTvrDeltaMap.get(baseTableInfo);
            if (committedVersionRange != null) {
                // TODO: how to handle the non-continuous version range?
                // merge the temp delta into baseTableInfoTvrDeltaMap
                if (!committedVersionRange.to().equals(toCommitVersionRange.from())) {
                    LOG.warn("TvrVersionRange is not continuous, "
                                    + "committedVersionRange: {}, toCommitVersionRange: {}",
                            committedVersionRange, toCommitVersionRange);
                }
            }
            this.baseTableInfoTvrDeltaMap.put(baseTableInfo, TvrTableSnapshot.of(toCommitVersionRange.to));
        }
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws StarRocksException {
        if (CollectionUtils.sizeIsEmpty(this.baseTableInfoTvrDeltaMap)) {
            LOG.info("Materialized view {} has no base table info tvr version range to update, skip", mv.getName());
            return;
        }
        LOG.info("Materialized view {} has been committed, update the base table info tvr version range: {}",
                mv.getName(), baseTableInfoTvrDeltaMap);
        final MaterializedView.MvRefreshScheme refreshScheme = mv.getRefreshScheme();
        final MaterializedView.AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
        Map<BaseTableInfo, TvrVersionRange> mvBaseTableInfoTvrDeltaMap =
                asyncRefreshContext.getBaseTableInfoTvrVersionRangeMap();

        // update mv's base table info tvr version range map
        mvBaseTableInfoTvrDeltaMap.putAll(baseTableInfoTvrDeltaMap);
        // clear the temp map
        asyncRefreshContext.clearTempBaseTableInfoTvrDeltaMap();

        long maxChangedTableRefreshTime = refreshScheme.getLastRefreshTime();
        mv.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
        ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog = new ChangeMaterializedViewRefreshSchemeLog(mv);
        GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        LOG.info("Update materialized view {} refresh scheme, " +
                "last refresh time: {}, version meta changed", mv.getName(), maxChangedTableRefreshTime);
    }
}
