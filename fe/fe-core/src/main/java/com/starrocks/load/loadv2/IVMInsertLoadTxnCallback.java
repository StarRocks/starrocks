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
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;
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
    // Owner captured in beforeCommitted; afterCommitted refuses to clear on mismatch.
    private String capturedOwner;

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
        this.capturedOwner = asyncRefreshContext.getTempTvrOwnerStartTaskRunId();

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

        if (!this.baseTableInfoTvrDeltaMap.isEmpty()) {
            // Failing the commit is the safe direction: an unrecorded window is retried, a committed one
            // that no record describes is applied twice.
            insertAttachmentOf(txnState).setIvmRefreshCommitInfo(
                    new IVMRefreshCommitInfo(Maps.newHashMap(this.baseTableInfoTvrDeltaMap)));
        }
    }

    private static InsertTxnCommitAttachment insertAttachmentOf(TransactionState txnState)
            throws TransactionException {
        TxnCommitAttachment attachment = txnState == null ? null : txnState.getTxnCommitAttachment();
        if (!(attachment instanceof InsertTxnCommitAttachment)) {
            throw new TransactionException("IVM refresh cannot record its tvr version range: expected an "
                    + "InsertTxnCommitAttachment on the transaction, got " + attachment);
        }
        return (InsertTxnCommitAttachment) attachment;
    }

    @Override
    public void afterCommitted(TransactionState txnState) throws StarRocksException {
        if (CollectionUtils.sizeIsEmpty(this.baseTableInfoTvrDeltaMap)) {
            LOG.info("Materialized view {} has no base table info tvr version range to update, skip", mv.getName());
            return;
        }
        LOG.info("Materialized view {} has been committed, update the base table info tvr version range: {}",
                mv.getName(), baseTableInfoTvrDeltaMap);
        final MaterializedView.MvRefreshScheme copiedScheme = mv.getRefreshScheme().copy(); // copy on write
        final MaterializedView.AsyncRefreshContext asyncRefreshContext = copiedScheme.getAsyncRefreshContext();

        promoteCommittedTvr(asyncRefreshContext, baseTableInfoTvrDeltaMap);
        // Only clear if we still own the slot — guard against takeover between the two callbacks.
        String currentOwner = asyncRefreshContext.getTempTvrOwnerStartTaskRunId();
        if (currentOwner == null || currentOwner.equals(capturedOwner)) {
            asyncRefreshContext.clearTempBaseTableInfoTvrDeltaState();
        } else {
            LOG.warn("Skip clearTempBaseTableInfoTvrDeltaState in IVM afterCommitted: " +
                    "captured owner={}, current owner={}", capturedOwner, currentOwner);
        }

        long maxChangedTableRefreshTime = mv.getRefreshScheme().getLastRefreshTime();
        copiedScheme.setLastRefreshTime(maxChangedTableRefreshTime);
        ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                new ChangeMaterializedViewRefreshSchemeLog(mv, copiedScheme);
        try {
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog,
                    wal -> mv.setRefreshScheme(copiedScheme));
        } catch (Throwable t) {
            // Only for a node that demotes and keeps running: it advances its replay position past its own
            // writes, so it alone never replays the txn record that already carries this window. A node that
            // exits replays that record normally on restart and needs nothing here.
            promoteCommittedTvr(mv.getRefreshScheme().getAsyncRefreshContext(), baseTableInfoTvrDeltaMap);
            throw t;
        }
        // The COMMITTED record is durable by now, and replay reads the window off it, not off this object.
        clearRefreshCommitInfo(txnState);
        LOG.info("Update materialized view {} refresh scheme, " +
                "last refresh time: {}, version meta changed", mv.getName(), maxChangedTableRefreshTime);
    }

    /**
     * The leader reaches this from {@link #afterCommitted} and every replaying node from
     * {@link #replayOnCommitted}, so the two paths cannot drift apart.
     */
    static void promoteCommittedTvr(MaterializedView.AsyncRefreshContext target,
                                    Map<BaseTableInfo, TvrVersionRange> committedTvrMap) {
        target.getBaseTableInfoTvrVersionRangeMap().putAll(committedTvrMap);
    }

    private static void clearRefreshCommitInfo(TransactionState txnState) {
        TxnCommitAttachment attachment = txnState == null ? null : txnState.getTxnCommitAttachment();
        if (attachment instanceof InsertTxnCommitAttachment) {
            ((InsertTxnCommitAttachment) attachment).setIvmRefreshCommitInfo(null);
        }
    }

    /** Only an IVM refresh ever fills this slot, so the record alone identifies one. */
    static boolean carriesCommittedTvr(TransactionState txnState) {
        return !CollectionUtils.sizeIsEmpty(committedTvrMapOf(txnState));
    }

    private static Map<BaseTableInfo, TvrVersionRange> committedTvrMapOf(TransactionState txnState) {
        IVMRefreshCommitInfo commitInfo = refreshCommitInfoOf(txnState);
        return commitInfo == null ? null : commitInfo.getCommittedTvrMap();
    }

    private static IVMRefreshCommitInfo refreshCommitInfoOf(TransactionState txnState) {
        TxnCommitAttachment attachment = txnState == null ? null : txnState.getTxnCommitAttachment();
        return attachment instanceof InsertTxnCommitAttachment
                ? ((InsertTxnCommitAttachment) attachment).getIvmRefreshCommitInfo() : null;
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        Map<BaseTableInfo, TvrVersionRange> committedTvrMap = committedTvrMapOf(txnState);
        if (CollectionUtils.sizeIsEmpty(committedTvrMap)) {
            return;
        }
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (database == null) {
            return;
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
        if (!(table instanceof MaterializedView)) {
            return;
        }
        MaterializedView mv = (MaterializedView) table;
        promoteCommittedTvr(mv.getRefreshScheme().getAsyncRefreshContext(), committedTvrMap);
        LOG.info("Replayed the base table tvr version range of materialized view {}: {}",
                mv.getName(), committedTvrMap);
    }
}
