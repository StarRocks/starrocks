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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.TraceManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.thrift.TUniqueId;
import io.opentelemetry.api.trace.Span;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/**
 * Transaction Manager in database level, as a component in GlobalTransactionMgr
 * DatabaseTransactionMgr mainly be responsible for the following content:
 * 1. provide read/write lock in database level
 * 2. provide basic txn infos interface in database level to GlobalTransactionMgr
 * 3. do some transaction management, such as add/update/delete transaction.
 * Attention: all api in DatabaseTransactionMgr should be only invoked by GlobalTransactionMgr
 */

public class DatabaseTransactionMgr {

    private static final Logger LOG = LogManager.getLogger(DatabaseTransactionMgr.class);

    private long dbId;

    // the lock is used to control the access to transaction states
    // no other locks should be inside this lock
    private ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock(true);

    // transactionId -> running TransactionState
    private Map<Long, TransactionState> idToRunningTransactionState = Maps.newHashMap();

    // transactionId -> final status TransactionState
    private Map<Long, TransactionState> idToFinalStatusTransactionState = Maps.newHashMap();

    // to store transtactionStates with final status
    private ArrayDeque<TransactionState> finalStatusTransactionStateDeque = new ArrayDeque<>();

    // store committed transactions' dependency relationships
    private TransactionGraph transactionGraph = new TransactionGraph();

    // label -> txn ids
    // this is used for checking if label already used. a label may correspond to multiple txns,
    // and only one is success.
    // this member should be consistent with idToTransactionState,
    // which means if a txn exist in idToRunningTransactionState or idToFinalStatusTransactionState
    // it must exists in dbIdToTxnLabels, and vice versa
    private Map<String, Set<Long>> labelToTxnIds = Maps.newHashMap();

    // count the number of running txns of database, except for the routine load txn
    private int runningTxnNums = 0;

    // count only the number of running routine load txns of database
    private int runningRoutineLoadTxnNums = 0;

    private GlobalStateMgr globalStateMgr;

    private EditLog editLog;

    private TransactionIdGenerator idGenerator;

    // not realtime usedQuota value to make a fast check for database data quota
    private volatile long usedQuotaDataBytes = -1;

    private long maxCommitTs = 0;

    private final TransactionStateListenerFactory stateListenerFactory = new TransactionStateListenerFactory();

    private final TransactionLogApplierFactory txnLogApplierFactory = new TransactionLogApplierFactory();

    protected void readLock() {
        this.transactionLock.readLock().lock();
    }

    protected void readUnlock() {
        this.transactionLock.readLock().unlock();
    }

    protected void writeLock() {
        this.transactionLock.writeLock().lock();
    }

    protected void writeUnlock() {
        this.transactionLock.writeLock().unlock();
    }

    public DatabaseTransactionMgr(long dbId, GlobalStateMgr globalStateMgr, TransactionIdGenerator idGenerator) {
        this.dbId = dbId;
        this.globalStateMgr = globalStateMgr;
        this.idGenerator = idGenerator;
        this.editLog = globalStateMgr.getEditLog();
    }

    public long getDbId() {
        return dbId;
    }

    public TransactionState getTransactionState(Long transactionId) {
        readLock();
        try {
            TransactionState transactionState = idToRunningTransactionState.get(transactionId);
            if (transactionState != null) {
                return transactionState;
            } else {
                return idToFinalStatusTransactionState.get(transactionId);
            }
        } finally {
            readUnlock();
        }
    }

    private TransactionState unprotectedGetTransactionState(Long transactionId) {
        TransactionState transactionState = idToRunningTransactionState.get(transactionId);
        if (transactionState != null) {
            return transactionState;
        } else {
            return idToFinalStatusTransactionState.get(transactionId);
        }
    }

    @VisibleForTesting
    @Nullable
    protected Set<Long> unprotectedGetTxnIdsByLabel(String label) {
        return labelToTxnIds.get(label);
    }

    @VisibleForTesting
    protected int getRunningTxnNums() {
        return runningTxnNums;
    }

    @VisibleForTesting
    protected int getRunningRoutineLoadTxnNums() {
        return runningRoutineLoadTxnNums;
    }

    @VisibleForTesting
    protected int getFinishedTxnNums() {
        return finalStatusTransactionStateDeque.size();
    }

    public List<List<String>> getTxnStateInfoList(boolean running, int limit) {
        List<List<String>> infos = Lists.newArrayList();
        Collection<TransactionState> transactionStateCollection = null;
        readLock();
        try {
            if (running) {
                transactionStateCollection = idToRunningTransactionState.values();
            } else {
                transactionStateCollection = finalStatusTransactionStateDeque;
            }
            // get transaction order by txn id desc limit 'limit'
            transactionStateCollection.stream()
                    .sorted(TransactionState.TXN_ID_COMPARATOR)
                    .limit(limit)
                    .forEach(t -> {
                        List<String> info = Lists.newArrayList();
                        getTxnStateInfo(t, info);
                        infos.add(info);
                    });
        } finally {
            readUnlock();
        }
        return infos;
    }

    public Optional<Long> getMinActiveTxnId() {
        readLock();
        try {
            if (idToRunningTransactionState.isEmpty()) {
                return Optional.empty();
            }
            long minId = idToRunningTransactionState.keySet().stream().min(Comparator.comparing(Long::longValue)).get();
            return Optional.of(minId);
        } finally {
            readUnlock();
        }
    }

    private void getTxnStateInfo(TransactionState txnState, List<String> info) {
        info.add(String.valueOf(txnState.getTransactionId()));
        info.add(txnState.getLabel());
        info.add(txnState.getCoordinator().toString());
        info.add(txnState.getTransactionStatus().name());
        info.add(txnState.getSourceType().name());
        info.add(TimeUtils.longToTimeString(txnState.getPrepareTime()));
        info.add(TimeUtils.longToTimeString(txnState.getCommitTime()));
        info.add(TimeUtils.longToTimeString(txnState.getPublishVersionTime()));
        info.add(TimeUtils.longToTimeString(txnState.getFinishTime()));
        info.add(txnState.getReason());
        info.add(String.valueOf(txnState.getErrorReplicas().size()));
        info.add(String.valueOf(txnState.getCallbackId()));
        info.add(String.valueOf(txnState.getTimeoutMs()));
        info.add(txnState.getErrMsg());
    }

    public long beginTransaction(List<Long> tableIdList, String label, TUniqueId requestId,
                                 TransactionState.TxnCoordinator coordinator,
                                 TransactionState.LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws DuplicatedRequestException, LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        checkDatabaseDataQuota();
        writeLock();
        try {
            Preconditions.checkNotNull(coordinator);
            Preconditions.checkNotNull(label);
            FeNameFormat.checkLabel(label);

            /*
             * Check if label already used, by following steps
             * 1. get all existing transactions
             * 2. if there is a PREPARE transaction, check if this is a retry request. If yes, return the
             *    existing txn id.
             * 3. if there is a non-aborted transaction, throw label already used exception.
             */
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds != null && !existingTxnIds.isEmpty()) {
                List<TransactionState> notAbortedTxns = Lists.newArrayList();
                for (long txnId : existingTxnIds) {
                    TransactionState txn = unprotectedGetTransactionState(txnId);
                    Preconditions.checkNotNull(txn);
                    if (txn.getTransactionStatus() != TransactionStatus.ABORTED) {
                        notAbortedTxns.add(txn);
                    }
                }
                // there should be at most 1 txn in PREPARE/COMMITTED/VISIBLE status
                Preconditions.checkState(notAbortedTxns.size() <= 1, notAbortedTxns);
                if (!notAbortedTxns.isEmpty()) {
                    TransactionState notAbortedTxn = notAbortedTxns.get(0);
                    if (requestId != null && notAbortedTxn.getTransactionStatus() == TransactionStatus.PREPARE
                            && notAbortedTxn.getRequestId() != null && notAbortedTxn.getRequestId().equals(requestId)) {
                        // this may be a retry request for same job, just return existing txn id.
                        throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                                notAbortedTxn.getTransactionId(), "");
                    }
                    throw new LabelAlreadyUsedException(label, notAbortedTxn.getTransactionStatus());
                }
            }

            checkRunningTxnExceedLimit(sourceType);

            long tid = idGenerator.getNextTransactionId();
            LOG.info("begin transaction: txn_id: {} with label {} from coordinator {}, listner id: {}",
                    tid, label, coordinator, listenerId);
            TransactionState transactionState =
                    new TransactionState(dbId, tableIdList, tid, label, requestId, sourceType,
                            coordinator, listenerId, timeoutSecond * 1000);
            transactionState.setPrepareTime(System.currentTimeMillis());
            unprotectUpsertTransactionState(transactionState, false);

            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
            }

            return tid;
        } catch (DuplicatedRequestException e) {
            throw e;
        } catch (Exception e) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_REJECT.increase(1L);
            }
            throw e;
        } finally {
            writeUnlock();
        }
    }

    private void checkDatabaseDataQuota() throws AnalysisException {
        Database db = globalStateMgr.getDb(dbId);
        if (db == null) {
            throw new AnalysisException("Database[" + dbId + "] does not exist");
        }

        if (usedQuotaDataBytes == -1) {
            usedQuotaDataBytes = db.getUsedDataQuotaWithLock();
        }

        long dataQuotaBytes = db.getDataQuota();
        if (usedQuotaDataBytes >= dataQuotaBytes) {
            Pair<Double, String> quotaUnitPair = DebugUtil.getByteUint(dataQuotaBytes);
            String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " "
                    + quotaUnitPair.second;
            throw new AnalysisException("Database[" + db.getOriginName()
                    + "] data size exceeds quota[" + readableQuota + "]");
        }
    }

    public void updateDatabaseUsedQuotaData(long usedQuotaDataBytes) {
        this.usedQuotaDataBytes = usedQuotaDataBytes;
    }

    /**
     * commit transaction process as follows:
     * 1. validate whether `Load` is cancelled
     * 2. validate whether `Table` is deleted
     * 3. validate replicas consistency
     * 4. update transaction state version
     * 5. persistent transactionState
     * 6. update nextVersion because of the failure of persistent transaction resulting in error version
     */
    public VisibleStateWaiter commitTransaction(long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                                List<TabletFailInfo> tabletFailInfos,
                                                TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        // 1. check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = globalStateMgr.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }

        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }
        if (transactionState == null) {
            throw new TransactionCommitFailedException("transaction not found");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionCommitFailedException(transactionState.getReason());
        }
        VisibleStateWaiter waiter = new VisibleStateWaiter(transactionState);
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            LOG.debug("transaction is already visible: {}", transactionId);
            return waiter;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            LOG.debug("transaction is already committed: {}", transactionId);
            return waiter;
        }
        // For compatible reason, the default behavior of empty load is still returning "all partitions have no load data" and abort transaction.
        if (Config.empty_load_as_error && (tabletCommitInfos == null || tabletCommitInfos.isEmpty())
                && transactionState.getSourceType() != TransactionState.LoadJobSourceType.INSERT_STREAMING) {
            throw new TransactionCommitFailedException(TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
        }
        if (tabletCommitInfos != null && !tabletCommitInfos.isEmpty()) {
            transactionState.setTabletCommitInfos(tabletCommitInfos);
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        Span txnSpan = transactionState.getTxnSpan();
        txnSpan.setAttribute("db", db.getOriginName());
        StringBuilder tableListString = new StringBuilder();
        txnSpan.addEvent("commit_start");

        if (transactionState.getTableIdList().isEmpty()) {
            // Defensive programming, there have been instances where the upper layer caller forgot to set tableIdList.
            Set<Long> tableSet = Sets.newHashSet();
            List<Long> tabletIds = tabletCommitInfos.stream().map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
            List<TabletMeta> tabletMetaList = globalStateMgr.getTabletInvertedIndex().getTabletMetaList(tabletIds);
            for (TabletMeta meta : tabletMetaList) {
                if (meta != TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                    tableSet.add(meta.getTableId());
                }
            }
            transactionState.setTableIdList(Lists.newArrayList(tableSet));
        }

        List<TransactionStateListener> stateListeners = Lists.newArrayList();
        for (Long tableId : transactionState.getTableIdList()) {
            Table table = db.getTable(tableId);
            if (table == null) {
                // this can happen when tableId == -1 (tablet being dropping)
                // or table really not exist.
                continue;
            }
            TransactionStateListener listener = stateListenerFactory.create(this, table);
            if (listener == null) {
                throw new TransactionCommitFailedException(table.getName() + " does not support write");
            }
            listener.preCommit(transactionState, tabletCommitInfos, tabletFailInfos);
            if (tableListString.length() != 0) {
                tableListString.append(',');
            }
            tableListString.append(table.getName());
            stateListeners.add(listener);
        }
        txnSpan.setAttribute("tables", tableListString.toString());

        // before state transform
        TxnStateChangeCallback callback = transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;

        Span unprotectedCommitSpan = TraceManager.startSpan("unprotectedCommitTransaction", txnSpan);

        writeLock();
        try {
            unprotectedCommitTransaction(transactionState, stateListeners);
            txnOperated = true;
        } finally {
            writeUnlock();
            int numPartitions = 0;
            for (Map.Entry<Long, TableCommitInfo> entry : transactionState.getIdToTableCommitInfos().entrySet()) {
                numPartitions += entry.getValue().getIdToPartitionCommitInfo().size();
            }
            txnSpan.setAttribute("num_partition", numPartitions);
            unprotectedCommitSpan.end();
            // after state transform
            transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated, callback, null);
        }
        transactionState.prepareFinishChecker(db);

        // 6. update nextVersion because of the failure of persistent transaction resulting in error version
        Span updateCatalogAfterCommittedSpan = TraceManager.startSpan("updateCatalogAfterCommitted", txnSpan);
        try {
            updateCatalogAfterCommitted(transactionState, db);
        } finally {
            updateCatalogAfterCommittedSpan.end();
        }
        LOG.info("transaction:[{}] successfully committed", transactionState);
        return waiter;
    }

    /**
     * pre commit transaction process as follows:
     * 1. validate whether `Load` is cancelled
     * 2. validate whether `Table` is deleted
     * 3. validate replicas consistency
     * 4. persistent transactionState
     */
    public void prepareTransaction(long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                   List<TabletFailInfo> tabletFailInfos,
                                   TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        // 1. check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = globalStateMgr.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }

        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }
        if (transactionState == null) {
            throw new TransactionCommitFailedException("transaction not found");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionCommitFailedException(transactionState.getReason());
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            LOG.debug("transaction is already visible: {}", transactionId);
            return;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            LOG.debug("transaction is already committed: {}", transactionId);
            return;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.PREPARED) {
            LOG.debug("transaction is already prepared: {}", transactionId);
            return;
        }
        // For compatible reason, the default behavior of empty load is still returning "all partitions have no load data" and abort transaction.
        if (Config.empty_load_as_error && (tabletCommitInfos == null || tabletCommitInfos.isEmpty())) {
            throw new TransactionCommitFailedException(TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        Span txnSpan = transactionState.getTxnSpan();
        txnSpan.setAttribute("db", db.getFullName());
        StringBuilder tableListString = new StringBuilder();
        txnSpan.addEvent("pre_commit_start");

        if (transactionState.getTableIdList().isEmpty()) {
            // Defensive programming, there have been instances where the upper layer caller forgot to set tableIdList.
            Set<Long> tableSet = Sets.newHashSet();
            List<Long> tabletIds = tabletCommitInfos.stream().map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
            List<TabletMeta> tabletMetaList = globalStateMgr.getTabletInvertedIndex().getTabletMetaList(tabletIds);
            for (TabletMeta meta : tabletMetaList) {
                if (meta != TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                    tableSet.add(meta.getTableId());
                }
            }
            transactionState.setTableIdList(Lists.newArrayList(tableSet));
        }

        List<TransactionStateListener> stateListeners = Lists.newArrayList();
        for (Long tableId : transactionState.getTableIdList()) {
            Table table = db.getTable(tableId);
            if (table == null) {
                // this can happen when tableId == -1 (tablet being dropping)
                // or table really not exist.
                continue;
            }
            TransactionStateListener listener = stateListenerFactory.create(this, table);
            if (listener == null) {
                throw new TransactionCommitFailedException(table.getName() + " does not support write");
            }
            listener.preCommit(transactionState, tabletCommitInfos, tabletFailInfos);
            if (tableListString.length() != 0) {
                tableListString.append(',');
            }
            tableListString.append(table.getName());
            stateListeners.add(listener);
        }

        // before state transform
        TxnStateChangeCallback callback = transactionState.beforeStateTransform(TransactionStatus.PREPARED);
        boolean txnOperated = false;
        txnSpan.setAttribute("tables", tableListString.toString());

        Span unprotectedCommitSpan = TraceManager.startSpan("unprotectedPreparedTransaction", txnSpan);

        writeLock();
        try {
            unprotectedPrepareTransaction(transactionState, stateListeners);
            txnOperated = true;
        } finally {
            writeUnlock();
            int numPartitions = 0;
            for (Map.Entry<Long, TableCommitInfo> entry : transactionState.getIdToTableCommitInfos().entrySet()) {
                numPartitions += entry.getValue().getIdToPartitionCommitInfo().size();
            }
            txnSpan.setAttribute("num_partition", numPartitions);
            unprotectedCommitSpan.end();
            // after state transform
            transactionState.afterStateTransform(TransactionStatus.PREPARED, txnOperated, callback, null);
        }

        LOG.info("transaction:[{}] successfully prepare", transactionState);
    }

    public VisibleStateWaiter commitPreparedTransaction(long transactionId)
            throws UserException {
        // 1. check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = globalStateMgr.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }

        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }
        if (transactionState == null) {
            throw new TransactionCommitFailedException("transaction not found");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionCommitFailedException(transactionState.getReason());
        }
        VisibleStateWaiter waiter = new VisibleStateWaiter(transactionState);
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            LOG.debug("transaction is already visible: {}", transactionId);
            return waiter;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            LOG.debug("transaction is already committed: {}", transactionId);
            return waiter;
        }

        Span txnSpan = transactionState.getTxnSpan();
        txnSpan.setAttribute("db", db.getFullName());
        StringBuilder tableListString = new StringBuilder();
        txnSpan.addEvent("commit_start");

        List<TransactionStateListener> stateListeners = Lists.newArrayList();
        for (Long tableId : transactionState.getTableIdList()) {
            Table table = db.getTable(tableId);
            if (table == null) {
                // this can happen when tableId == -1 (tablet being dropping)
                // or table really not exist.
                continue;
            }
            if (tableListString.length() != 0) {
                tableListString.append(',');
            }
            tableListString.append(table.getName());
        }

        txnSpan.setAttribute("tables", tableListString.toString());

        // before state transform
        TxnStateChangeCallback callback = transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;

        Span unprotectedCommitSpan = TraceManager.startSpan("unprotectedCommitPreparedTransaction", txnSpan);

        writeLock();
        try {
            unprotectedCommitPreparedTransaction(transactionState, db);
            txnOperated = true;
        } finally {
            writeUnlock();
            int numPartitions = 0;
            for (Map.Entry<Long, TableCommitInfo> entry : transactionState.getIdToTableCommitInfos().entrySet()) {
                numPartitions += entry.getValue().getIdToPartitionCommitInfo().size();
            }
            txnSpan.setAttribute("num_partition", numPartitions);
            unprotectedCommitSpan.end();
            // after state transform
            transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated, callback, null);
        }

        // 6. update nextVersion because of the failure of persistent transaction resulting in error version
        Span updateCatalogAfterCommittedSpan = TraceManager.startSpan("updateCatalogAfterCommitted", txnSpan);
        try {
            updateCatalogAfterCommitted(transactionState, db);
        } finally {
            updateCatalogAfterCommittedSpan.end();
        }
        LOG.info("transaction:[{}] successfully committed", transactionState);
        return waiter;
    }

    public void deleteTransaction(TransactionState transactionState) {
        writeLock();
        try {
            // here we only delete the oldest element, so if element exist in finalStatusTransactionStateDeque,
            // it must at the front of the finalStatusTransactionStateDeque
            if (!finalStatusTransactionStateDeque.isEmpty() &&
                    transactionState.getTransactionId() ==
                            finalStatusTransactionStateDeque.getFirst().getTransactionId()) {
                finalStatusTransactionStateDeque.pop();
                clearTransactionState(transactionState);
            }
        } finally {
            writeUnlock();
        }
    }

    public TransactionStatus getLabelState(String label) {
        readLock();
        try {
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return TransactionStatus.UNKNOWN;
            }
            // find the latest txn (which id is largest)
            long maxTxnId = existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf)).orElse(Long.MIN_VALUE);
            return unprotectedGetTransactionState(maxTxnId).getTransactionStatus();
        } finally {
            readUnlock();
        }
    }

    public Long getLabelTxnID(String label) {
        readLock();
        try {
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return (long) -1;
            }
            // find the latest txn (which id is largest)
            Optional<Long> v = existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf));
            if (v.isPresent()) {
                return v.get();
            } else {
                return (long) -1;
            }
        } finally {
            readUnlock();
        }
    }

    public List<TransactionState> getCommittedTxnList() {
        readLock();
        try {
            // only send task to committed transaction
            return idToRunningTransactionState.values().stream()
                    .filter(transactionState -> (transactionState.getTransactionStatus() ==
                            TransactionStatus.COMMITTED))
                    .sorted(Comparator.comparing(TransactionState::getCommitTime))
                    .collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    public List<TransactionState> getReadyToPublishTxnList() {
        readLock();
        try {
            List<Long> txnIds = transactionGraph.getTxnsWithoutDependency();
            return txnIds.stream().map(id -> idToRunningTransactionState.get(id)).collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    // check whether transaction can be finished or not
    // for each tablet of load txn, if most replicas version publish successed
    // the trasaction can be treated as successful and can be finished
    public boolean canTxnFinished(TransactionState txn, Set<Long> errReplicas, Set<Long> unfinishedBackends) {
        Database db = globalStateMgr.getDb(txn.getDbId());
        if (db == null) {
            return true;
        }
        db.readLock();
        long currentTs = System.currentTimeMillis();
        try {
            // check each table involved in transaction
            for (TableCommitInfo tableCommitInfo : txn.getIdToTableCommitInfos().values()) {
                long tableId = tableCommitInfo.getTableId();
                OlapTable table = (OlapTable) db.getTable(tableId);
                // table maybe dropped between commit and publish, ignore it
                // it will be processed in finishTransaction
                if (table == null) {
                    continue;
                }
                PartitionInfo partitionInfo = table.getPartitionInfo();
                for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                    long partitionId = partitionCommitInfo.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    // partition maybe dropped between commit and publish version, ignore it
                    if (partition == null) {
                        continue;
                    }

                    if (partition.getVisibleVersion() != partitionCommitInfo.getVersion() - 1) {
                        return false;
                    }

                    List<MaterializedIndex> allIndices = txn.getPartitionLoadedTblIndexes(tableId, partition);
                    int quorumNum = partitionInfo.getQuorumNum(partitionId, table.writeQuorum());
                    int replicaNum = partitionInfo.getReplicationNum(partitionId);
                    for (MaterializedIndex index : allIndices) {
                        for (Tablet tablet : index.getTablets()) {
                            int successHealthyReplicaNum = 0;
                            // if most replica's version have been updated to version published
                            // which means publish version task finished in replica
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                if (!errReplicas.contains(replica.getId())) {
                                    // if replica not in can load state, skip it.
                                    if (!replica.getState().canLoad()) {
                                        continue;
                                    }
                                    // success healthy replica condition:
                                    // 1. version is equal to partition's visible version
                                    // 2. publish version task in this replica has finished
                                    if (replica.checkVersionCatchUp(partition.getVisibleVersion(), true)
                                            && replica.getLastFailedVersion() < 0
                                            && (unfinishedBackends == null
                                            || !unfinishedBackends.contains(replica.getBackendId()))) {
                                        ++successHealthyReplicaNum;
                                        // replica report version has greater cur transaction commit version
                                        // This can happen when the BE publish succeeds but fails to send a response to FE
                                    } else if (replica.getVersion() >= partitionCommitInfo.getVersion()) {
                                        ++successHealthyReplicaNum;
                                    } else if (unfinishedBackends != null
                                            && unfinishedBackends.contains(replica.getBackendId())) {
                                        errReplicas.add(replica.getId());
                                    }
                                } else if (replica.getVersion() >= partitionCommitInfo.getVersion()) {
                                    // the replica's version is larger than or equal to current transaction partition's version
                                    // the replica is normal, then remove it from error replica ids
                                    // this branch will be true if BE's replica reports it's version to FE
                                    // after publish version succeed in BE
                                    errReplicas.remove(replica.getId());
                                    ++successHealthyReplicaNum;
                                }
                            }
                            if (successHealthyReplicaNum < quorumNum) {
                                return false;
                            }
                            // quorum publish will make table unstable
                            // so that we wait quorum_publish_wait_time_ms util all backend publish finish
                            // before quorum publish
                            if (successHealthyReplicaNum != replicaNum
                                    && CollectionUtils.isNotEmpty(unfinishedBackends)
                                    && currentTs
                                    - txn.getCommitTime() < Config.quorom_publish_wait_time_ms) {

                                // if all unfinished backends already down through heartbeat detect, we don't need to wait anymore
                                for (Long backendID : unfinishedBackends) {
                                    if (globalStateMgr.getCurrentSystemInfo().checkBackendAlive(backendID)) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }
        return true;
    }

    public void finishTransaction(long transactionId, Set<Long> errorReplicaIds) throws UserException {
        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }
        // add all commit errors and publish errors to a single set
        if (errorReplicaIds == null) {
            errorReplicaIds = Sets.newHashSet();
        }
        Set<Long> originalErrorReplicas = transactionState.getErrorReplicas();
        if (originalErrorReplicas != null) {
            errorReplicaIds.addAll(originalErrorReplicas);
        }

        Database db = globalStateMgr.getDb(transactionState.getDbId());
        if (db == null) {
            writeLock();
            try {
                transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                transactionState.setReason("db is dropped");
                LOG.warn("db is dropped during transaction, abort transaction {}", transactionState);
                unprotectUpsertTransactionState(transactionState, false);
                return;
            } finally {
                writeUnlock();
            }
        }
        Span finishSpan = TraceManager.startSpan("finishTransaction", transactionState.getTxnSpan());
        db.writeLock();
        try {
            boolean hasError = false;
            for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
                long tableId = tableCommitInfo.getTableId();
                OlapTable table = (OlapTable) db.getTable(tableId);
                // table maybe dropped between commit and publish, ignore this error
                if (table == null) {
                    transactionState.removeTable(tableId);
                    LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}",
                            tableId,
                            transactionState);
                    continue;
                }
                PartitionInfo partitionInfo = table.getPartitionInfo();
                for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                    long partitionId = partitionCommitInfo.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    // partition maybe dropped between commit and publish version, ignore this error
                    if (partition == null) {
                        tableCommitInfo.removePartition(partitionId);
                        LOG.warn("partition {} is dropped, skip version check and remove it from transaction state {}",
                                partitionId,
                                transactionState);
                        continue;
                    }
                    if (partition.getVisibleVersion() != partitionCommitInfo.getVersion() - 1) {
                        // prevent excessive logging
                        if (transactionState.getLastErrTimeMs() + 3000 < System.nanoTime() / 1000000) {
                            LOG.debug("transactionId {} partition commitInfo version {} is not equal with " +
                                            "partition visible version {} plus one, need wait",
                                    transactionId,
                                    partitionCommitInfo.getVersion(),
                                    partition.getVisibleVersion());
                        }
                        String errMsg =
                                String.format("wait for publishing partition %d version %d. self version: %d. table %d",
                                        partitionId, partition.getVisibleVersion() + 1,
                                        partitionCommitInfo.getVersion(), tableId);
                        transactionState.setErrorMsg(errMsg);
                        return;
                    }

                    if (table.isCloudNativeTableOrMaterializedView()) {
                        continue;
                    }

                    int quorumReplicaNum = partitionInfo.getQuorumNum(partitionId, table.writeQuorum());

                    List<MaterializedIndex> allIndices =
                            transactionState.getPartitionLoadedTblIndexes(tableId, partition);
                    for (MaterializedIndex index : allIndices) {
                        for (Tablet tablet : index.getTablets()) {
                            int healthReplicaNum = 0;
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                if (!errorReplicaIds.contains(replica.getId())
                                        && replica.getLastFailedVersion() < 0) {
                                    // if replica not in can load state, skip it.
                                    if (!replica.getState().canLoad()) {
                                        continue;
                                    }
                                    // if replica not commit yet, skip it. This may happen when it's just create by clone.
                                    if (!transactionState.tabletCommitInfosContainsReplica(tablet.getId(), 
                                            replica.getBackendId())) {
                                        continue;
                                    }
                                    // this means the replica is a healthy replica,
                                    // it is healthy in the past and does not have error in current load
                                    if (replica.checkVersionCatchUp(partition.getVisibleVersion(), true)) {
                                        // during rollup, the rollup replica's last failed version < 0,
                                        // it may be treated as a normal replica.

                                        // Here we still update the replica's info even if we failed to publish
                                        // this txn, for the following case:
                                        // replica A,B,C is successfully committed, but only A is successfully
                                        // published,
                                        // B and C is crashed, now we need a Clone task to repair this tablet.
                                        // So, here we update A's version info, so that clone task will clone
                                        // the latest version of data.

                                        replica.updateRowCount(partitionCommitInfo.getVersion(),
                                                replica.getDataSize(), replica.getRowCount());
                                        ++healthReplicaNum;
                                    } else {
                                        // this means the replica has error in the past, but we did not observe it
                                        // during upgrade, one job maybe in quorum finished state, for example, A,B,C 3 replica
                                        // A,B 's version is 10, C's version is 10 but C' 10 is abnormal should be rollback
                                        // then we will detect this and set C's last failed version to 10 and last success version to 11
                                        // this logic has to be replayed in checkpoint thread
                                        replica.updateVersionInfo(replica.getVersion(),
                                                partition.getVisibleVersion(),
                                                partitionCommitInfo.getVersion());
                                        LOG.warn("transaction state {} has error, the replica [{}] not appeared " +
                                                        "in error replica list and its version not equal to partition " +
                                                        "commit version or commit version - 1 if its not a upgrate " +
                                                        "stage, its a fatal error. ",
                                                transactionState, replica);
                                    }
                                } else if (replica.getVersion() >= partitionCommitInfo.getVersion()) {
                                    // the replica's version is larger than or equal to current transaction partition's version
                                    // the replica is normal, then remove it from error replica ids
                                    errorReplicaIds.remove(replica.getId());
                                    ++healthReplicaNum;
                                }
                            }

                            if (healthReplicaNum < quorumReplicaNum) {
                                // prevent excessive logging
                                if (transactionState.getLastErrTimeMs() + 3000 < System.nanoTime() / 1000000) {
                                    LOG.info("publish version failed for transaction {} on tablet {}, with only {} " +
                                                    "replicas less than quorum {}", transactionState, tablet, healthReplicaNum,
                                            quorumReplicaNum);
                                }
                                String errMsg = String.format(
                                        "publish on tablet %d failed. succeed replica num %d less than quorum %d."
                                                + " table: %d, partition: %d, publish version: %d",
                                        tablet.getId(), healthReplicaNum, quorumReplicaNum, tableId, partitionId,
                                        partition.getVisibleVersion() + 1);
                                transactionState.setErrorMsg(errMsg);
                                hasError = true;
                            }
                        }
                    }
                }
            }
            if (hasError) {
                return;
            }
            boolean txnOperated = false;
            writeLock();
            try {
                transactionState.setErrorReplicas(errorReplicaIds);
                transactionState.setFinishTime(System.currentTimeMillis());
                transactionState.clearErrorMsg();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                unprotectUpsertTransactionState(transactionState, false);
                transactionState.notifyVisible();
                txnOperated = true;
                // TODO(cmy): We found a very strange problem. When delete-related transactions are processed here,
                // subsequent `updateCatalogAfterVisible()` is called, but it does not seem to be executed here
                // (because the relevant editlog does not see the log of visible transactions).
                // So I add a log here for observation.
                LOG.debug("after set transaction {} to visible", transactionState);
            } finally {
                writeUnlock();
                transactionState.afterStateTransform(TransactionStatus.VISIBLE, txnOperated);
            }
            Span updateCatalogSpan = TraceManager.startSpan("updateCatalogAfterVisible", finishSpan);
            try {
                updateCatalogAfterVisible(transactionState, db);
            } finally {
                updateCatalogSpan.end();
            }
        } finally {
            db.writeUnlock();
            finishSpan.end();
        }
        LOG.info("finish transaction {} successfully", transactionState);
    }

    protected void unprotectedCommitTransaction(TransactionState transactionState,
                                                List<TransactionStateListener> stateListeners) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // commit timestamps needs to be strictly monotonically increasing
        long commitTs = Math.max(System.currentTimeMillis(), maxCommitTs + 1);
        transactionState.setCommitTime(commitTs);
        // update transaction state version
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);

        for (TransactionStateListener listener : stateListeners) {
            listener.preWriteCommitLog(transactionState);
        }

        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);

        for (TransactionStateListener listener : stateListeners) {
            listener.postWriteCommitLog(transactionState);
        }
    }

    protected void unprotectedPrepareTransaction(TransactionState transactionState,
                                                 List<TransactionStateListener> stateListeners) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        long commitTs = System.currentTimeMillis();
        transactionState.setCommitTime(commitTs);
        // update transaction state version
        transactionState.setTransactionStatus(TransactionStatus.PREPARED);

        for (TransactionStateListener listener : stateListeners) {
            listener.preWriteCommitLog(transactionState);
        }

        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);

        for (TransactionStateListener listener : stateListeners) {
            listener.postWriteCommitLog(transactionState);
        }
    }

    protected void unprotectedCommitPreparedTransaction(TransactionState transactionState, Database db) {
        // transaction state is modified during check if the transaction could be committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARED) {
            return;
        }
        // commit timestamps needs to be strictly monotonically increasing
        long commitTs = Math.max(System.currentTimeMillis(), maxCommitTs + 1);
        transactionState.setCommitTime(commitTs);
        // update transaction state version
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);

        Iterator<TableCommitInfo> tableCommitInfoIterator = transactionState.getIdToTableCommitInfos().values().iterator();
        while (tableCommitInfoIterator.hasNext()) {
            TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTable(tableId);
            // table maybe dropped between commit and publish, ignore this error
            if (table == null) {
                transactionState.removeTable(tableId);
                LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}",
                        tableId,
                        transactionState);
                continue;
            }
            Iterator<PartitionCommitInfo> partitionCommitInfoIterator = tableCommitInfo.getIdToPartitionCommitInfo()
                    .values().iterator();
            while (partitionCommitInfoIterator.hasNext()) {
                PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                // partition maybe dropped between commit and publish version, ignore this error
                if (partition == null) {
                    partitionCommitInfoIterator.remove();
                    LOG.warn("partition {} is dropped, skip and remove it from transaction state {}",
                            partitionId,
                            transactionState);
                    continue;
                }
                partitionCommitInfo.setVersion(partition.getNextVersion());
                partitionCommitInfo.setVersionTime(table.isCloudNativeTable() ? 0 : commitTs);
            }
        }

        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);

    }

    // for add/update/delete TransactionState
    protected void unprotectUpsertTransactionState(TransactionState transactionState, boolean isReplay) {
        // if this is a replay operation, we should not log it
        if (!isReplay) {
            if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE
                    || transactionState.getSourceType() == TransactionState.LoadJobSourceType.FRONTEND) {
                // if this is a prepared txn, and load source type is not FRONTEND
                // no need to persist it. if prepare txn lost, the following commit will just be failed.
                // user only need to retry this txn.
                // The FRONTEND type txn is committed and running asynchronously, so we have to persist it.
                long start = System.currentTimeMillis();
                editLog.logInsertTransactionState(transactionState);
                LOG.debug("insert txn state for txn {}, current state: {}, cost: {}ms",
                        transactionState.getTransactionId(), transactionState.getTransactionStatus(),
                        System.currentTimeMillis() - start);
            }
        }
        // it's OK if getCommitTime() returns -1
        maxCommitTs = Math.max(maxCommitTs, transactionState.getCommitTime());
        if (!transactionState.getTransactionStatus().isFinalStatus()) {
            if (idToRunningTransactionState.put(transactionState.getTransactionId(), transactionState) == null) {
                if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK) {
                    runningRoutineLoadTxnNums++;
                } else {
                    runningTxnNums++;
                }
            }
            if (Config.enable_new_publish_mechanism && transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                transactionGraph.add(transactionState.getTransactionId(), transactionState.getTableIdList());
            }
        } else {
            if (idToRunningTransactionState.remove(transactionState.getTransactionId()) != null) {
                if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK) {
                    runningRoutineLoadTxnNums--;
                } else {
                    runningTxnNums--;
                }
            }
            transactionGraph.remove(transactionState.getTransactionId());
            idToFinalStatusTransactionState.put(transactionState.getTransactionId(), transactionState);
            finalStatusTransactionStateDeque.add(transactionState);
        }
        updateTxnLabels(transactionState);
    }

    private void updateTxnLabels(TransactionState transactionState) {
        Set<Long> txnIds = labelToTxnIds.computeIfAbsent(transactionState.getLabel(), k -> Sets.newHashSet());
        txnIds.add(transactionState.getTransactionId());
    }

    public void abortTransaction(String label, String reason) throws UserException {
        Preconditions.checkNotNull(label);
        long transactionId = -1;
        readLock();
        try {
            Set<Long> existingTxns = unprotectedGetTxnIdsByLabel(label);
            if (existingTxns == null || existingTxns.isEmpty()) {
                throw new TransactionNotFoundException("transaction not found, label=" + label);
            }
            // find PREPARE txn. For one load label, there should be only one PREPARE txn.
            TransactionState prepareTxn = null;
            for (Long txnId : existingTxns) {
                TransactionState txn = unprotectedGetTransactionState(txnId);
                if (txn.getTransactionStatus() == TransactionStatus.PREPARE) {
                    prepareTxn = txn;
                    break;
                }
            }

            if (prepareTxn == null) {
                throw new TransactionNotFoundException("running transaction not found, label=" + label);
            }

            transactionId = prepareTxn.getTransactionId();
        } finally {
            readUnlock();
        }
        abortTransaction(transactionId, reason, null);
    }

    public void abortTransaction(long transactionId, String reason, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        abortTransaction(transactionId, true, reason, txnCommitAttachment, Lists.newArrayList());
    }

    public void abortTransaction(long transactionId, String reason,
                                 TxnCommitAttachment txnCommitAttachment, List<TabletFailInfo> failedTablets)
            throws UserException {
        abortTransaction(transactionId, true, reason, txnCommitAttachment, failedTablets);
    }

    public void abortTransaction(long transactionId, boolean abortPrepared, String reason,
                                 TxnCommitAttachment txnCommitAttachment, List<TabletFailInfo> failedTablets)
            throws UserException {
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job, ignore abort operation",
                    transactionId);
            return;
        }
        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = idToRunningTransactionState.get(transactionId);
        } finally {
            readUnlock();
        }
        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction not found", transactionId);
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        // before state transform
        TxnStateChangeCallback callback = transactionState.beforeStateTransform(TransactionStatus.ABORTED);
        boolean txnOperated = false;
        writeLock();
        try {
            txnOperated = unprotectAbortTransaction(transactionId, abortPrepared, reason);
        } finally {
            writeUnlock();
            transactionState.afterStateTransform(TransactionStatus.ABORTED, txnOperated, callback, reason);
        }

        if (!txnOperated || transactionState.getTransactionStatus() != TransactionStatus.ABORTED) {
            return;
        }

        LOG.info("transaction:[{}] successfully rollback", transactionState);

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            return;
        }
        List<TransactionStateListener> listeners = Lists.newArrayListWithCapacity(transactionState.getTableIdList().size());
        db.readLock();
        try {
            for (Long tableId : transactionState.getTableIdList()) {
                Table table = db.getTable(tableId);
                if (table != null) {
                    TransactionStateListener listener = stateListenerFactory.create(this, table);
                    if (listener != null) {
                        listeners.add(listener);
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        for (TransactionStateListener listener : listeners) {
            listener.postAbort(transactionState, failedTablets);
        }
    }

    private boolean unprotectAbortTransaction(long transactionId, boolean abortPrepared, String reason)
            throws UserException {
        TransactionState transactionState = unprotectedGetTransactionState(transactionId);
        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction not found", transactionId);
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            return false;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.PREPARED && !abortPrepared) {
            return false;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            String msg = String.format("transaction %d state is %s, could not abort",
                    transactionId, transactionState.getTransactionStatus().toString());
            LOG.warn(msg);
            throw new TransactionAlreadyCommitException(msg);
        }
        transactionState.setFinishTime(System.currentTimeMillis());
        transactionState.setReason(reason);
        transactionState.setTransactionStatus(TransactionStatus.ABORTED);
        unprotectUpsertTransactionState(transactionState, false);
        return true;
    }

    protected List<List<Comparable>> getTableTransInfo(long txnId) throws AnalysisException {
        List<List<Comparable>> tableInfos = new ArrayList<>();
        readLock();
        try {
            TransactionState transactionState = unprotectedGetTransactionState(txnId);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + txnId + "] does not exist.");
            }

            for (Map.Entry<Long, TableCommitInfo> entry : transactionState.getIdToTableCommitInfos().entrySet()) {
                List<Comparable> tableInfo = new ArrayList<>();
                tableInfo.add(entry.getKey());
                tableInfo.add(Joiner.on(", ").join(entry.getValue().getIdToPartitionCommitInfo().values().stream().map(
                        PartitionCommitInfo::getPartitionId).collect(Collectors.toList())));
                tableInfos.add(tableInfo);
            }
        } finally {
            readUnlock();
        }
        return tableInfos;
    }

    protected List<List<Comparable>> getPartitionTransInfo(long txnId, long tableId) throws AnalysisException {
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        readLock();
        try {
            TransactionState transactionState = unprotectedGetTransactionState(txnId);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + txnId + "] does not exist.");
            }

            TableCommitInfo tableCommitInfo = transactionState.getIdToTableCommitInfos().get(tableId);
            Map<Long, PartitionCommitInfo> idToPartitionCommitInfo = tableCommitInfo.getIdToPartitionCommitInfo();
            for (Map.Entry<Long, PartitionCommitInfo> entry : idToPartitionCommitInfo.entrySet()) {
                List<Comparable> partitionInfo = new ArrayList<Comparable>();
                partitionInfo.add(entry.getKey());
                partitionInfo.add(entry.getValue().getVersion());
                partitionInfos.add(partitionInfo);
            }
        } finally {
            readUnlock();
        }
        return partitionInfos;
    }

    public void removeExpiredTxns(long currentMillis) {
        writeLock();
        try {
            StringBuilder expiredTxnMsgs = new StringBuilder(1024);
            String prefix = "";
            int numJobsToRemove = getTransactionNum() - Config.label_keep_max_num;
            while (!finalStatusTransactionStateDeque.isEmpty()) {
                TransactionState transactionState = finalStatusTransactionStateDeque.getFirst();
                if (transactionState.isExpired(currentMillis) || numJobsToRemove > 0) {
                    finalStatusTransactionStateDeque.pop();
                    clearTransactionState(transactionState);
                    --numJobsToRemove;
                    expiredTxnMsgs.append(prefix);
                    prefix = ", ";
                    expiredTxnMsgs.append(transactionState.getTransactionId());
                    if (expiredTxnMsgs.length() > 4096) {
                        LOG.info("transaction list [{}] are expired, remove them from transaction manager",
                                expiredTxnMsgs);
                        expiredTxnMsgs = new StringBuilder(1024);
                    }
                } else {
                    break;
                }
            }
            if (expiredTxnMsgs.length() > 0) {
                LOG.info("transaction list [{}] are expired, remove them from transaction manager",
                        expiredTxnMsgs);
            }
        } finally {
            writeUnlock();
        }
    }

    private void clearTransactionState(TransactionState transactionState) {
        idToFinalStatusTransactionState.remove(transactionState.getTransactionId());
        Set<Long> txnIds = unprotectedGetTxnIdsByLabel(transactionState.getLabel());
        txnIds.remove(transactionState.getTransactionId());
        if (txnIds.isEmpty()) {
            labelToTxnIds.remove(transactionState.getLabel());
        }
    }

    public int getTransactionNum() {
        return idToRunningTransactionState.size() + finalStatusTransactionStateDeque.size();
    }

    public List<Pair<Long, Long>> getTransactionIdByCoordinateBe(String coordinateHost, int limit) {
        ArrayList<Pair<Long, Long>> txnInfos = new ArrayList<>();
        readLock();
        try {
            idToRunningTransactionState.values().stream()
                    .filter(t -> (t.getCoordinator().sourceType == TransactionState.TxnSourceType.BE
                            && t.getCoordinator().ip.equals(coordinateHost)))
                    .limit(limit)
                    .forEach(t -> txnInfos.add(new Pair<>(t.getDbId(), t.getTransactionId())));
        } finally {
            readUnlock();
        }
        return txnInfos;
    }

    // get show info of a specified txnId
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        List<List<String>> infos = new ArrayList<List<String>>();
        readLock();
        try {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                throw new AnalysisException("Database[" + dbId + "] does not exist");
            }

            TransactionState txnState = unprotectedGetTransactionState(txnId);
            if (txnState == null) {
                throw new AnalysisException("transaction with id " + txnId + " does not exist");
            }

            if (ConnectContext.get() != null) {
                // check auth
                Set<Long> tblIds = txnState.getIdToTableCommitInfos().keySet();
                for (Long tblId : tblIds) {
                    Table tbl = db.getTable(tblId);
                    if (tbl != null) {
                        // won't check privilege in new RBAC framework
                        if (!GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
                            if (!GlobalStateMgr.getCurrentState().getAuth()
                                    .checkTblPriv(ConnectContext.get(), db.getFullName(),
                                            tbl.getName(), PrivPredicate.SHOW)) {
                                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                        "SHOW TRANSACTION",
                                        ConnectContext.get().getQualifiedUser(),
                                        ConnectContext.get().getRemoteIP(),
                                        tbl.getName());
                            }
                        }
                    }
                }
            }

            List<String> info = Lists.newArrayList();
            getTxnStateInfo(txnState, info);
            infos.add(info);
        } finally {
            readUnlock();
        }
        return infos;
    }

    protected void checkRunningTxnExceedLimit(TransactionState.LoadJobSourceType sourceType)
            throws BeginTransactionException {
        switch (sourceType) {
            case ROUTINE_LOAD_TASK:
                // no need to check limit for routine load task:
                // 1. the number of running routine load tasks is limited by Config.max_routine_load_task_num_per_be
                // 2. if we add routine load txn to runningTxnNums, runningTxnNums will always be occupied by routine load,
                //    and other txn may not be able to submitted.
                break;
            default:
                if (runningTxnNums >= Config.max_running_txn_num_per_db) {
                    throw new BeginTransactionException("current running txns on db " + dbId + " is "
                            + runningTxnNums + ", larger than limit " + Config.max_running_txn_num_per_db);
                }
                break;
        }
    }

    private void updateCatalogAfterCommitted(TransactionState transactionState, Database db) {
        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
            long tableId = tableCommitInfo.getTableId();
            Table table = db.getTable(tableId);
            TransactionLogApplier applier = txnLogApplierFactory.create(table);
            applier.applyCommitLog(transactionState, tableCommitInfo);
        }
    }

    private boolean updateCatalogAfterVisible(TransactionState transactionState, Database db) {
        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
            Table table = db.getTable(tableCommitInfo.getTableId());
            // table may be dropped by force after transaction committed
            // so that it will be a visible edit log after drop table
            if (table == null) {
                continue;
            }
            TransactionLogApplier applier = txnLogApplierFactory.create(table);
            applier.applyVisibleLog(transactionState, tableCommitInfo, db);
        }
        GlobalStateMgr.getCurrentAnalyzeMgr().updateLoadRows(transactionState);
        return true;
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, List<Long> tableIdList) {
        readLock();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
                if (entry.getValue().getDbId() != dbId || !isIntersectionNotEmpty(entry.getValue().getTableIdList(),
                        tableIdList) || !entry.getValue().isRunning()) {
                    continue;
                }
                if (entry.getKey() <= endTransactionId) {
                    LOG.debug("find a running txn with txn_id: {} on db: {}, less than watermark txn_id {}",
                            entry.getKey(), dbId, endTransactionId);
                    return false;
                }
            }
        } finally {
            readUnlock();
        }
        return true;
    }

    /**
     * check if there exists a intersection between the source tableId list and target tableId list
     * if one of them is null or empty, that means that we don't know related tables in tableList,
     * we think the two lists may have intersection for right ordered txns
     */
    public boolean isIntersectionNotEmpty(List<Long> sourceTableIdList, List<Long> targetTableIdList) {
        if (CollectionUtils.isEmpty(sourceTableIdList) || CollectionUtils.isEmpty(targetTableIdList)) {
            return true;
        }
        for (Long srcValue : sourceTableIdList) {
            for (Long targetValue : targetTableIdList) {
                if (srcValue.equals(targetValue)) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<Long> getTimeoutTxns(long currentMillis) {
        List<Long> timeoutTxns = Lists.newArrayList();
        readLock();
        try {
            for (TransactionState transactionState : idToRunningTransactionState.values()) {
                if (transactionState.isTimeout(currentMillis)) {
                    // txn is running but timeout, abort it.
                    timeoutTxns.add(transactionState.getTransactionId());
                }
            }
        } finally {
            readUnlock();
        }
        return timeoutTxns;
    }

    public void abortTimeoutTxns(long currentMillis) {
        List<Long> timeoutTxns = getTimeoutTxns(currentMillis);
        // abort timeout txns
        for (Long txnId : timeoutTxns) {
            try {
                abortTransaction(txnId, "timeout by txn manager", null);
                LOG.info("transaction [" + txnId + "] is timeout, abort it by transaction manager");
            } catch (UserException e) {
                // abort may be failed. it is acceptable. just print a log
                LOG.warn("abort timeout txn {} failed. msg: {}", txnId, e.getMessage());
            }
        }
    }

    public void replayUpsertTransactionState(TransactionState transactionState) {
        writeLock();
        try {
            if (transactionState.getTransactionStatus() == TransactionStatus.UNKNOWN) {
                LOG.info("remove unknown transaction: {}", transactionState);
                return;
            }
            // set transaction status will call txn state change listener
            transactionState.replaySetTransactionStatus();
            Database db = globalStateMgr.getDb(transactionState.getDbId());
            if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                LOG.info("replay a committed transaction {}", transactionState);
                updateCatalogAfterCommitted(transactionState, db);
            } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                LOG.info("replay a visible transaction {}", transactionState);
                updateCatalogAfterVisible(transactionState, db);
            }
            unprotectUpsertTransactionState(transactionState, true);
            if (transactionState.isExpired(System.currentTimeMillis())) {
                LOG.info("remove expired transaction: {}", transactionState);
                deleteTransaction(transactionState);
            }
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getDbTransStateInfo() {
        List<List<String>> infos = Lists.newArrayList();
        readLock();
        try {
            infos.add(Lists.newArrayList("running", String.valueOf(
                    runningTxnNums + runningRoutineLoadTxnNums)));
            long finishedNum = getFinishedTxnNums();
            infos.add(Lists.newArrayList("finished", String.valueOf(finishedNum)));
        } finally {
            readUnlock();
        }
        return infos;
    }

    public void unprotectWriteAllTransactionStates(DataOutput out) throws IOException {
        for (TransactionState transactionState : idToRunningTransactionState.values()) {
            transactionState.write(out);
        }

        for (TransactionState transactionState : finalStatusTransactionStateDeque) {
            transactionState.write(out);
        }
    }

    GlobalStateMgr getGlobalStateMgr() {
        return globalStateMgr;
    }

    public void finishTransactionNew(TransactionState transactionState, Set<Long> publishErrorReplicas) throws UserException {
        Database db = globalStateMgr.getDb(transactionState.getDbId());
        if (db == null) {
            writeLock();
            try {
                transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                transactionState.setReason("db is dropped");
                LOG.warn("db is dropped during transaction, abort transaction {}", transactionState);
                unprotectUpsertTransactionState(transactionState, false);
                return;
            } finally {
                writeUnlock();
            }
        }
        Span finishSpan = TraceManager.startSpan("finishTransaction", transactionState.getTxnSpan());
        db.writeLock();
        finishSpan.addEvent("db_lock");
        try {
            boolean txnOperated = false;
            writeLock();
            finishSpan.addEvent("txnmgr_lock");
            try {
                transactionState.setErrorReplicas(publishErrorReplicas);
                transactionState.setFinishTime(System.currentTimeMillis());
                transactionState.clearErrorMsg();
                transactionState.setNewFinish();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                unprotectUpsertTransactionState(transactionState, false);
                transactionState.notifyVisible();
                txnOperated = true;
            } finally {
                writeUnlock();
                transactionState.afterStateTransform(TransactionStatus.VISIBLE, txnOperated);
            }
            Span updateCatalogSpan = TraceManager.startSpan("updateCatalogAfterVisible", finishSpan);
            try {
                updateCatalogAfterVisible(transactionState, db);
            } finally {
                updateCatalogSpan.end();
            }
        } finally {
            db.writeUnlock();
            finishSpan.end();
        }
        LOG.info("finish transaction {} successfully", transactionState);
    }

    public String getTxnPublishTimeoutDebugInfo(long txnId) {
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(txnId);
        } finally {
            readUnlock();
        }
        if (transactionState == null) {
            return "";
        }
        return transactionState.getPublishTimeoutDebugInfo();
    }
}
