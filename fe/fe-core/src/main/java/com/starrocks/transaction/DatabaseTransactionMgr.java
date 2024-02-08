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
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.TraceManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.replication.ReplicationTxnCommitAttachment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TUniqueId;
import io.opentelemetry.api.trace.Span;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * Transaction Manager in database level, as a component in GlobalTransactionMgr
 * DatabaseTransactionMgr mainly be responsible for the following content:
 * 1. provide read/write lock in database level
 * 2. provide basic txn infos interface in database level to GlobalTransactionMgr
 * 3. do some transaction management, such as add/update/delete transaction.
 * Attention: all api in DatabaseTransactionMgr should be only invoked by GlobalTransactionMgr
 */

public class DatabaseTransactionMgr {
    public static final String TXN_TIMEOUT_BY_MANAGER = "timeout by txn manager";
    private static final Logger LOG = LogManager.getLogger(DatabaseTransactionMgr.class);
    private final TransactionStateListenerFactory stateListenerFactory = new TransactionStateListenerFactory();
    private final TransactionLogApplierFactory txnLogApplierFactory = new TransactionLogApplierFactory();
    private final GlobalStateMgr globalStateMgr;
    private final EditLog editLog;

    // The id of the database that shapeless.the current transaction manager is responsible for
    private final long dbId;

    // not realtime usedQuota value to make a fast check for database data quota
    private volatile long usedQuotaDataBytes = -1;

    /*
     * transactionLock is used to control the access to database transaction manager data
     * Modifications to the following multiple data structures must be protected by this lock
     * */
    private final ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock(true);

    // count the number of running transactions of database, except for shapeless.the routine load txn
    private int runningTxnNums = 0;

    // count only the number of running routine load transactions of database
    private int runningRoutineLoadTxnNums = 0;

    /*
     * idToRunningTransactionState: transactionId -> running TransactionState
     * idToFinalStatusTransactionState: transactionId -> final status TransactionState
     * finalStatusTransactionStateDeque: to store transactionStates with final status
     * */
    private final Map<Long, TransactionState> idToRunningTransactionState = Maps.newHashMap();
    private final Map<Long, TransactionState> idToFinalStatusTransactionState = Maps.newHashMap();
    private final ArrayDeque<TransactionState> finalStatusTransactionStateDeque = new ArrayDeque<>();

    // store committed transactions' dependency relationships
    private final TransactionGraph transactionGraph = new TransactionGraph();

    /*
     * `labelToTxnIds` is used for checking if label already used. map label to transaction id
     * One label may correspond to multiple transactions, and only one is success.
     */
    private final Map<String, Set<Long>> labelToTxnIds = Maps.newHashMap();
    private long maxCommitTs = 0;

    public DatabaseTransactionMgr(long dbId, GlobalStateMgr globalStateMgr) {
        this.dbId = dbId;
        this.globalStateMgr = globalStateMgr;
        this.editLog = globalStateMgr.getEditLog();
    }

    /**
     * begin transaction and return new transaction id
     * <p>
     *
     * @param requestId is used to judge that whether the request is a internal retry request
     *                  if label already exist, and requestId are equal, we return the exist tid,
     *                  and consider this 'begin' as success. requestId == null is for compatibility
     * @return transaction id
     * @throws RunningTxnExceedException  when running transaction exceed limit
     * @throws DuplicatedRequestException when duplicate label
     */
    public long beginTransaction(List<Long> tableIdList, String label, TUniqueId requestId,
                                 TransactionState.TxnCoordinator coordinator,
                                 TransactionState.LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws DuplicatedRequestException, LabelAlreadyUsedException, RunningTxnExceedException, AnalysisException {
        checkDatabaseDataQuota();
        Preconditions.checkNotNull(coordinator);
        Preconditions.checkNotNull(label);
        FeNameFormat.checkLabel(label);

        long tid = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        LOG.info("begin transaction: txn_id: {} with label {} from coordinator {}, listner id: {}",
                tid, label, coordinator, listenerId);
        TransactionState transactionState = new TransactionState(dbId, tableIdList, tid, label, requestId, sourceType,
                coordinator, listenerId, timeoutSecond * 1000);
        transactionState.setPrepareTime(System.currentTimeMillis());

        transactionState.writeLock();
        try {
            writeLock();
            try {
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

                unprotectUpsertTransactionState(transactionState, false);

                if (MetricRepo.hasInit) {
                    MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
                }
            } catch (DuplicatedRequestException e) {
                throw e;
            } catch (Exception e) {
                if (MetricRepo.hasInit) {
                    MetricRepo.COUNTER_TXN_REJECT.increase(1L);
                }
                throw e;
            } finally {
                writeUnlock();
            }
            persistTxnStateInTxnLevelLock(transactionState);
            return tid;
        } finally {
            transactionState.writeUnlock();
        }
    }

    /**
     * Change the transaction status to Prepared, indicating that the data has been prepared and is waiting for commit
     * prepared transaction process as follows:
     * 1. validate whether `Load` is cancelled
     * 2. validate whether `Table` is deleted
     * 3. validate replicas consistency
     * 4. persistent transactionState
     *
     * @param transactionId     transactionId
     * @param tabletCommitInfos tabletCommitInfos
     */
    public void prepareTransaction(long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                   List<TabletFailInfo> tabletFailInfos,
                                   TxnCommitAttachment txnCommitAttachment,
                                   boolean writeEditLog)
            throws UserException {
        Preconditions.checkNotNull(tabletCommitInfos, "tabletCommitInfos is null");
        Preconditions.checkNotNull(tabletFailInfos, "tabletFailInfos is null");
        // 1. check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = globalStateMgr.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }

        TransactionState transactionState = getTransactionState(transactionId);
        if (transactionState == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        transactionState.writeLock();
        try {
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
            if (Config.empty_load_as_error && tabletCommitInfos.isEmpty()
                    && transactionState.getSourceType() != TransactionState.LoadJobSourceType.INSERT_STREAMING) {
                throw new TransactionCommitFailedException(TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
            }

            if (transactionState.getWriteEndTimeMs() < 0) {
                transactionState.setWriteEndTimeMs(System.currentTimeMillis());
            }

            // update transaction state extra if exists
            if (txnCommitAttachment != null) {
                transactionState.setTxnCommitAttachment(txnCommitAttachment);
            }

            Span txnSpan = transactionState.getTxnSpan();
            txnSpan.setAttribute("db", db.getFullName());
            txnSpan.addEvent("pre_commit_start");

            List<TransactionStateListener> stateListeners = populateTransactionStateListeners(transactionState, db);
            String tableNames = stateListeners.stream().map(TransactionStateListener::getTableName)
                    .collect(Collectors.joining(","));
            txnSpan.setAttribute("tables", tableNames);

            for (TransactionStateListener listener : stateListeners) {
                listener.preCommit(transactionState, tabletCommitInfos, tabletFailInfos);
            }

            TxnStateChangeCallback callback = transactionState.beforeStateTransform(TransactionStatus.PREPARED);
            boolean txnOperated = false;

            Span unprotectedCommitSpan = TraceManager.startSpan("unprotectedPreparedTransaction", txnSpan);

            writeLock();
            try {
                // transaction state is modified during check if the transaction could commit
                if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
                    return;
                }

                // update transaction state version
                transactionState.setTransactionStatus(TransactionStatus.PREPARED);
                transactionState.setPreparedTime(System.currentTimeMillis());

                for (TransactionStateListener listener : stateListeners) {
                    listener.preWriteCommitLog(transactionState);
                }

                // persist transactionState
                if (writeEditLog) {
                    unprotectUpsertTransactionState(transactionState, false);
                }

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

            persistTxnStateInTxnLevelLock(transactionState);

            LOG.info("transaction:[{}] successfully prepare", transactionState);
        } finally {
            transactionState.writeUnlock();
        }
    }

    /**
     * Change the transaction status to COMMITTED, indicating that the transaction has been committed
     * <p>
     * commit transaction process as follows:
     * 1. validate whether `Load` is cancelled
     * 2. validate whether `Table` is deleted
     * 3. validate replicas consistency
     * 4. update transaction state version
     * 5. persistent transactionState
     * 6. update nextVersion because of the failure of persistent transaction resulting in error version
     *
     * @param transactionId transactionId
     * @return a {@link VisibleStateWaiter} object used to wait for the transaction become visible.
     */
    @NotNull
    public VisibleStateWaiter commitPreparedTransaction(long transactionId) throws UserException {
        // 1. check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = globalStateMgr.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }

        TransactionState transactionState = getTransactionState(transactionId);
        if (transactionState == null) {
            throw new TransactionNotFoundException(transactionId);
        }
        transactionState.writeLock();
        try {
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
            transactionState.prepareFinishChecker(db);

            persistTxnStateInTxnLevelLock(transactionState);

            // 6. update nextVersion because of the failure of persistent transaction resulting in error version
            Span updateCatalogAfterCommittedSpan = TraceManager.startSpan("updateCatalogAfterCommitted", txnSpan);
            try {
                updateCatalogAfterCommitted(transactionState, db);
            } finally {
                updateCatalogAfterCommittedSpan.end();
            }
            LOG.info("transaction:[{}] successfully committed", transactionState);
            return waiter;
        } finally {
            transactionState.writeUnlock();
        }
    }

    /**
     * Merge prepare and commit phases and automatically commit transactions
     *
     * @param transactionId     transactionId
     * @param tabletCommitInfos tabletCommitInfos
     * @return a {@link VisibleStateWaiter} object used to wait for the transaction become visible.
     * @throws TransactionCommitFailedException when commit transaction failed
     * @note callers should get db.write lock before call this api
     */
    @NotNull
    public VisibleStateWaiter commitTransaction(long transactionId,
                                                @NotNull List<TabletCommitInfo> tabletCommitInfos,
                                                @NotNull List<TabletFailInfo> tabletFailInfos,
                                                @Nullable TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        prepareTransaction(transactionId, tabletCommitInfos, tabletFailInfos, txnCommitAttachment, false);
        return commitPreparedTransaction(transactionId);
    }

    /**
     * Abort transaction
     *
     * @param transactionId transactionId
     * @param reason        abort reason
     */
    public void abortTransaction(long transactionId, boolean abortPrepared, String reason,
                                 TxnCommitAttachment txnCommitAttachment,
                                 List<TabletCommitInfo> finishedTablets,
                                 List<TabletFailInfo> failedTablets)
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
            // If the transaction state does not exist, this task might have been aborted by
            // the txntimeoutchecker thread. We need to perform some additional work.
            processNotFoundTxn(transactionId, reason, txnCommitAttachment);
            throw new TransactionNotFoundException(transactionId);
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        // before state transform
        TxnStateChangeCallback callback = transactionState.beforeStateTransform(TransactionStatus.ABORTED);
        boolean txnOperated = false;

        transactionState.writeLock();
        try {
            writeLock();
            try {
                txnOperated = unprotectAbortTransaction(transactionId, abortPrepared, reason);
            } finally {
                writeUnlock();
                transactionState.afterStateTransform(TransactionStatus.ABORTED, txnOperated, callback, reason);
            }

            persistTxnStateInTxnLevelLock(transactionState);
        } finally {
            transactionState.writeUnlock();
        }

        if (!txnOperated || transactionState.getTransactionStatus() != TransactionStatus.ABORTED) {
            return;
        }

        LOG.info("transaction:[{}] successfully rollback", transactionState);

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            return;
        }
        for (Long tableId : transactionState.getTableIdList()) {
            Table table = db.getTable(tableId);
            if (table == null) {
                continue;
            }
            TransactionStateListener listener = stateListenerFactory.create(this, table);
            if (listener != null) {
                listener.postAbort(transactionState, finishedTablets, failedTablets);
            }
        }
    }

    /**
     * Delete transaction
     *
     * @param transactionState transactionState
     */
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

    public long getDbId() {
        return dbId;
    }

    public TransactionState getTransactionState(Long transactionId) {
        readLock();
        try {
            return unprotectedGetTransactionState(transactionId);
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
            return idToRunningTransactionState.keySet().stream().min(Comparator.comparing(Long::longValue));
        } finally {
            readUnlock();
        }
    }

    public Optional<Long> getMinActiveCompactionTxnId() {
        readLock();
        try {
            OptionalLong minId = idToRunningTransactionState.values().stream()
                    .filter(state -> state.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION)
                    .mapToLong(TransactionState::getTransactionId).min();
            return minId.isPresent() ? Optional.of(minId.getAsLong()) : Optional.empty();
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

    public TransactionState getLabelTransactionState(String label) {
        readLock();
        try {
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return null;
            }
            // find the latest txn (which id is largest)
            long maxTxnId = existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf)).orElse(Long.MIN_VALUE);
            return unprotectedGetTransactionState(maxTxnId);
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
            return txnIds.stream().map(idToRunningTransactionState::get).collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    public List<TransactionStateBatch> getReadyToPublishTxnListBatch() {
        List<TransactionStateBatch> result = new ArrayList<>();
        readLock();

        try {
            List<Long> txnIds = transactionGraph.getTxnsWithoutDependency();
            for (long txnId : txnIds) {
                List<Long> txnsWithDependency = transactionGraph.getTxnsWithTxnDependencyBatch(
                        Config.lake_batch_publish_min_version_num,
                        Config.lake_batch_publish_max_version_num, txnId);
                List<TransactionState> states = txnsWithDependency.stream().map(idToRunningTransactionState::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                if (states.isEmpty()) {
                    continue;
                }
                if (states.size() == 1) { // fast path: no batch
                    result.add(new TransactionStateBatch(states));
                    continue;
                }

                // Only single table transactions will be batched together.
                Preconditions.checkState(states.get(0).getTableIdList().size() == 1);

                long tableId = states.get(0).getTableIdList().get(0);

                // check whether version is consequent
                // for schema change will occupy a version
                Map<Long, PartitionCommitInfo> versions = new HashMap<>();

                outerLoop:
                for (int i = 0; i < states.size(); i++) {
                    TransactionState state = states.get(i);
                    TableCommitInfo tableInfo = state.getTableCommitInfo(tableId);
                    // TableCommitInfo could be null if the table has been dropped before this transaction is committed.
                    if (tableInfo == null) {
                        states = states.subList(0, Math.max(i, 1));
                        break;
                    }
                    Map<Long, PartitionCommitInfo> partitionInfoMap = tableInfo.getIdToPartitionCommitInfo();
                    for (Map.Entry<Long, PartitionCommitInfo> item : partitionInfoMap.entrySet()) {
                        PartitionCommitInfo currTxnInfo = item.getValue();
                        PartitionCommitInfo prevTxnInfo = versions.get(item.getKey());
                        if (prevTxnInfo != null && prevTxnInfo.getVersion() + 1 != currTxnInfo.getVersion()) {
                            assert i > 0;
                            // version is not consecutive
                            // may schema change occupy a version
                            states = states.subList(0, i);
                            break outerLoop;
                        }
                        versions.put(item.getKey(), currTxnInfo);
                    }
                }

                TransactionStateBatch batch = new TransactionStateBatch(states);
                result.add(batch);
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    // check whether transaction can be finished or not
    // for each tablet of load txn, if most replicas version publish successed
    // the trasaction can be treated as successful and can be finished
    public boolean canTxnFinished(TransactionState txn, Set<Long> errReplicas, Set<Long> unfinishedBackends) {
        Database db = globalStateMgr.getDb(txn.getDbId());
        if (db == null) {
            return true;
        }

        List<Long> tableIdList = txn.getTableIdList();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, tableIdList, LockType.READ);
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
                    PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                    // partition maybe dropped between commit and publish version, ignore it
                    if (partition == null) {
                        continue;
                    }

                    // The version of a replication transaction may not continuously
                    if (txn.getSourceType() != TransactionState.LoadJobSourceType.REPLICATION &&
                            partition.getVisibleVersion() != partitionCommitInfo.getVersion() - 1) {
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
                                    - txn.getCommitTime() < Config.quorum_publish_wait_time_ms) {

                                // if all unfinished backends already down through heartbeat detect, we don't need to wait anymore
                                for (Long backendID : unfinishedBackends) {
                                    if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                                            .checkBackendAlive(backendID)) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, tableIdList, LockType.READ);
        }
        return true;
    }

    public void finishTransaction(long transactionId, Set<Long> errorReplicaIds) throws UserException {
        TransactionState transactionState = getTransactionState(transactionId);
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
            transactionState.writeLock();
            try {
                writeLock();
                try {
                    transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                    transactionState.setReason("db is dropped");
                    LOG.warn("db is dropped during transaction, abort transaction {}", transactionState);
                    unprotectUpsertTransactionState(transactionState, false);
                } finally {
                    writeUnlock();
                }

                persistTxnStateInTxnLevelLock(transactionState);
                return;
            } finally {
                transactionState.writeUnlock();
            }
        }
        Span finishSpan = TraceManager.startSpan("finishTransaction", transactionState.getTxnSpan());

        List<Long> tableIdList = transactionState.getTableIdList();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, tableIdList, LockType.WRITE);
        try {
            transactionState.writeLock();
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
                        PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                        // partition maybe dropped between commit and publish version, ignore this error
                        if (partition == null) {
                            tableCommitInfo.removePartition(partitionId);
                            LOG.warn("partition {} is dropped, skip version check and remove it from transaction state {}",
                                    partitionId,
                                    transactionState);
                            continue;
                        }
                        // The version of a replication transaction may not continuously
                        if (transactionState.getSourceType() != TransactionState.LoadJobSourceType.REPLICATION &&
                                partition.getVisibleVersion() != partitionCommitInfo.getVersion() - 1) {
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
                                        // if replica not commit yet, skip it. This may happen when it's just create by clone.
                                        if (!transactionState.tabletCommitInfosContainsReplica(tablet.getId(),
                                                replica.getBackendId(), replica.getState())) {
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
                                                            "commit version or commit version - 1 if it's not a upgrade " +
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
                                                        "replicas less than quorum {}",
                                                transactionState, tablet, healthReplicaNum, quorumReplicaNum);
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

                persistTxnStateInTxnLevelLock(transactionState);

                Span updateCatalogSpan = TraceManager.startSpan("updateCatalogAfterVisible", finishSpan);
                try {
                    updateCatalogAfterVisible(transactionState, db);
                } finally {
                    updateCatalogSpan.end();
                }
            } finally {
                transactionState.writeUnlock();
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, tableIdList, LockType.WRITE);
            finishSpan.end();
        }

        collectStatisticsForStreamLoadOnFirstLoad(transactionState, db);

        LOG.info("finish transaction {} successfully", transactionState);
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
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                // partition maybe dropped between commit and publish version, ignore this error
                if (partition == null) {
                    partitionCommitInfoIterator.remove();
                    LOG.warn("partition {} is dropped, skip and remove it from transaction state {}",
                            partitionId,
                            transactionState);
                    continue;
                }
                if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.REPLICATION) {
                    Map<Long, Long> partitionVersions = ((ReplicationTxnCommitAttachment) transactionState
                            .getTxnCommitAttachment()).getPartitionVersions();
                    partitionCommitInfo.setVersion(partitionVersions.get(partitionCommitInfo.getPartitionId()));
                } else {
                    partitionCommitInfo.setVersion(partition.getNextVersion());
                }
                // versionTime has different meanings in shared data and shared nothing mode.
                // In shared nothing mode, versionTime is the time when the transaction was
                // committed, while in shared data mode, versionTime is the time when the
                // transaction was successfully published. This is a design error due to
                // carelessness, and should have been consistent here.
                partitionCommitInfo.setVersionTime(table.isCloudNativeTableOrMaterializedView() ? 0 : commitTs);
            }
        }

        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);
    }

    // for add/update/delete TransactionState
    protected void unprotectUpsertTransactionState(TransactionState transactionState, boolean isReplay) {
        // if this is a replay operation, we should not log it
        if (!isReplay && !Config.lock_manager_enable_loading_using_fine_granularity_lock) {
            doWriteTxnStateEditLog(transactionState);
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
            if ((Config.enable_new_publish_mechanism || RunMode.isSharedDataMode()) &&
                    transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
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

    private void persistTxnStateInTxnLevelLock(TransactionState transactionState) {
        if (Config.lock_manager_enable_loading_using_fine_granularity_lock) {
            doWriteTxnStateEditLog(transactionState);
        }
    }

    private void doWriteTxnStateEditLog(TransactionState transactionState) {
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

    // The status of stateBach is VISIBLE or ABORTED
    public void unprotectSetTransactionStateBatch(TransactionStateBatch stateBatch, boolean isReplay) {
        if (!isReplay && !Config.lock_manager_enable_loading_using_fine_granularity_lock) {
            long start = System.currentTimeMillis();
            editLog.logInsertTransactionStateBatch(stateBatch);
            LOG.debug("insert txn state visible for txnIds batch {}, cost: {}ms",
                    stateBatch.getTxnIds(), System.currentTimeMillis() - start);
        }

        for (TransactionState transactionState : stateBatch.getTransactionStates()) {
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
            updateTxnLabels(transactionState);
        }
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
                throw new TransactionNotFoundException(label);
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
                throw new TransactionNotFoundException(label);
            }

            transactionId = prepareTxn.getTransactionId();
        } finally {
            readUnlock();
        }
        abortTransaction(transactionId, reason, null);
    }

    public void abortAllRunningTransaction() throws UserException {
        for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
            abortTransaction(entry.getKey(), "The cluster is under safe mode!", null);
        }
    }

    public void abortTransaction(long transactionId, String reason, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        abortTransaction(transactionId, true, reason, txnCommitAttachment,
                Collections.emptyList(), Collections.emptyList());
    }

    private void processNotFoundTxn(long transactionId, String reason, TxnCommitAttachment txnCommitAttachment) {
        if (txnCommitAttachment == null) {
            return;
        }
        if (txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
            GlobalStateMgr.getCurrentState().getRoutineLoadMgr().setRoutineLoadJobOtherMsg(reason, txnCommitAttachment);
        }
    }

    private boolean unprotectAbortTransaction(long transactionId, boolean abortPrepared, String reason)
            throws UserException {
        TransactionState transactionState = unprotectedGetTransactionState(transactionId);
        if (transactionState == null) {
            throw new TransactionNotFoundException(transactionId);
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
        try {
            readLock();
            return idToRunningTransactionState.size() + finalStatusTransactionStateDeque.size();
        } finally {
            readUnlock();
        }
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

    public Long getTransactionNumByCoordinateBe(String coordinateHost) {
        readLock();
        try {
            return idToRunningTransactionState.values().stream()
                    .filter(t -> (t.getCoordinator().sourceType == TransactionState.TxnSourceType.BE
                            && t.getCoordinator().ip.equals(coordinateHost)))
                    .mapToLong(item -> 1).sum();
        } finally {
            readUnlock();
        }
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

            List<String> info = Lists.newArrayList();
            getTxnStateInfo(txnState, info);
            infos.add(info);
        } finally {
            readUnlock();
        }
        return infos;
    }

    protected void checkRunningTxnExceedLimit(TransactionState.LoadJobSourceType sourceType) throws RunningTxnExceedException {
        switch (sourceType) {
            case ROUTINE_LOAD_TASK:
                // no need to check limit for routine load task:
                // 1. the number of running routine load tasks is limited by Config.max_routine_load_task_num_per_be
                // 2. if we add routine load txn to runningTxnNums, runningTxnNums will always be occupied by routine load,
                //    and other txn may not be able to submitted.
                break;
            case LAKE_COMPACTION:
                // no need to check limit for cloud native table compaction.
                // high frequency and small batch loads may cause compaction execute rarely.
                break;
            default:
                if (runningTxnNums >= Config.max_running_txn_num_per_db) {
                    throw new RunningTxnExceedException("current running txns on db " + dbId + " is "
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
        try {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateLoadRows(transactionState);
        } catch (Throwable t) {
            LOG.warn("update load rows failed for txn: {}", transactionState, t);
        }
        return true;
    }

    // the write lock of database has been hold
    private boolean updateCatalogAfterVisibleBatch(TransactionStateBatch transactionStateBatch, Database db) {
        Table table = db.getTable(transactionStateBatch.getTableId());
        if (table == null) {
            return true;
        }
        TransactionLogApplier applier = txnLogApplierFactory.create(table);
        ((LakeTableTxnLogApplier) applier).applyVisibleLogBatch(transactionStateBatch, db);
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
                abortTransaction(txnId, TXN_TIMEOUT_BY_MANAGER, null);
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

    public void replayUpsertTransactionStateBatch(TransactionStateBatch transactionStateBatch) {
        writeLock();
        try {
            LOG.info("replay a transaction state batch{}", transactionStateBatch);
            Database db = globalStateMgr.getDb(transactionStateBatch.getDbId());
            updateCatalogAfterVisibleBatch(transactionStateBatch, db);

            unprotectSetTransactionStateBatch(transactionStateBatch, true);
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

    public void unprotectWriteAllTransactionStatesV2(SRMetaBlockWriter writer)
            throws IOException, SRMetaBlockException {
        for (TransactionState transactionState : idToRunningTransactionState.values()) {
            writer.writeJson(transactionState);
        }

        for (TransactionState transactionState : finalStatusTransactionStateDeque) {
            writer.writeJson(transactionState);
        }
    }

    GlobalStateMgr getGlobalStateMgr() {
        return globalStateMgr;
    }

    public void finishTransactionNew(TransactionState transactionState, Set<Long> publishErrorReplicas) throws UserException {
        Database db = globalStateMgr.getDb(transactionState.getDbId());
        if (db == null) {
            transactionState.writeLock();
            try {
                writeLock();
                try {
                    transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                    transactionState.setReason("db is dropped");
                    LOG.warn("db is dropped during transaction, abort transaction {}", transactionState);
                    unprotectUpsertTransactionState(transactionState, false);
                } finally {
                    writeUnlock();
                }

                persistTxnStateInTxnLevelLock(transactionState);
                return;
            } finally {
                transactionState.writeUnlock();
            }
        }

        Span finishSpan = TraceManager.startSpan("finishTransaction", transactionState.getTxnSpan());
        Locker locker = new Locker();
        List<Long> tableIdList = transactionState.getTableIdList();
        locker.lockTablesWithIntensiveDbLock(db, tableIdList, LockType.WRITE);
        finishSpan.addEvent("db_lock");
        try {
            transactionState.writeLock();
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
                persistTxnStateInTxnLevelLock(transactionState);

                Span updateCatalogSpan = TraceManager.startSpan("updateCatalogAfterVisible", finishSpan);
                try {
                    updateCatalogAfterVisible(transactionState, db);
                } finally {
                    updateCatalogSpan.end();
                }
            } finally {
                transactionState.writeUnlock();
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, tableIdList, LockType.WRITE);
            finishSpan.end();
        }

        collectStatisticsForStreamLoadOnFirstLoad(transactionState, db);

        LOG.info("finish transaction {} successfully", transactionState);

    }

    public void finishTransactionBatch(TransactionStateBatch stateBatch, Set<Long> errorReplicaIds) {
        Database db = globalStateMgr.getDb(stateBatch.getDbId());
        if (db == null) {
            stateBatch.writeLock();
            try {
                writeLock();
                try {
                    stateBatch.setTransactionStatus(TransactionStatus.ABORTED);
                    LOG.warn("db is dropped during transaction batch, abort transaction {}", stateBatch);
                    unprotectSetTransactionStateBatch(stateBatch, false);
                } finally {
                    writeUnlock();
                }
                if (Config.lock_manager_enable_loading_using_fine_granularity_lock) {
                    long start = System.currentTimeMillis();
                    editLog.logInsertTransactionStateBatch(stateBatch);
                    LOG.debug("insert txn state visible for txnIds batch {}, cost: {}ms",
                            stateBatch.getTxnIds(), System.currentTimeMillis() - start);
                }
                return;
            } finally {
                stateBatch.writeUnlock();
            }
        }
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            boolean txnOperated = false;
            stateBatch.writeLock();
            try {
                writeLock();
                try {
                    stateBatch.setTransactionVisibleInfo();
                    unprotectSetTransactionStateBatch(stateBatch, false);
                    txnOperated = true;
                } finally {
                    writeUnlock();
                    stateBatch.afterVisible(TransactionStatus.VISIBLE, txnOperated);
                }
                if (Config.lock_manager_enable_loading_using_fine_granularity_lock) {
                    long start = System.currentTimeMillis();
                    editLog.logInsertTransactionStateBatch(stateBatch);
                    LOG.debug("insert txn state visible for txnIds batch {}, cost: {}ms",
                            stateBatch.getTxnIds(), System.currentTimeMillis() - start);
                }

                updateCatalogAfterVisibleBatch(stateBatch, db);
            } finally {
                stateBatch.writeUnlock();
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        collectStatisticsForStreamLoadOnFirstLoadBatch(stateBatch, db);

        LOG.info("finish transaction {} batch successfully", stateBatch);
    }

    private void collectStatisticsForStreamLoadOnFirstLoad(TransactionState txnState, Database db) {
        TransactionState.LoadJobSourceType sourceType = txnState.getSourceType();
        if (!TransactionState.LoadJobSourceType.FRONTEND_STREAMING.equals(sourceType)
                && !TransactionState.LoadJobSourceType.BACKEND_STREAMING.equals(sourceType)) {
            return;
        }
        List<Table> tables = txnState.getIdToTableCommitInfos().values().stream()
                .map(TableCommitInfo::getTableId)
                .distinct()
                .map(db::getTable)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        for (Table table : tables) {
            StatisticUtils.triggerCollectionOnFirstLoad(txnState, db, table, false);
        }
    }

    private void collectStatisticsForStreamLoadOnFirstLoadBatch(TransactionStateBatch txnStateBatch, Database db) {
        for (TransactionState txnState : txnStateBatch.getTransactionStates()) {
            collectStatisticsForStreamLoadOnFirstLoad(txnState, db);
        }
    }

    public String getTxnPublishTimeoutDebugInfo(long txnId) {
        TransactionState transactionState = getTransactionState(txnId);
        if (transactionState == null) {
            return "";
        }
        return transactionState.getPublishTimeoutDebugInfo();
    }

    @NotNull
    private List<TransactionStateListener> populateTransactionStateListeners(@NotNull TransactionState transactionState,
                                                                             @NotNull Database database)
            throws TransactionException {
        List<TransactionStateListener> stateListeners = Lists.newArrayList();
        for (Long tableId : transactionState.getTableIdList()) {
            Table table = database.getTable(tableId);
            if (table == null) {
                // this can happen when tableId == -1 (tablet being dropping)
                // or table really not exist.
                continue;
            }
            TransactionStateListener listener = stateListenerFactory.create(this, table);
            if (listener == null) {
                throw new TransactionCommitFailedException(table.getName() + " does not support write");
            }
            stateListeners.add(listener);
        }
        return stateListeners;
    }

    public TTransactionStatus getTxnStatus(long txnId) {
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(txnId);
        } finally {
            readUnlock();
        }
        return Optional.ofNullable(transactionState)
                .map(TransactionState::getTransactionStatus)
                .map(TransactionStatus::toThrift)
                .orElse(TTransactionStatus.UNKNOWN);
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
            String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " " + quotaUnitPair.second;
            throw new AnalysisException("Database[" + db.getOriginName()
                    + "] data size exceeds quota[" + readableQuota + "]");
        }
    }

    public void updateDatabaseUsedQuotaData(long usedQuotaDataBytes) {
        this.usedQuotaDataBytes = usedQuotaDataBytes;
    }
}
