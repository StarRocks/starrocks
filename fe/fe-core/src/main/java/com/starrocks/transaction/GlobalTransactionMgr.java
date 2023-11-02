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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/GlobalTransactionMgr.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.meta.lock.LockTimeoutException;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * Transaction Manager
 * 1. begin
 * 2. commit
 * 3. abort
 * Attention: all api in txn manager should get db lock or load lock first, then get txn manager's lock, or
 * there will be dead lock
 */
public class GlobalTransactionMgr {
    private static final Logger LOG = LogManager.getLogger(GlobalTransactionMgr.class);

    private final Map<Long, DatabaseTransactionMgr> dbIdToDatabaseTransactionMgrs = Maps.newConcurrentMap();

    private TransactionIdGenerator idGenerator = new TransactionIdGenerator();
    private final TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();

    private final GlobalStateMgr globalStateMgr;

    private Map<Long, CreateTableTxnState> createTableTxnMap = Maps.newConcurrentMap();

    public GlobalTransactionMgr(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    @NotNull
    public DatabaseTransactionMgr getDatabaseTransactionMgr(long dbId) throws AnalysisException {
        DatabaseTransactionMgr databaseTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (databaseTransactionMgr == null) {
            throw new AnalysisException("databaseTransactionMgr[" + dbId + "] does not exist");
        }
        return databaseTransactionMgr;
    }

    public void addDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.putIfAbsent(dbId, new DatabaseTransactionMgr(dbId, globalStateMgr)) == null) {
            LOG.debug("add database transaction manager for db {}", dbId);
        }
    }

    public void removeDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.remove(dbId) != null) {
            LOG.debug("remove database transaction manager for db {}", dbId);
        }
    }

    /**
     * the app could specify the transaction id
     * <p>
     * requestId is used to judge that whether the request is a internal retry request
     * if label already exist, and requestId are equal, we return the exist tid, and consider this 'begin'
     * as success.
     * requestId == null is for compatibility
     *
     * @param coordinator
     * @throws RunningTxnExceedException
     * @throws DuplicatedRequestException
     * @throws IllegalTransactionParameterException
     */
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
                                 TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId,
                                 long timeoutSecond)
            throws LabelAlreadyUsedException, RunningTxnExceedException, DuplicatedRequestException, AnalysisException {

        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        if (GlobalStateMgr.getCurrentState().isSafeMode()) {
            throw new AnalysisException(String.format("The cluster is under safe mode state," +
                    " all load jobs are rejected."));
        }

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second, Config.min_load_timeout_second);
                break;
            case LAKE_COMPACTION:
                // skip transaction timeout range check for lake compaction
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }

        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.beginTransaction(tableIdList, label, requestId, coordinator, sourceType, listenerId,
                timeoutSecond);
    }

    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
                                 LoadJobSourceType sourceType,
                                 long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, RunningTxnExceedException, DuplicatedRequestException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
    }

    private void checkValidTimeoutSecond(long timeoutSecond, int maxLoadTimeoutSecond, int minLoadTimeOutSecond)
            throws AnalysisException {
        if (timeoutSecond > maxLoadTimeoutSecond ||
                timeoutSecond < minLoadTimeOutSecond) {
            throw new AnalysisException("Invalid timeout. Timeout should between "
                    + minLoadTimeOutSecond + " and " + maxLoadTimeoutSecond
                    + " seconds");
        }
    }

    public TransactionStatus getLabelStatus(long dbId, String label) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getLabelState(label);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction status by label " + label + " failed", e);
            return TransactionStatus.UNKNOWN;
        }
    }

    public Long getLabelTxnID(long dbId, String label) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getLabelTxnID(label);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction status by label " + label + " failed", e);
            return (long) -1;
        }
    }

    public TransactionState getLabelTransactionState(long dbId, String label) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getLabelTransactionState(label);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction state by label " + label + " failed", e);
            return null;
        }
    }

    /**
     * @param transactionId
     * @param tabletCommitInfos
     * @return a {@link VisibleStateWaiter} object used to wait for the transaction become visible.
     * @throws UserException
     * @throws TransactionCommitFailedException
     * @note it is necessary to optimize the `lock` mechanism and `lock` scope resulting from wait lock long time
     * @note callers should get db.write lock before call this api
     */
    @NotNull
    public VisibleStateWaiter commitTransaction(long dbId, long transactionId,
                                                @NotNull List<TabletCommitInfo> tabletCommitInfos,
                                                @NotNull List<TabletFailInfo> tabletFailInfos,
                                                @Nullable TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        LOG.debug("try to commit transaction: {}", transactionId);
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.commitTransaction(transactionId, tabletCommitInfos, tabletFailInfos,
                txnCommitAttachment);
    }

    public void prepareTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                   List<TabletFailInfo> tabletFailInfos,
                                   TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        LOG.debug("try to pre commit transaction: {}", transactionId);
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.prepareTransaction(transactionId, tabletCommitInfos, tabletFailInfos, txnCommitAttachment, true);
    }

    public void commitPreparedTransaction(long dbId, long transactionId, long timeoutMillis)
            throws UserException {

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            LOG.warn("Database {} does not exist", dbId);
            throw new UserException("Database[" + dbId + "] does not exist");
        }
        commitPreparedTransaction(db, transactionId, timeoutMillis);
    }

    public void commitPreparedTransaction(Database db, long transactionId, long timeoutMillis)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        LOG.debug("try to commit prepared transaction: {}", transactionId);

        VisibleStateWaiter waiter;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Locker locker = new Locker();
        if (!locker.tryLockDatabase(db, LockType.WRITE, timeoutMillis)) {
            throw new UserException("get database write lock timeout, database="
                    + db.getFullName() + ", timeoutMillis=" + timeoutMillis);
        }
        try {
            waiter = getDatabaseTransactionMgr(db.getId()).commitPreparedTransaction(transactionId);
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        stopWatch.stop();
        long publishTimeoutMillis = timeoutMillis - stopWatch.getTime();
        if (publishTimeoutMillis < 0) {
            // here commit transaction successfully cost too much time to cause publisTimeoutMillis is less than zero,
            // so we just return false to indicate publish timeout
            throw new UserException("publish timeout: " + timeoutMillis);
        }
        if (!waiter.await(publishTimeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("publish timeout: " + timeoutMillis);
        }
    }

    public boolean commitAndPublishTransaction(@NotNull Database db,
                                               long transactionId,
                                               @NotNull List<TabletCommitInfo> tabletCommitInfos,
                                               @NotNull List<TabletFailInfo> tabletFailInfos,
                                               long timeoutMillis) throws UserException {
        return commitAndPublishTransaction(db, transactionId, tabletCommitInfos, tabletFailInfos, timeoutMillis, null);
    }

    /**
     * Commit and wait for the transaction to become visible.
     *
     * @param db                  the database in which the transaction is being committed
     * @param transactionId       the ID of the transaction to commit
     * @param tabletCommitInfos   a list of tablet commit information
     * @param tabletFailInfos     a list of tablet fail information
     * @param timeoutMillis       the timeout for waiting for the transaction to become visible, in milliseconds
     * @param txnCommitAttachment an optional attachment to include in the transaction commit
     * @return {@code true} if the transaction becomes visible within the given timeout,
     * {@code false} otherwise
     * @throws UserException                    if an error occurs during the transaction commit
     * @throws TransactionCommitFailedException if the transaction commit fails due to a disabled load job
     * @note This method acquires the write lock on the database to commit transaction, callers should NOT already
     * hold the database lock when calling this method, otherwise it will lead to deadlock.
     */
    public boolean commitAndPublishTransaction(@NotNull Database db,
                                               long transactionId,
                                               @NotNull List<TabletCommitInfo> tabletCommitInfos,
                                               @NotNull List<TabletFailInfo> tabletFailInfos,
                                               long timeoutMillis,
                                               @Nullable TxnCommitAttachment txnCommitAttachment) throws UserException {
        long dueTime = timeoutMillis != 0 ? System.currentTimeMillis() + timeoutMillis : Long.MAX_VALUE;
        VisibleStateWaiter waiter = retryCommitOnRateLimitExceeded(db, transactionId, tabletCommitInfos,
                tabletFailInfos, txnCommitAttachment, timeoutMillis);
        long now = System.currentTimeMillis();
        return waiter.await(Math.max(dueTime - now, 0), TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the commit operation on the given transaction with retry mechanism
     * in case of rate limit exceeded exception.
     *
     * @param db                  the {@link Database} object where the transaction is being committed
     * @param transactionId       the ID of the transaction to commit
     * @param tabletCommitInfos   the list of {@link TabletCommitInfo} objects containing information about the tablets
     *                            to commit
     * @param tabletFailInfos     the list of {@link TabletFailInfo} objects containing information about the tablets that
     *                            failed to commit
     * @param txnCommitAttachment the optional {@link TxnCommitAttachment} object
     * @param timeoutMs           the timeout value in milliseconds for the commit operation
     * @return a {@link VisibleStateWaiter} object used to wait for the transaction to become visible
     * @throws UserException if an error occurs during the commit operation
     * @note This method acquires the write lock on the database to commit transaction, callers should NOT already
     * hold the database lock when calling this method, otherwise it will lead to deadlock.
     */
    @NotNull
    public VisibleStateWaiter retryCommitOnRateLimitExceeded(
            @NotNull Database db, long transactionId, @NotNull List<TabletCommitInfo> tabletCommitInfos,
            @NotNull List<TabletFailInfo> tabletFailInfos,
            @Nullable TxnCommitAttachment txnCommitAttachment,
            long timeoutMs) throws UserException {
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                return commitTransactionUnderDatabaseWLock(db, transactionId, tabletCommitInfos, tabletFailInfos,
                        txnCommitAttachment, timeoutMs);
            } catch (CommitRateExceededException e) {
                throttleCommitOnRateExceed(e, startTime, timeoutMs);
            } catch (LockTimeoutException e) {
                throw e;
            } catch (Exception e) {
                throw new UserException("fail to execute commit task: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Handles the case when the commit rate exceeds the limit.
     *
     * @param e           The CommitRateExceededException to handle.
     * @param startTimeMs The start time of the commit.
     * @param timeoutMs   The timeout duration in milliseconds. If timeoutMs is non-zero and the allowed commit time is
     *                    greater than (startTimeMs + timeoutMs), then CommitRateExceededException is re-thrown.
     * @throws CommitRateExceededException if the allowed commit time exceeds the timeout duration.
     */
    static void throttleCommitOnRateExceed(CommitRateExceededException e, long startTimeMs, long timeoutMs)
            throws CommitRateExceededException {
        if (timeoutMs != 0 && e.getAllowCommitTime() > (startTimeMs + timeoutMs)) {
            throw e;
        } else {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(e.getAllowCommitTime() - System.currentTimeMillis());
        }
    }

    private VisibleStateWaiter commitTransactionUnderDatabaseWLock(
            @NotNull Database db, long transactionId, @NotNull List<TabletCommitInfo> tabletCommitInfos,
            @NotNull List<TabletFailInfo> tabletFailInfos,
            @Nullable TxnCommitAttachment attachment, long timeoutMs) throws UserException {
        Locker locker = new Locker();
        if (!locker.tryLockDatabase(db, LockType.WRITE, timeoutMs)) {
            throw new LockTimeoutException(
                    "get database write lock timeout, database=" + db.getFullName() + ", timeout=" + timeoutMs + "ms");
        }
        try {
            return commitTransaction(db.getId(), transactionId, tabletCommitInfos, tabletFailInfos, attachment);
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public void abortAllRunningTransactions() throws UserException {
        for (Map.Entry<Long, DatabaseTransactionMgr> entry : dbIdToDatabaseTransactionMgrs.entrySet()) {
            entry.getValue().abortAllRunningTransaction();
        }
    }

    public void abortTransaction(long dbId, long transactionId, String reason) throws UserException {
        abortTransaction(dbId, transactionId, reason, Lists.newArrayList());
    }

    public void abortTransaction(long dbId, long transactionId, String reason, List<TabletFailInfo> failedTablets)
            throws UserException {
        abortTransaction(dbId, transactionId, reason, failedTablets, null);
    }

    public void abortTransaction(Long dbId, Long transactionId, String reason, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        abortTransaction(dbId, transactionId, reason, Lists.newArrayList(), txnCommitAttachment);
    }

    public void abortTransaction(long dbId, long transactionId, String reason, List<TabletFailInfo> failedTablets,
                                 TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.abortTransaction(transactionId, true, reason, txnCommitAttachment, failedTablets);
    }

    // for http cancel stream load api
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.abortTransaction(label, reason);
    }

    public TTransactionStatus getTxnStatus(Database db, long transactionId) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(db.getId());
        return dbTransactionMgr.getTxnStatus(transactionId);
    }

    /**
     * get all txns which is ready to publish
     * a ready-to-publish txn's partition's visible version should be ONE less than txn's commit version.
     *
     * @param nodep if true, only get txns without dependencies
     * @return list of txn state
     */
    public List<TransactionState> getReadyToPublishTransactions(boolean nodep) {
        List<TransactionState> transactionStateList = Lists.newArrayList();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            if (nodep) {
                transactionStateList.addAll(dbTransactionMgr.getReadyToPublishTxnList());
            } else {
                transactionStateList.addAll(dbTransactionMgr.getCommittedTxnList());
            }
        }
        return transactionStateList;
    }

    public List<TransactionStateBatch> getReadyPublishTransactionsBatch() {
        List<TransactionStateBatch> transactionStateList = Lists.newArrayList();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            List<TransactionStateBatch> dbTransactionStatesBatch = dbTransactionMgr.getReadyToPublishTxnListBatch();
            if (dbTransactionStatesBatch.size() != 0) {
                transactionStateList.addAll(dbTransactionStatesBatch);
            }
        }
        return transactionStateList;
    }

    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId) {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (tableId == null && partitionId == null) {
            return !dbTransactionMgr.getCommittedTxnList().isEmpty();
        }

        for (TransactionState transactionState : dbTransactionMgr.getCommittedTxnList()) {
            if (transactionState.getTableIdList().contains(tableId)) {
                if (partitionId == null) {
                    return true;
                } else if (transactionState.getTableCommitInfo(tableId).getPartitionCommitInfo(partitionId) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * if the table is deleted between commit and publish version, then should ignore the partition
     *
     * @param transactionId
     * @param errorReplicaIds
     * @return
     */
    public void finishTransaction(long dbId, long transactionId, Set<Long> errorReplicaIds) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.finishTransaction(transactionId, errorReplicaIds);
    }

    public void finishTransactionBatch(long dbId, TransactionStateBatch stateBatch, Set<Long> errorReplicaIds)
            throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.finishTransactionBatch(stateBatch, errorReplicaIds);
    }

    public void finishTransactionNew(TransactionState txnState, Set<Long> publishErrorReplicas) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(txnState.getDbId());
        dbTransactionMgr.finishTransactionNew(txnState, publishErrorReplicas);
    }

    public boolean canTxnFinished(TransactionState txn, Set<Long> errReplicas,
                                  Set<Long> unfinishedBackends) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(txn.getDbId());
        return dbTransactionMgr.canTxnFinished(txn, errReplicas, unfinishedBackends);
    }

    /**
     * Check whether a load job already exists before checking all `TransactionId` related with this load job have finished.
     * finished
     *
     * @throws AnalysisException is database does not exist anymore
     */
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.isPreviousTransactionsFinished(endTransactionId, tableIdList);
        } catch (AnalysisException e) {
            // NOTICE: At present, this situation will only happen when the database no longer exists.
            // In fact, getDatabaseTransactionMgr() should explicitly throw a MetaNotFoundException,
            // but changing the type of exception will cause a large number of code changes,
            // which is not worth the loss.
            // So here just simply think that AnalysisException only means that db does not exist.
            LOG.warn("Check whether all previous transactions in db [" + dbId + "] finished failed", e);
            throw e;
        }
    }

    /**
     * The txn cleaner will run at a fixed interval and try to delete expired and timeout txns:
     * expired: txn is in VISIBLE or ABORTED, and is expired.
     * timeout: txn is in PREPARE, but timeout
     */
    public void abortTimeoutTxns() {
        long currentMillis = System.currentTimeMillis();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.abortTimeoutTxns(currentMillis);
        }
        for (CreateTableTxnState createTableTxnState : createTableTxnMap.values()) {
            if (createTableTxnState.isTimeout(currentMillis / 1000)) {
                createTableTxnState.abort();
            }
        }
    }

    public void removeExpiredTxns() {
        long currentMillis = System.currentTimeMillis();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.removeExpiredTxns(currentMillis);
        }
        for (CreateTableTxnState createTableTxnState : createTableTxnMap.values()) {
            if (createTableTxnState.isTimeout(currentMillis / 1000)) {
                createTableTxnMap.remove(createTableTxnState.getTxnId());
            }
        }
    }

    /**
     * Get the min txn id of running transactions.
     *
     * @return the min txn id of running transactions. If there are no running transactions, return the next transaction id
     * that will be assigned.
     */
    public long getMinActiveTxnId() {
        long minId = idGenerator.peekNextTransactionId();
        for (Map.Entry<Long, DatabaseTransactionMgr> entry : dbIdToDatabaseTransactionMgrs.entrySet()) {
            DatabaseTransactionMgr dbTransactionMgr = entry.getValue();
            minId = Math.min(minId, dbTransactionMgr.getMinActiveTxnId().orElse(Long.MAX_VALUE));
        }
        return minId;
    }

    /**
     * Get the min txn id of running compaction transactions.
     *
     * @return the min txn id of running compaction transactions.
     * If there are no running compaction transactions, return the next transaction id that will be assigned.
     */
    public long getMinActiveCompactionTxnId() {
        long minId = idGenerator.peekNextTransactionId();
        for (Map.Entry<Long, DatabaseTransactionMgr> entry : dbIdToDatabaseTransactionMgrs.entrySet()) {
            DatabaseTransactionMgr dbTransactionMgr = entry.getValue();
            minId = Math.min(minId, dbTransactionMgr.getMinActiveCompactionTxnId().orElse(Long.MAX_VALUE));
        }
        return minId;
    }

    /**
     * Get the smallest transaction ID of active transactions in a database.
     * If there are no active transactions in the database, return the transaction ID that will be assigned to the
     * next transaction.
     *
     * @param dbId the ID the database
     * @return the min txn id of running transactions in the database. If there are no running transactions, return
     * the next transaction id that will be assigned.
     */
    public long getMinActiveTxnIdOfDatabase(long dbId) {
        long minId = idGenerator.peekNextTransactionId();
        DatabaseTransactionMgr dbTxnMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (dbTxnMgr != null) {
            minId = Math.min(minId, dbTxnMgr.getMinActiveTxnId().orElse(Long.MAX_VALUE));
        }
        return minId;
    }

    public TransactionState getTransactionState(long dbId, long transactionId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getTransactionState(transactionId);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction {} in db {} failed. msg: {}", transactionId, dbId, e.getMessage());
            return null;
        }
    }

    // for replay idToTransactionState
    // check point also run transaction cleaner, the cleaner maybe concurrently modify id to 
    public void replayUpsertTransactionState(TransactionState transactionState) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
            dbTransactionMgr.replayUpsertTransactionState(transactionState);
        } catch (AnalysisException e) {
            LOG.warn("replay upsert transaction [" + transactionState.getTransactionId() + "] failed", e);
        }
    }

    public void replayUpsertTransactionStateBatch(TransactionStateBatch transactionStateBatch) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionStateBatch.getDbId());
            dbTransactionMgr.replayUpsertTransactionStateBatch(transactionStateBatch);
        } catch (AnalysisException e) {
            LOG.warn("replay upsert transaction batch[" + transactionStateBatch + "] failed", e);
        }
    }

    public List<List<Comparable>> getDbInfo() {
        List<List<Comparable>> infos = new ArrayList<List<Comparable>>();
        List<Long> dbIds = Lists.newArrayList(dbIdToDatabaseTransactionMgrs.keySet());
        for (long dbId : dbIds) {
            List<Comparable> info = new ArrayList<Comparable>();
            info.add(dbId);
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                continue;
            }
            info.add(db.getOriginName());
            infos.add(info);
        }
        return infos;
    }

    public List<List<String>> getDbTransStateInfo(long dbId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getDbTransStateInfo();
        } catch (AnalysisException e) {
            LOG.warn("Get db [" + dbId + "] transactions info failed", e);
            return Lists.newArrayList();
        }
    }

    public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getTxnStateInfoList(running, limit);
    }

    // get show info of a specified txnId
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getSingleTranInfo(dbId, txnId);
    }

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getTableTransInfo(txnId);
    }

    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getPartitionTransInfo(tid, tableId);
    }

    public int getTransactionNum() {
        int txnNum = 0;
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnNum += dbTransactionMgr.getTransactionNum();
        }
        return txnNum;
    }

    public int getFinishedTransactionNum() {
        int txnNum = 0;
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnNum += dbTransactionMgr.getFinishedTxnNums();
        }
        return txnNum;
    }

    public TransactionIdGenerator getTransactionIDGenerator() {
        return this.idGenerator;
    }

    public void addCreateTableTxn(Long txnId, CreateTableTxnState txnState) {
        if (createTableTxnMap.containsKey(txnId)) {
            LOG.warn("txnId already exist. txnId: {}", txnId);
        }
        createTableTxnMap.put(txnId, txnState);
    }

    public void abortCreateTableTxn(Long txnId) {
        if (createTableTxnMap.containsKey(txnId)) {
            CreateTableTxnState txnState = createTableTxnMap.get(txnId);
            txnState.abort();
            createTableTxnMap.remove(txnId);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int numTransactions = getTransactionNum();
        out.writeInt(numTransactions);
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.unprotectWriteAllTransactionStates(out);
        }
        idGenerator.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        long now = System.currentTimeMillis();
        int numTransactions = in.readInt();
        List<TransactionState> transactionStates = new ArrayList<>(numTransactions);
        for (int i = 0; i < numTransactions; ++i) {
            TransactionState transactionState = new TransactionState();
            transactionState.readFields(in);
            if (transactionState.isExpired(now)) {
                LOG.info("discard expired transaction state: {}", transactionState);
                continue;
            } else if (transactionState.getTransactionStatus() == TransactionStatus.UNKNOWN) {
                LOG.info("discard unknown transaction state: {}", transactionState);
                continue;
            }
            transactionStates.add(transactionState);
        }
        putTransactionStats(transactionStates);
        idGenerator.readFields(in);
    }

    public long loadTransactionState(DataInputStream dis, long checksum) throws IOException {
        int size = dis.readInt();
        long newChecksum = checksum ^ size;
        readFields(dis);
        LOG.info("finished replay transactionState from image");
        return newChecksum;
    }

    public void loadTransactionStateV2(SRMetaBlockReader reader)
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        long now = System.currentTimeMillis();
        idGenerator = reader.readJson(TransactionIdGenerator.class);
        int numTransactions = reader.readInt();
        List<TransactionState> transactionStates = new ArrayList<>(numTransactions);
        for (int i = 0; i < numTransactions; ++i) {
            TransactionState transactionState = reader.readJson(TransactionState.class);
            if (transactionState.isExpired(now)) {
                LOG.info("discard expired transaction state: {}", transactionState);
                continue;
            } else if (transactionState.getTransactionStatus() == TransactionStatus.UNKNOWN) {
                LOG.info("discard unknown transaction state: {}", transactionState);
                continue;
            }
            transactionStates.add(transactionState);
        }
        putTransactionStats(transactionStates);

        int numCreateTableTxn = reader.readInt();
        for (int i = 0; i < numCreateTableTxn; ++i) {
            CreateTableTxnState txnState = reader.readJson(CreateTableTxnState.class);
            createTableTxnMap.put(txnState.getTxnId(), txnState);
        }
    }

    private void putTransactionStats(List<TransactionState> transactionStates) throws IOException {
        transactionStates.sort(Comparator.comparingLong(TransactionState::getCommitTime));
        for (TransactionState transactionState : transactionStates) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
                dbTransactionMgr.unprotectUpsertTransactionState(transactionState, true);
            } catch (AnalysisException e) {
                LOG.warn("failed to get db transaction manager for {}", transactionState, e);
                throw new IOException(
                        "failed to get db transaction manager for txn " + transactionState.getTransactionId(), e);
            }
        }
    }

    public List<Pair<Long, Long>> getTransactionIdByCoordinateBe(String coordinateHost, int limit) {
        ArrayList<Pair<Long, Long>> txnInfos = new ArrayList<>();
        for (DatabaseTransactionMgr databaseTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnInfos.addAll(databaseTransactionMgr.getTransactionIdByCoordinateBe(coordinateHost, limit));
            if (txnInfos.size() > limit) {
                break;
            }
        }
        return txnInfos.size() > limit ? new ArrayList<>(txnInfos.subList(0, limit)) : txnInfos;
    }

    public Long getTransactionNumByCoordinateBe(String coordinateHost) {
        Long txnNum = 0L;
        for (DatabaseTransactionMgr databaseTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnNum += databaseTransactionMgr.getTransactionNumByCoordinateBe(coordinateHost);
        }
        return txnNum;
    }

    /**
     * If a Coordinate BE is down when running txn, the txn will remain in FE until killed by timeout
     * So when FE identify the Coordiante BE is down, FE should cancel it initiative
     */
    public void abortTxnWhenCoordinateBeDown(String coordinateHost, int limit) {
        List<Pair<Long, Long>> transactionIdByCoordinateBe = getTransactionIdByCoordinateBe(coordinateHost, limit);
        for (Pair<Long, Long> txnInfo : transactionIdByCoordinateBe) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(txnInfo.first);
                dbTransactionMgr.abortTransaction(txnInfo.second, false, "coordinate BE is down", null,
                        Lists.newArrayList());
            } catch (UserException e) {
                LOG.warn("Abort txn on coordinate BE {} failed, msg={}", coordinateHost, e.getMessage());
            }
        }
    }

    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.updateDatabaseUsedQuotaData(usedQuotaDataBytes);
    }

    public void saveTransactionStateV2(DataOutputStream dos) throws IOException, SRMetaBlockException {
        int txnNum = getTransactionNum();
        final int cnt = 3 + txnNum;
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.GLOBAL_TRANSACTION_MGR, cnt);
        writer.writeJson(idGenerator);
        writer.writeJson(txnNum);
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.unprotectWriteAllTransactionStatesV2(writer);
        }
        writer.writeJson(createTableTxnMap.size());
        for (Map.Entry<Long, CreateTableTxnState> entry : createTableTxnMap.entrySet()) {
            writer.writeJson(entry.getValue());
        }
        writer.close();
    }

    public String getTxnPublishTimeoutDebugInfo(long dbId, long txnId) {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (dbTransactionMgr == null) {
            return "";
        }
        return dbTransactionMgr.getTxnPublishTimeoutDebugInfo(txnId);
    }
}
