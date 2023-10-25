// This file is made available under Elastic License 2.0.
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
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.EditLog;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAbortRemoteTxnRequest;
import com.starrocks.thrift.TAbortRemoteTxnResponse;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TBeginRemoteTxnRequest;
import com.starrocks.thrift.TBeginRemoteTxnResponse;
import com.starrocks.thrift.TCommitRemoteTxnRequest;
import com.starrocks.thrift.TCommitRemoteTxnResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
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

import static java.lang.Math.min;

/**
 * Transaction Manager
 * 1. begin
 * 2. commit
 * 3. abort
 * Attention: all api in txn manager should get db lock or load lock first, then get txn manager's lock, or
 * there will be dead lock
 */
public class GlobalTransactionMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(GlobalTransactionMgr.class);

    private Map<Long, DatabaseTransactionMgr> dbIdToDatabaseTransactionMgrs = Maps.newConcurrentMap();

    private TransactionIdGenerator idGenerator = new TransactionIdGenerator();
    private TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();

    private GlobalStateMgr globalStateMgr;

    public GlobalTransactionMgr(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    public DatabaseTransactionMgr getDatabaseTransactionMgr(long dbId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (dbTransactionMgr == null) {
            throw new AnalysisException("databaseTransactionMgr[" + dbId + "] does not exist");
        }
        return dbTransactionMgr;
    }

    public void addDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.putIfAbsent(dbId,
                new DatabaseTransactionMgr(dbId, globalStateMgr, idGenerator)) ==
                null) {
            LOG.debug("add database transaction manager for db {}", dbId);
        }
    }

    public void removeDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.remove(dbId) != null) {
            LOG.debug("remove database transaction manager for db {}", dbId);
        }
    }

    // begin transaction in remote StarRocks cluster
    public long beginRemoteTransaction(long dbId, List<Long> tableIds, String label,
                                       String host, int port, TxnCoordinator coordinator,
                                       LoadJobSourceType sourceType, long timeoutSecond,
                                       TAuthenticateParams authenticateParams)
            throws AnalysisException, BeginTransactionException {
        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                        Config.min_load_timeout_second);
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }

        TNetworkAddress addr = new TNetworkAddress(host, port);
        TBeginRemoteTxnRequest request = new TBeginRemoteTxnRequest();
        request.setDb_id(dbId);
        for (Long tableId : tableIds) {
            request.addToTable_ids(tableId);
        }
        request.setLabel(label);
        request.setSource_type(sourceType.ordinal());
        request.setTimeout_second(timeoutSecond);
        request.setAuth_info(authenticateParams);
        TBeginRemoteTxnResponse response;
        try {
            response = FrontendServiceProxy.call(addr,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.beginRemoteTxn(request));
        } catch (Exception e) {
            LOG.warn("call fe {} beginRemoteTransaction rpc method failed, label: {}", addr, label, e);
            throw new BeginTransactionException(e.getMessage());
        }
        if (response.status.getStatus_code() != TStatusCode.OK) {
            String errStr;
            if (response.status.getError_msgs() != null) {
                errStr = String.join(". ", response.status.getError_msgs());
            } else {
                errStr = "";
            }
            LOG.warn("call fe {} beginRemoteTransaction rpc method failed, label: {}, error: {}", addr, label, errStr);
            throw new BeginTransactionException(errStr);
        } else {
            if (response.getTxn_id() <= 0) {
                LOG.warn("beginRemoteTransaction returns invalid txn_id: {}, label: {}",
                        response.getTxn_id(), label);
                throw new BeginTransactionException("beginRemoteTransaction returns invalid txn id");
            }
            LOG.info("begin remote txn, label: {}, txn_id: {}", label, response.getTxn_id());
            return response.getTxn_id();
        }
    }

    // commit transaction in remote StarRocks cluster
    public boolean commitRemoteTransaction(long dbId, long transactionId,
                                           String host, int port, List<TTabletCommitInfo> tabletCommitInfos)
            throws TransactionCommitFailedException {

        TNetworkAddress addr = new TNetworkAddress(host, port);
        TCommitRemoteTxnRequest request = new TCommitRemoteTxnRequest();
        request.setDb_id(dbId);
        request.setTxn_id(transactionId);
        request.setCommit_infos(tabletCommitInfos);
        request.setCommit_timeout_ms(Config.external_table_commit_timeout_ms);
        TCommitRemoteTxnResponse response;
        try {
            response = FrontendServiceProxy.call(addr,
                    // commit txn might take a while, so add transaction timeout
                    Config.thrift_rpc_timeout_ms + Config.external_table_commit_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.commitRemoteTxn(request));
        } catch (Exception e) {
            LOG.warn("call fe {} commitRemoteTransaction rpc method failed, txn_id: {} e: {}", addr, transactionId, e);
            throw new TransactionCommitFailedException(e.getMessage());
        }
        if (response.status.getStatus_code() != TStatusCode.OK) {
            String errStr;
            if (response.status.getError_msgs() != null) {
                errStr = String.join(",", response.status.getError_msgs());
            } else {
                errStr = "";
            }
            LOG.warn("call fe {} commitRemoteTransaction rpc method failed, txn_id: {}, error: {}", addr, transactionId,
                    errStr);
            if (response.status.getStatus_code() == TStatusCode.TIMEOUT) {
                return false;
            } else {
                throw new TransactionCommitFailedException(errStr);
            }
        } else {
            LOG.info("commit remote, txn_id: {}", transactionId);
            return true;
        }
    }

    // abort transaction in remote StarRocks cluster
    public void abortRemoteTransaction(long dbId, long transactionId,
                                       String host, int port, String errorMsg)
            throws AbortTransactionException {

        TNetworkAddress addr = new TNetworkAddress(host, port);
        TAbortRemoteTxnRequest request = new TAbortRemoteTxnRequest();
        request.setDb_id(dbId);
        request.setTxn_id(transactionId);
        request.setError_msg(errorMsg);
        TAbortRemoteTxnResponse response;
        try {
            response = FrontendServiceProxy.call(addr,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.abortRemoteTxn(request));
        } catch (Exception e) {
            LOG.warn("call fe {} abortRemoteTransaction rpc method failed, txn_id: {} e: {}", addr, transactionId, e);
            throw new AbortTransactionException(e.getMessage());
        }
        if (response.status.getStatus_code() != TStatusCode.OK) {
            String errStr;
            if (response.status.getError_msgs() != null) {
                errStr = String.join(",", response.status.getError_msgs());
            } else {
                errStr = "";
            }
            LOG.warn("call fe {} abortRemoteTransaction rpc method failed, txn_id: {}, error: {}", addr, transactionId,
                    errStr);
            throw new AbortTransactionException(errStr);
        } else {
            LOG.info("abort remote, txn_id: {}", transactionId);
        }
    }

    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
                                 LoadJobSourceType sourceType,
                                 long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
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
     * @throws BeginTransactionException
     * @throws DuplicatedRequestException
     * @throws IllegalTransactionParameterException
     */
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
                                 TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId,
                                 long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException {

        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                        Config.min_load_timeout_second);
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }

        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr
                .beginTransaction(tableIdList, label, requestId, coordinator, sourceType, listenerId, timeoutSecond);
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

    public VisibleStateWaiter commitTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        return commitTransaction(dbId, transactionId, tabletCommitInfos, Lists.newArrayList(), null);
    }

    public VisibleStateWaiter commitTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        return commitTransaction(dbId, transactionId, tabletCommitInfos, Lists.newArrayList(), txnCommitAttachment);
    }

    public VisibleStateWaiter commitTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
            List<TabletFailInfo> tabletFailInfos)
            throws UserException {
        return commitTransaction(dbId, transactionId, tabletCommitInfos, tabletFailInfos, null);
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
    public VisibleStateWaiter commitTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
            List<TabletFailInfo> tabletFailInfos,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        LOG.debug("try to commit transaction: {}", transactionId);
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.commitTransaction(transactionId, tabletCommitInfos, tabletFailInfos, txnCommitAttachment);
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
        dbTransactionMgr.prepareTransaction(transactionId, tabletCommitInfos, tabletFailInfos, txnCommitAttachment);
    }

    public void commitPreparedTransaction(long dbId, long transactionId, long timeoutMillis)
            throws UserException  {

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
        if (!db.tryWriteLock(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("get database write lock timeout, database="
                    + db.getFullName() + ", timeoutMillis=" + timeoutMillis);
        }
        try {
            waiter = getDatabaseTransactionMgr(db.getId()).commitPreparedTransaction(transactionId);
        } finally {
            db.writeUnlock();
        }

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

    public boolean commitAndPublishTransaction(Database db, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, List<TabletFailInfo> tabletFailInfos, long timeoutMillis)
            throws UserException {
        return commitAndPublishTransaction(db, transactionId, tabletCommitInfos, tabletFailInfos, timeoutMillis, null);
    }

    public boolean commitAndPublishTransaction(Database db, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, List<TabletFailInfo> tabletFailInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (!db.tryWriteLock(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("get database write lock timeout, database="
                    + db.getOriginName() + ", timeoutMillis=" + timeoutMillis);
        }
        VisibleStateWaiter waiter;
        try {
            waiter = commitTransaction(db.getId(), transactionId, tabletCommitInfos, tabletFailInfos,
                    txnCommitAttachment);
        } finally {
            db.writeUnlock();
        }
        stopWatch.stop();
        long publishTimeoutMillis = timeoutMillis - stopWatch.getTime();
        if (publishTimeoutMillis < 0) {
            // here commit transaction successfully cost too much time to cause publisTimeoutMillis is less than zero,
            // so we just return false to indicate publish timeout
            return false;
        }
        return waiter.await(publishTimeoutMillis, TimeUnit.MILLISECONDS);
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
        dbTransactionMgr.abortTransaction(transactionId, reason, txnCommitAttachment, failedTablets);
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
    }

    public void removeExpiredTxns() {
        long currentMillis = System.currentTimeMillis();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.removeExpiredTxns(currentMillis);
        }
    }

    /**
     * Get the min txn id of running transactions.
     *
     * @return the min txn id of running transactions, null if no running transaction.
     */
    @Nullable
    public Long getMinActiveTxnId() {
        long result = Long.MAX_VALUE;
        for (Map.Entry<Long, DatabaseTransactionMgr> entry : dbIdToDatabaseTransactionMgrs.entrySet()) {
            DatabaseTransactionMgr dbTransactionMgr = entry.getValue();
            result = min(result, dbTransactionMgr.getMinActiveTxnId());
        }
        return result == Long.MAX_VALUE ? null : result;
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

    public void setEditLog(EditLog editLog) {
        this.idGenerator.setEditLog(editLog);
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

    public void replayDeleteTransactionState(TransactionState transactionState) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
            dbTransactionMgr.deleteTransaction(transactionState);
        } catch (AnalysisException e) {
            LOG.warn("replay delete transaction [" + transactionState.getTransactionId() + "] failed", e);
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
        transactionStates.sort(Comparator.comparingLong(TransactionState::getCommitTime));
        for (TransactionState transactionState : transactionStates) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
                dbTransactionMgr.unprotectUpsertTransactionState(transactionState, true);
            } catch (AnalysisException e) {
                LOG.warn("failed to get db transaction manager for {}", transactionState);
                throw new IOException("failed to get db transaction manager for txn " + transactionState.getTransactionId(), e);
            }
        }
        idGenerator.readFields(in);
    }

    public long loadTransactionState(DataInputStream dis, long checksum) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_45) {
            int size = dis.readInt();
            long newChecksum = checksum ^ size;
            readFields(dis);
            LOG.info("finished replay transactionState from image");
            return newChecksum;
        }
        return checksum;
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

    public long saveTransactionState(DataOutputStream dos, long checksum) throws IOException {
        int size = getTransactionNum();
        checksum ^= size;
        dos.writeInt(size);
        write(dos);
        return checksum;
    }

    public String getTxnPublishTimeoutDebugInfo(long dbId, long txnId) {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (dbTransactionMgr == null) {
            return "";
        }
        return dbTransactionMgr.getTxnPublishTimeoutDebugInfo(txnId);
    }
}
