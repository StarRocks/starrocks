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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/LoadAction.java

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

package com.starrocks.http.rest;

import com.codahale.metrics.Histogram;
<<<<<<< HEAD
import com.google.common.base.Strings;
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.UserException;
=======
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.StarRocksHttpException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpMetricRegistry;
import com.starrocks.http.IllegalArgException;
<<<<<<< HEAD
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionStatus;
import io.netty.handler.codec.http.HttpMethod;
=======
import com.starrocks.http.rest.transaction.BypassWriteTransactionHandler;
import com.starrocks.http.rest.transaction.TransactionOperation;
import com.starrocks.http.rest.transaction.TransactionOperationHandler;
import com.starrocks.http.rest.transaction.TransactionOperationHandler.ResultWrapper;
import com.starrocks.http.rest.transaction.TransactionOperationParams;
import com.starrocks.http.rest.transaction.TransactionOperationParams.Body;
import com.starrocks.http.rest.transaction.TransactionOperationParams.Channel;
import com.starrocks.http.rest.transaction.TransactionWithChannelHandler;
import com.starrocks.http.rest.transaction.TransactionWithoutChannelHandler;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.StringUtils;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_BEGIN_LATENCY_MS;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_BEGIN_NUM;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_COMMIT_LATENCY_MS;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_COMMIT_NUM;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_LOAD_LATENCY_MS;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_LOAD_NUM;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_PREPARE_LATENCY_MS;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_PREPARE_NUM;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_ROLLBACK_LATENCY_MS;
import static com.starrocks.http.HttpMetricRegistry.TXN_STREAM_LOAD_ROLLBACK_NUM;
<<<<<<< HEAD

public class TransactionLoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(TransactionLoadAction.class);
    private static final String TXN_OP_KEY = "txn_op";
    private static final String TXN_BEGIN = "begin";
    private static final String TXN_LOAD = "load";
    private static final String TXN_PREPARE = "prepare";
    private static final String TXN_COMMIT = "commit";
    private static final String TXN_ROLLBACK = "rollback";
    private static final String TIMEOUT_KEY = "timeout";
    private static final String CHANNEL_NUM_STR = "channel_num";
    private static final String CHANNEL_ID_STR = "channel_id";
    private static TransactionLoadAction ac;

    // Map operation name to metrics
    private final Map<String, OpMetrics> opMetricsMap = new HashMap<>();

    private Map<String, Long> txnNodeMap = new LinkedHashMap<String, Long>(512, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > (GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber() +
                    GlobalStateMgr.getCurrentSystemInfo().getTotalComputeNodeNumber()) * 512;
=======
import static com.starrocks.http.rest.transaction.TransactionOperation.TXN_BEGIN;
import static com.starrocks.http.rest.transaction.TransactionOperation.TXN_COMMIT;
import static com.starrocks.http.rest.transaction.TransactionOperation.TXN_LOAD;
import static com.starrocks.http.rest.transaction.TransactionOperation.TXN_PREPARE;
import static com.starrocks.http.rest.transaction.TransactionOperation.TXN_ROLLBACK;

public class TransactionLoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(TransactionLoadAction.class);

    private static final long DEFAULT_TXN_TIMEOUT_MILLIS = 20000L;

    private static final String TXN_OP_KEY = "txn_op";
    private static final String TIMEOUT_KEY = "timeout";
    private static final String CHANNEL_NUM_STR = "channel_num";
    private static final String CHANNEL_ID_STR = "channel_id";
    private static final String SOURCE_TYPE = "source_type";

    private static TransactionLoadAction ac;

    // Map operation name to metrics
    private final Map<TransactionOperation, OpMetrics> opMetricsMap = new HashMap<>();

    private final ReadWriteLock txnNodeMapAccessLock = new ReentrantReadWriteLock();
    private final Map<String, Long> txnNodeMap = new LinkedHashMap<>(512, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber() +
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalComputeNodeNumber()) * 512;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    };

    public TransactionLoadAction(ActionController controller) {
        super(controller);
        initMetrics();
    }

    private void initMetrics() {
        HttpMetricRegistry metricRegistry = HttpMetricRegistry.getInstance();

        LongCounterMetric txnStreamLoadBeginNum = new LongCounterMetric(TXN_STREAM_LOAD_BEGIN_NUM,
                Metric.MetricUnit.NOUNIT, "the number of begin operations in transaction stream load that are being handled");
        metricRegistry.registerCounter(txnStreamLoadBeginNum);
        Histogram beginLatency = metricRegistry.registerHistogram(TXN_STREAM_LOAD_BEGIN_LATENCY_MS);
        opMetricsMap.put(TXN_BEGIN, OpMetrics.of(txnStreamLoadBeginNum, beginLatency));

        LongCounterMetric txnStreamLoadLoadNum = new LongCounterMetric(TXN_STREAM_LOAD_LOAD_NUM,
                Metric.MetricUnit.NOUNIT, "the number of load operations in transaction stream load that are being handled");
        metricRegistry.registerCounter(txnStreamLoadLoadNum);
        Histogram loadLatency = metricRegistry.registerHistogram(TXN_STREAM_LOAD_LOAD_LATENCY_MS);
        opMetricsMap.put(TXN_LOAD, OpMetrics.of(txnStreamLoadLoadNum, loadLatency));

        LongCounterMetric txnStreamLoadPrepareNum = new LongCounterMetric(TXN_STREAM_LOAD_PREPARE_NUM,
                Metric.MetricUnit.NOUNIT, "the number of prepare operations in transaction stream load that are being handled");
        metricRegistry.registerCounter(txnStreamLoadPrepareNum);
        Histogram prepareLatency = metricRegistry.registerHistogram(TXN_STREAM_LOAD_PREPARE_LATENCY_MS);
        opMetricsMap.put(TXN_PREPARE, OpMetrics.of(txnStreamLoadPrepareNum, prepareLatency));

        LongCounterMetric txnStreamLoadCommitNum = new LongCounterMetric(TXN_STREAM_LOAD_COMMIT_NUM,
                Metric.MetricUnit.NOUNIT, "the number of commit operations in transaction stream load that are being handled");
        metricRegistry.registerCounter(txnStreamLoadCommitNum);
        Histogram commitLatency = metricRegistry.registerHistogram(TXN_STREAM_LOAD_COMMIT_LATENCY_MS);
        opMetricsMap.put(TXN_COMMIT, OpMetrics.of(txnStreamLoadCommitNum, commitLatency));

        LongCounterMetric txnStreamLoadRollbackNum = new LongCounterMetric(TXN_STREAM_LOAD_ROLLBACK_NUM,
                Metric.MetricUnit.NOUNIT, "the number of rollback operations in transaction stream load that are being handled");
        metricRegistry.registerCounter(txnStreamLoadRollbackNum);
        Histogram rollbackLatency = metricRegistry.registerHistogram(TXN_STREAM_LOAD_ROLLBACK_LATENCY_MS);
        opMetricsMap.put(TXN_ROLLBACK, OpMetrics.of(txnStreamLoadRollbackNum, rollbackLatency));
    }

    public int txnNodeMapSize() {
<<<<<<< HEAD
        synchronized (this) {
            return txnNodeMap.size();
        }
=======
        return accessTxnNodeMapWithReadLock(Map::size);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public static TransactionLoadAction getAction() {
        return ac;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ac = new TransactionLoadAction(controller);
        controller.registerHandler(HttpMethod.POST, "/api/transaction/{" + TXN_OP_KEY + "}", ac);
<<<<<<< HEAD
        controller.registerHandler(HttpMethod.PUT,
                "/api/transaction/{" + TXN_OP_KEY + "}", ac);
=======
        controller.registerHandler(HttpMethod.PUT, "/api/transaction/{" + TXN_OP_KEY + "}", ac);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        OpMetrics opMetrics = null;
        long startTime = System.currentTimeMillis();
        try {
            if (redirectToLeader(request, response)) {
                return;
            }
<<<<<<< HEAD
            String op = request.getSingleParameter(TXN_OP_KEY);
            opMetrics = opMetricsMap.get(op);
            if (opMetrics != null) {
                opMetrics.opRunningNum.increase(1L);
            }
            executeTransaction(request, response, op);
=======
            TransactionOperation txnOperation = TransactionOperation.parse(request.getSingleParameter(TXN_OP_KEY))
                    .orElseThrow(() -> new StarRocksException(
                            "Unknown transaction operation: " + request.getSingleParameter(TXN_OP_KEY)));
            opMetrics = opMetricsMap.get(txnOperation);
            if (opMetrics != null) {
                opMetrics.opRunningNum.increase(1L);
            }
            executeTransaction(request, response);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        } catch (Exception e) {
            TransactionResult resp = new TransactionResult();
            if (e instanceof LabelAlreadyUsedException) {
                resp.status = ActionStatus.LABEL_ALREADY_EXISTS;
                resp.msg = e.getMessage();
                resp.addResultEntry("ExistingJobStatus", ((LabelAlreadyUsedException) e).getJobStatus());
            } else {
                resp.status = ActionStatus.FAILED;
<<<<<<< HEAD
                resp.msg = e.getClass().toString() + ": " + e.getMessage();
=======
                resp.msg = e.getClass() + ": " + e.getMessage();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            }
            LOG.warn(DebugUtil.getStackTrace(e));
            sendResult(request, response, resp);
        } finally {
            if (opMetrics != null) {
                opMetrics.opRunningNum.increase(-1L);
                opMetrics.opLatencyMs.update(System.currentTimeMillis() - startTime);
            }
        }
    }

<<<<<<< HEAD
    public void executeTransaction(BaseRequest request, BaseResponse response, String op) throws UserException {
        String dbName = request.getRequest().headers().get(DB_KEY);
        String tableName = request.getRequest().headers().get(TABLE_KEY);
        String label = request.getRequest().headers().get(LABEL_KEY);
        String timeout = request.getRequest().headers().get(TIMEOUT_KEY);
        String channelNumStr = null;
        String channelIdStr = null;
        if (request.getRequest().headers().contains(CHANNEL_NUM_STR)) {
            channelNumStr = request.getRequest().headers().get(CHANNEL_NUM_STR);
        }
        if (request.getRequest().headers().contains(CHANNEL_ID_STR)) {
            channelIdStr = request.getRequest().headers().get(CHANNEL_ID_STR);
        }

        if (channelNumStr != null && channelIdStr == null) {
            throw new DdlException("Must provide channel_id when stream load begin.");
        }
        if (channelNumStr == null && channelIdStr != null) {
            throw new DdlException("Must provide channel_num when stream load begin.");
        }

        Long nodeID = null;

        if (Strings.isNullOrEmpty(dbName)) {
            throw new UserException("No database selected.");
        }
        if (Strings.isNullOrEmpty(label)) {
            throw new UserException("empty label.");
        }

        // 1. handle commit/rollback PREPARED transaction
        if ((op.equalsIgnoreCase(TXN_COMMIT) || op.equalsIgnoreCase(TXN_ROLLBACK)) && channelIdStr == null) {
            TransactionResult resp = new TransactionResult();
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                throw new UserException("database " + dbName + " not exists");
            }
            TransactionStatus txnStatus = GlobalStateMgr.getCurrentGlobalTransactionMgr().getLabelStatus(db.getId(),
                    label);
            Long txnID = GlobalStateMgr.getCurrentGlobalTransactionMgr().getLabelTxnID(db.getId(), label);
            if (txnStatus == TransactionStatus.PREPARED) {
                if (txnID == -1) {
                    throw new UserException("label " + label + " txn not exist");
                }

                if (op.equalsIgnoreCase(TXN_COMMIT)) {
                    long timeoutMillis = 20000;
                    if (timeout != null) {
                        timeoutMillis = Long.parseLong(timeout) * 1000;
                    }
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().commitPreparedTransaction(db, txnID, timeoutMillis);
                } else if (op.equalsIgnoreCase(TXN_ROLLBACK)) {
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(db.getId(), txnID,
                            "User Aborted");
                }
                resp.addResultEntry("Label", label);
                sendResult(request, response, resp);
                return;
            } else if (txnStatus == TransactionStatus.COMMITTED || txnStatus == TransactionStatus.VISIBLE) {
                // whether txnId is valid or not is not important
                if (op.equalsIgnoreCase(TXN_ROLLBACK)) {
                    throw new UserException(String.format(
                        "cannot abort committed transaction %s, label %s ", Long.toString(txnID), label));
                }
                resp.setOKMsg("label " + label + " transaction " + txnID + " has already committed");
                resp.addResultEntry("Label", label);
                sendResult(request, response, resp);
                return;
            } else if (txnStatus == TransactionStatus.ABORTED) {
                // whether txnId is valid or not is not important
                if (op.equalsIgnoreCase(TXN_COMMIT)) {
                    throw new UserException(String.format(
                        "cannot commit aborted transaction %s, label %s ", Long.toString(txnID), label));
                }
                resp.setOKMsg("label " + label + " transaction " + txnID + " has already aborted");
                resp.addResultEntry("Label", label);
                sendResult(request, response, resp);
                return;
            }
        }

        if (channelIdStr == null) {
            // 2. redirect transaction op to BE
            synchronized (this) {
                // 2.1 save label->be hashmap when begin transaction, so that subsequent operator can send to same BE
                if (op.equalsIgnoreCase(TXN_BEGIN)) {
                    nodeID = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                               .getNodeSelector().seqChooseBackendOrComputeId();
                    // txnNodeMap is LRU cache, it atomic remove unused entry
                    txnNodeMap.put(label, nodeID);
                } else {
                    nodeID = txnNodeMap.get(label);
                }
            }
        }

        if (op.equalsIgnoreCase(TXN_BEGIN) && channelIdStr != null) {
            TransactionResult resp = new TransactionResult();
            long timeoutMillis = 20000;
            if (timeout != null) {
                timeoutMillis = Long.parseLong(timeout) * 1000;
            }
            int channelNum = Integer.parseInt(channelNumStr);
            int channelId = Integer.parseInt(channelIdStr);
            if (channelId >= channelNum || channelId < 0) {
                throw new DdlException("channel id should be between [0, " + String.valueOf(channelNum - 1) + "].");
            }

            // context.parseHttpHeader(request.getRequest().headers());
            GlobalStateMgr.getCurrentState().getStreamLoadMgr().beginLoadTask(
                    dbName, tableName, label, timeoutMillis, channelNum, channelId, resp);
            sendResult(request, response, resp);
            return;
        }

        if (op.equalsIgnoreCase(TXN_LOAD) && channelIdStr != null) {
            int channelId = Integer.parseInt(channelIdStr);
            TransactionResult resp = new TransactionResult();
            TNetworkAddress redirectAddr = GlobalStateMgr.getCurrentState().getStreamLoadMgr().executeLoadTask(
                    label, channelId, request.getRequest().headers(), resp, dbName, tableName);
            if (!resp.stateOK() || resp.containMsg()) {
                sendResult(request, response, resp);
                return;
            }
            LOG.info("redirect transaction action to destination={}, db: {}, table: {}, op: {}, label: {}",
                    redirectAddr, dbName, tableName, op, label);
            redirectTo(request, response, redirectAddr);
            return;
        } 

        if (op.equalsIgnoreCase(TXN_PREPARE) && channelIdStr != null) {
            int channelId = Integer.parseInt(channelIdStr);
            TransactionResult resp = new TransactionResult();
            GlobalStateMgr.getCurrentState().getStreamLoadMgr().prepareLoadTask(
                    label, channelId, request.getRequest().headers(), resp);
            if (!resp.stateOK() || resp.containMsg()) {
                sendResult(request, response, resp);
                return;
            }
            GlobalStateMgr.getCurrentState().getStreamLoadMgr().tryPrepareLoadTaskTxn(label, resp);
            sendResult(request, response, resp);
            return;
        }

        if (op.equalsIgnoreCase(TXN_COMMIT) && channelIdStr != null) {
            TransactionResult resp = new TransactionResult();
            GlobalStateMgr.getCurrentState().getStreamLoadMgr().commitLoadTask(label, resp);
            sendResult(request, response, resp);
            return;
        }

        if (op.equalsIgnoreCase(TXN_ROLLBACK) && channelIdStr != null) {
            TransactionResult resp = new TransactionResult();
            GlobalStateMgr.getCurrentState().getStreamLoadMgr().rollbackLoadTask(label, resp);
            sendResult(request, response, resp);
            return;
        }


        if (nodeID == null) {
            throw new UserException("transaction with op " + op + " label " + label + " has no node");
        }

        ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackend(nodeID);
        if (node == null) {
            node = GlobalStateMgr.getCurrentSystemInfo().getComputeNode(nodeID);
            if (node == null) {
                throw new UserException("Node " + nodeID + " is not alive");
            }
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(node.getHost(), node.getHttpPort());

        LOG.info("redirect transaction action to destination={}, db: {}, table: {}, op: {}, label: {}",
                redirectAddr, dbName, tableName, op, label);
        redirectTo(request, response, redirectAddr);
    }

=======
    protected void executeTransaction(BaseRequest request, BaseResponse response) throws StarRocksException {
        TransactionOperationParams txnOperationParams = toTxnOperationParams(request);
        TransactionOperation txnOperation = txnOperationParams.getTxnOperation();
        String label = txnOperationParams.getLabel();

        TransactionOperationHandler txnOperationHandler = getTxnOperationHandler(txnOperationParams);
        ResultWrapper result = txnOperationHandler.handle(request, response);
        if (null != result.getResult()) {
            sendResult(request, response, result.getResult());
            return;
        }

        // redirect transaction op to BE
        TNetworkAddress redirectAddress = result.getRedirectAddress();
        if (null == redirectAddress) {
            Long nodeId = getNodeId(txnOperation, label);
            ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(nodeId);
            if (node == null) {
                node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNode(nodeId);
                if (node == null) {
                    throw new StarRocksException("Node " + nodeId + " is not alive");
                }
            }

            redirectAddress = new TNetworkAddress(node.getHost(), node.getHttpPort());
        }

        LOG.info("Redirect transaction action to destination={}, db: {}, table: {}, op: {}, label: {}",
                redirectAddress, txnOperationParams.getDbName(), txnOperationParams.getTableName(), txnOperation, label);
        redirectTo(request, response, redirectAddress);
    }

    private TransactionOperationHandler getTxnOperationHandler(TransactionOperationParams params) throws
            StarRocksException {
        if (params.getChannel().notNull()) {
            return new TransactionWithChannelHandler(params);
        }

        TransactionOperation txnOperation = params.getTxnOperation();
        LoadJobSourceType sourceType = params.getSourceType();
        if ((TXN_BEGIN.equals(txnOperation) || TXN_LOAD.equals(txnOperation)) && null == sourceType) {
            return new TransactionWithoutChannelHandler(params);
        }

        String label = params.getLabel();
        if (accessTxnNodeMapWithReadLock(txnNodeMap -> txnNodeMap.containsKey(label))) {
            /*
             * The Bypass Write scenario will not redirect the request to BE definitely,
             * so if txnNodeMap contains the label, this must not be a Bypass Write scenario.
             */
            return new TransactionWithoutChannelHandler(params);
        }

        if (null == sourceType) {
            String dbName = params.getDbName();
            Database db = Optional.ofNullable(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName))
                    .orElseThrow(() -> new StarRocksException(String.format("Database[%s] does not exist.", dbName)));

            TransactionState txnState = GlobalStateMgr.getCurrentState()
                    .getGlobalTransactionMgr().getLabelTransactionState(db.getId(), label);
            if (null == txnState) {
                throw new StarRocksException(String.format("No transaction found by label %s", label));
            }
            sourceType = txnState.getSourceType();
        }

        return LoadJobSourceType.BYPASS_WRITE.equals(sourceType)
                ? new BypassWriteTransactionHandler(params) : new TransactionWithoutChannelHandler(params);
    }

    private Long getNodeId(TransactionOperation txnOperation, String label) throws StarRocksException {
        Long nodeId;
        // save label->be hashmap when begin transaction, so that subsequent operator can send to same BE
        if (TXN_BEGIN.equals(txnOperation)) {
            Long chosenNodeId = GlobalStateMgr.getCurrentState().getNodeMgr()
                    .getClusterInfo().getNodeSelector().seqChooseBackendOrComputeId();
            nodeId = chosenNodeId;
            // txnNodeMap is LRU cache, it atomic remove unused entry
            accessTxnNodeMapWithWriteLock(txnNodeMap -> txnNodeMap.put(label, chosenNodeId));
        } else {
            nodeId = accessTxnNodeMapWithReadLock(txnNodeMap -> txnNodeMap.get(label));
        }

        if (nodeId == null) {
            throw new StarRocksException(String.format(
                    "Transaction with op[%s] and label[%s] has no node.", txnOperation.getValue(), label));
        }

        return nodeId;
    }

    /**
     * Resolve and validate request, and wrap params it as {@link TransactionOperationParams} object.
     */
    private static TransactionOperationParams toTxnOperationParams(BaseRequest request) throws StarRocksException {
        String dbName = request.getRequest().headers().get(DB_KEY);
        if (StringUtils.isBlank(dbName)) {
            throw new StarRocksException("No database selected.");
        }

        String tableName = request.getRequest().headers().get(TABLE_KEY);
        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (request.getRequest().headers().contains(WAREHOUSE_KEY)) {
            warehouseName = request.getRequest().headers().get(WAREHOUSE_KEY);
        }

        String label = request.getRequest().headers().get(LABEL_KEY);
        if (StringUtils.isBlank(label)) {
            throw new StarRocksException("Empty label.");
        }
        String user = request.getRequest().headers().get(USER_KEY);

        TransactionOperation txnOperation = TransactionOperation.parse(request.getSingleParameter(TXN_OP_KEY))
                .orElseThrow(() -> new StarRocksException(
                        "Unknown transaction operation: " + request.getSingleParameter(TXN_OP_KEY)));
        Long timeoutMillis = Optional.ofNullable(request.getRequest().headers().get(TIMEOUT_KEY))
                .map(Long::parseLong)
                .map(sec -> sec * 1000L)
                .orElse(DEFAULT_TXN_TIMEOUT_MILLIS);
        LoadJobSourceType sourceType = parseSourceType(request.getSingleParameter(SOURCE_TYPE));

        Integer channelId = Optional
                .ofNullable(request.getRequest().headers().get(CHANNEL_ID_STR))
                .map(Integer::parseInt)
                .orElse(null);

        Integer channelNum = Optional
                .ofNullable(request.getRequest().headers().get(CHANNEL_NUM_STR))
                .map(Integer::parseInt)
                .orElse(null);

        if (channelNum != null && channelId == null) {
            throw new DdlException("Must provide channel_id when stream load begin.");
        }

        if (channelNum == null && channelId != null) {
            throw new DdlException("Must provide channel_num when stream load begin.");
        }

        Channel channel = new Channel(channelId, channelNum);
        if (LoadJobSourceType.BYPASS_WRITE.equals(sourceType) && channel.notNull()) {
            throw new StarRocksException(String.format(
                    "Param %s and %s is not expected when source type is %s",
                    CHANNEL_NUM_STR, CHANNEL_ID_STR, sourceType));
        }

        Body body = new Body();
        if (txnOperation != TransactionOperation.TXN_LOAD && StringUtils.isNotBlank(request.getContent())) {
            try {
                LOG.info("Parse request body, label: {}, {}", label, request.getContent());
                body = mapper.readValue(request.getContent(), new TypeReference<>() {
                });
                if (null == body) {
                    throw new StarRocksHttpException(
                            HttpResponseStatus.BAD_REQUEST, "Malformed json tablets, label is " + label);
                }
            } catch (JsonProcessingException e) {
                LOG.warn("Parse request body error, label: {}, {}", label, e.getMessage());
                throw new StarRocksHttpException(
                        HttpResponseStatus.BAD_REQUEST, "Malformed json tablets, label is " + label);
            }
        }

        return new TransactionOperationParams(
                dbName,
                tableName,
                warehouseName,
                label,
                txnOperation,
                timeoutMillis,
                channel,
                sourceType,
                body
        );
    }

    private static LoadJobSourceType parseSourceType(String sourceType) throws StarRocksException {
        if (StringUtils.isBlank(sourceType)) {
            return null;
        }

        try {
            LoadJobSourceType jobSourceType = LoadJobSourceType.valueOf(Integer.parseInt(sourceType));
            if (null == jobSourceType) {
                throw new StarRocksException("Unknown source type: " + sourceType);
            }

            return jobSourceType;
        } catch (NumberFormatException e) {
            throw new StarRocksException("Invalid source type: " + sourceType);
        }
    }

    private <T> T accessTxnNodeMapWithReadLock(Function<Map<String, Long>, T> function) {
        txnNodeMapAccessLock.readLock().lock();
        try {
            return function.apply(txnNodeMap);
        } finally {
            txnNodeMapAccessLock.readLock().unlock();
        }
    }

    private <T> T accessTxnNodeMapWithWriteLock(Function<Map<String, Long>, T> function) {
        txnNodeMapAccessLock.writeLock().lock();
        try {
            return function.apply(txnNodeMap);
        } finally {
            txnNodeMapAccessLock.writeLock().unlock();
        }
    }

    /* helper classes */

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private static class OpMetrics {
        LongCounterMetric opRunningNum;
        Histogram opLatencyMs;

        static OpMetrics of(LongCounterMetric opRunningNum, Histogram opLatencyMs) {
            OpMetrics metrics = new OpMetrics();
            metrics.opRunningNum = opRunningNum;
            metrics.opLatencyMs = opLatencyMs;
            return metrics;
        }
    }
}

