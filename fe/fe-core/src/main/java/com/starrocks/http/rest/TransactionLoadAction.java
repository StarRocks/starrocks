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
import com.google.common.base.Strings;
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpMetricRegistry;
import com.starrocks.http.IllegalArgException;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionStatus;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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
        synchronized (this) {
            return txnNodeMap.size();
        }
    }

    public static TransactionLoadAction getAction() {
        return ac;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ac = new TransactionLoadAction(controller);
        controller.registerHandler(HttpMethod.POST, "/api/transaction/{" + TXN_OP_KEY + "}", ac);
        controller.registerHandler(HttpMethod.PUT,
                "/api/transaction/{" + TXN_OP_KEY + "}", ac);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        OpMetrics opMetrics = null;
        long startTime = System.currentTimeMillis();
        try {
            if (redirectToLeader(request, response)) {
                return;
            }
            String op = request.getSingleParameter(TXN_OP_KEY);
            opMetrics = opMetricsMap.get(op);
            if (opMetrics != null) {
                opMetrics.opRunningNum.increase(1L);
            }
            executeTransaction(request, response, op);
        } catch (Exception e) {
            TransactionResult resp = new TransactionResult();
            if (e instanceof LabelAlreadyUsedException) {
                resp.status = ActionStatus.LABEL_ALREADY_EXISTS;
                resp.msg = e.getMessage();
                resp.addResultEntry("ExistingJobStatus", ((LabelAlreadyUsedException) e).getJobStatus());
            } else {
                resp.status = ActionStatus.FAILED;
                resp.msg = e.getClass().toString() + ": " + e.getMessage();
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
                // 2.1 save label->be map when begin transaction, so that subsequent operator can send to same BE
                if (op.equalsIgnoreCase(TXN_BEGIN)) {
                    nodeID = GlobalStateMgr.getCurrentSystemInfo().seqChooseBackendOrComputeId();
                    // txnNodeMap is LRU cache, it atomic remove unused entry
                    txnNodeMap.put(label, nodeID);
                } else if (channelIdStr == null) {
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

