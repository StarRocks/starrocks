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
package com.starrocks.transaction;

import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.AnalysisException;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RemoteTransactionMgr {
    private static final Logger LOG = LogManager.getLogger(RemoteTransactionMgr.class);

    public static long beginTransaction(long dbId, List<Long> tableIdList, String label,
                                        TransactionState.LoadJobSourceType sourceType,
                                        long timeoutSecond,
                                        String host, int port, TAuthenticateParams authenticateParams)
            throws BeginTransactionException, AnalysisException {
        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        if (GlobalStateMgr.getCurrentState().isSafeMode()) {
            throw new AnalysisException(String.format("The cluster is under safe mode state," +
                    " all load jobs are rejected."));
        }

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                        Config.min_load_timeout_second);
                break;
            case LAKE_COMPACTION:
                // skip transaction timeout range check for lake compaction
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }


        TNetworkAddress addr = new TNetworkAddress(host, port);
        TBeginRemoteTxnRequest request = new TBeginRemoteTxnRequest();
        request.setDb_id(dbId);
        for (Long tableId : tableIdList) {
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
    public static boolean commitRemoteTransaction(long dbId, long transactionId,
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
    public static void abortRemoteTransaction(long dbId, long transactionId,
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

    private static void checkValidTimeoutSecond(long timeoutSecond, int maxLoadTimeoutSecond, int minLoadTimeOutSecond)
            throws AnalysisException {
        if (timeoutSecond > maxLoadTimeoutSecond ||
                timeoutSecond < minLoadTimeOutSecond) {
            throw new AnalysisException("Invalid timeout. Timeout should between "
                    + minLoadTimeOutSecond + " and " + maxLoadTimeoutSecond
                    + " seconds");
        }
    }
}
