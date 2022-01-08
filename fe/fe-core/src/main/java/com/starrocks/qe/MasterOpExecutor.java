// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/MasterOpExecutor.java

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

package com.starrocks.qe;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.ClientPool;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;

public class MasterOpExecutor {
    private static final Logger LOG = LogManager.getLogger(MasterOpExecutor.class);

    private final OriginStatement originStmt;
    private StatementBase parsedStmt;
    private final ConnectContext ctx;
    private TMasterOpResult result;

    private int waitTimeoutMs;
    // the total time of thrift connectTime add readTime and writeTime
    private int thriftTimeoutMs;

    private boolean shouldNotRetry;

    public MasterOpExecutor(OriginStatement originStmt, ConnectContext ctx, RedirectStatus status, boolean isQuery) {
        this(null, originStmt, ctx, status, isQuery);
    }

    public MasterOpExecutor(StatementBase parsedStmt, OriginStatement originStmt,
                            ConnectContext ctx, RedirectStatus status, boolean isQuery) {
        this.originStmt = originStmt;
        this.ctx = ctx;
        if (status.isNeedToWaitJournalSync()) {
            this.waitTimeoutMs = ctx.getSessionVariable().getQueryTimeoutS() * 1000;
        } else {
            this.waitTimeoutMs = 0;
        }
        this.thriftTimeoutMs = ctx.getSessionVariable().getQueryTimeoutS() * 1000;
        // if isQuery=false, we shouldn't retry twice when catch exception because of Idempotency
        this.shouldNotRetry = !isQuery;
        this.parsedStmt = parsedStmt;
    }

    public void execute() throws Exception {
        forward();
        LOG.info("forwarding to master get result max journal id: {}", result.maxJournalId);
        ctx.getCatalog().getJournalObservable().waitOn(result.maxJournalId, waitTimeoutMs);

        if (result.state != null) {
            MysqlStateType state = MysqlStateType.fromString(result.state);
            if (state != null) {
                ctx.getState().setStateType(state);
                if (state == MysqlStateType.EOF || state == MysqlStateType.OK) {
                    afterForward();
                }
            }
        }
    }

    private void afterForward() throws DdlException {
        if (parsedStmt != null) {
            if (parsedStmt instanceof SetStmt) {
                SetExecutor executor = new SetExecutor(ctx, (SetStmt) parsedStmt);
                try {
                    executor.setSessionVars();
                } catch (DdlException e) {
                    LOG.warn("set session variables after forward failed", e);
                    // set remote result to null, so that mysql protocol will show the error message
                    result = null;
                    throw new DdlException("Global level variables are set successfully, " +
                            "but session level variables are set failed with error: " + e.getMessage() + ". " +
                            "Please check if the version of fe currently connected is the same as the version of master, " +
                            "or re-establish the connection and you will see the new variables");
                }
            }
        }
    }

    // Send request to Master
    private void forward() throws Exception {
        String masterHost = ctx.getCatalog().getMasterIp();
        int masterRpcPort = ctx.getCatalog().getMasterRpcPort();
        TNetworkAddress thriftAddress = new TNetworkAddress(masterHost, masterRpcPort);

        FrontendService.Client client = null;
        try {
            client = ClientPool.frontendPool.borrowObject(thriftAddress, thriftTimeoutMs);
        } catch (Exception e) {
            // may throw NullPointerException. add err msg
            throw new Exception("Failed to get master client.", e);
        }
        TMasterOpRequest params = new TMasterOpRequest();
        params.setCluster(ctx.getClusterName());
        params.setSql(originStmt.originStmt);
        params.setStmtIdx(originStmt.idx);
        params.setUser(ctx.getQualifiedUser());
        params.setDb(ctx.getDatabase());
        params.setSqlMode(ctx.getSessionVariable().getSqlMode());
        params.setResourceInfo(ctx.toResourceCtx());
        params.setUser_ip(ctx.getRemoteIP());
        params.setTime_zone(ctx.getSessionVariable().getTimeZone());
        params.setStmt_id(ctx.getStmtId());
        params.setEnableStrictMode(ctx.getSessionVariable().getEnableInsertStrict());
        params.setCurrent_user_ident(ctx.getCurrentUserIdentity().toThrift());
        params.setIsLastStmt(ctx.getIsLastStmt());

        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setMem_limit(ctx.getSessionVariable().getMaxExecMemByte());
        queryOptions.setQuery_timeout(ctx.getSessionVariable().getQueryTimeoutS());
        queryOptions.setLoad_mem_limit(ctx.getSessionVariable().getLoadMemLimit());
        params.setQuery_options(queryOptions);

        params.setQueryId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        LOG.info("Forward statement {} to Master {}", ctx.getStmtId(), thriftAddress);

        boolean isReturnToPool = false;
        try {
            result = client.forward(params);
            isReturnToPool = true;
        } catch (TTransportException e) {
            LOG.warn("Forward statement {} to Master {} failed, error type {}",
                    ctx.getStmtId(), thriftAddress, e.getType(), e);
            boolean ok = ClientPool.frontendPool.reopen(client, thriftTimeoutMs);
            if (!ok) {
                throw e;
            }
            // NOTE: master execute timeout will return TTransportException.UNKNOWN not TTransportException.TIMED_OUT
            if (shouldNotRetry || e.getType() == TTransportException.TIMED_OUT) {
                throw e;
            } else {
                LOG.info("Retry Forward statement {} to Master {}", ctx.getStmtId(), thriftAddress);
                result = client.forward(params);
                isReturnToPool = true;
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(thriftAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(thriftAddress, client);
            }
        }
    }

    public ByteBuffer getOutputPacket() {
        if (result == null) {
            return null;
        }
        return result.packet;
    }

    public ShowResultSet getProxyResultSet() {
        if (result == null) {
            return null;
        }
        if (result.isSetResultSet()) {
            return new ShowResultSet(result.resultSet);
        } else {
            return null;
        }
    }

    public void setResult(TMasterOpResult result) {
        this.result = result;
    }
}

