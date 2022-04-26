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
import com.starrocks.catalog.Catalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.rpc.FrontendServiceProxy;
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

    private static final int RETRY_TIMES = 2;
    private final OriginStatement originStmt;
    private StatementBase parsedStmt;
    private final ConnectContext ctx;
    private TMasterOpResult result;

    private int waitTimeoutMs;
    // the total time of thrift connectTime add readTime and writeTime
    private int thriftTimeoutMs;

    private boolean isQuery;

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
        this.isQuery = isQuery;
        this.parsedStmt = parsedStmt;
    }

    public void execute() throws Exception {
        forward();
<<<<<<< HEAD
        LOG.info("forwarding to master get result max journal id: {}", result.maxJournalId);
        ctx.getGlobalStateMgr().getJournalObservable().waitOn(result.maxJournalId, waitTimeoutMs);
=======
        if (!Catalog.getCurrentCatalog().isMaster()) {
            LOG.info("forwarding to master get result max journal id: {}", result.maxJournalId);
            ctx.getCatalog().getJournalObservable().waitOn(result.maxJournalId, waitTimeoutMs);
        }
>>>>>>> f90e64d35... add

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
        for (int i = 0; i < RETRY_TIMES; i++) {
            TNetworkAddress thriftAddress = new TNetworkAddress(ctx.getCatalog().getMasterIp(),
                    ctx.getCatalog().getMasterRpcPort());
            LOG.info("Forward statement {} to Master {}, retried times: {}", ctx.getStmtId(), thriftAddress, i);
            try {
                result = FrontendServiceProxy.call(thriftAddress, thriftTimeoutMs,
                        client -> client.forward(params));
                break;
            } catch (TTransportException e) {
                LOG.warn("Forward statement {} to Master {} failed, error type {}",
                        ctx.getStmtId(), thriftAddress, e.getType(), e);

                // END_OF_FILE means server is stopped, only retry on this case
                if (!isQuery && e.getType() != TTransportException.END_OF_FILE) {
                    break;
                } else {
                    Thread.sleep(10000);
                }
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

