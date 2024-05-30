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

package com.starrocks.server;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TListSessionsOptions;
import com.starrocks.thrift.TListSessionsRequest;
import com.starrocks.thrift.TListSessionsResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TSessionInfo;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

// TemporaryTableCleaner is used to automatically clean up temporary tables.
// It is only executed on the FE leader.
// It periodically obtains the active session IDs related to all temporary tables on all FEs and
// automatically cleans up temporary tables that are no longer active.
public class TemporaryTableCleaner extends FrontendDaemon  {
    private static final Logger LOG = LogManager.getLogger(TemporaryTableCleaner.class);
    private static final int TEMP_TABLE_CLEANER_THREAD_NUM = 1;
    private static final int TEMP_TABLE_CLEANER_QUEUE_SIZE = 100000;

    private final ExecutorService executor;

    public TemporaryTableCleaner() {
        this.executor = ThreadPoolManager.newDaemonFixedThreadPool(TEMP_TABLE_CLEANER_THREAD_NUM, TEMP_TABLE_CLEANER_QUEUE_SIZE,
                "temp-table-cleaner-pool", false);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        List<Frontend> frontends = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null);
        List<UUID> aliveSessions = new ArrayList<>();

        TListSessionsOptions options = new TListSessionsOptions();
        options.setTemporary_table_only(true);
        TListSessionsRequest request = new TListSessionsRequest();
        request.setOptions(options);

        for (Frontend frontend : frontends) {
            try {
                TNetworkAddress thriftAddress = new TNetworkAddress(frontend.getHost(), frontend.getRpcPort());
                TListSessionsResponse response = FrontendServiceProxy.call(
                        thriftAddress, Config.thrift_rpc_timeout_ms, Config.thrift_rpc_retry_times,
                        client -> client.listSessions(request));
                if (response.getStatus() == null || response.getStatus().getStatus_code() != TStatusCode.OK) {
                    throw new Exception("response status is not ok: " +
                            (response.getStatus() == null ? "NULL" : response.getStatus().getStatus_code()));
                }
                if (response.getSessions() != null) {
                    List<TSessionInfo> sessions = response.getSessions();
                    for (TSessionInfo sessionInfo : sessions) {
                        UUID sessionId = UUID.fromString(sessionInfo.getSession_id());
                        LOG.debug("alive session {} on fe {}:{}",
                                sessionId, frontend.getHost(), frontend.getRpcPort());
                        aliveSessions.add(sessionId);
                    }
                }
            } catch (Throwable e) {
                LOG.warn("listSessions return error from {}:{}, skip clean temporary tables",
                        frontend.getHost(), frontend.getRpcPort(), e);
                return;
            }
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
        Set<UUID> recordSessions = temporaryTableMgr.listSessions();
        for (UUID sessionId : recordSessions) {
            if (!aliveSessions.contains(sessionId)) {
                LOG.info("cannot find alive session {}, should clean all temporary tables on it", sessionId);
                metadataMgr.cleanTemporaryTables(sessionId);
            }
        }
    }
}
