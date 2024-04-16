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
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TListSessionOptions;
import com.starrocks.thrift.TListSessionRequest;
import com.starrocks.thrift.TListSessionResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TSessionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class SessionCleaner extends FrontendDaemon  {
    private static final Logger LOG = LogManager.getLogger(SessionCleaner.class);
    private static final int SESSION_CLEANER_THREAD_NUM = 1;
    private static final int SESSION_CLEANER_QUEUE_SIZE = 100000;

    private final ExecutorService executor;

    public SessionCleaner() {
        this.executor = ThreadPoolManager.newDaemonFixedThreadPool(SESSION_CLEANER_THREAD_NUM, SESSION_CLEANER_QUEUE_SIZE,
                "session-cleaner-pool", false);
    }

    @Override
    protected void runAfterCatalogReady() {
        List<Frontend> frontends = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null);
        List<UUID> aliveSessions = new ArrayList<>();

        TListSessionOptions options = new TListSessionOptions();
        options.setTemporary_table_only(true);
        TListSessionRequest request = new TListSessionRequest();
        request.setOptions(options);

        for (Frontend frontend : frontends) {
            try {
                TNetworkAddress thriftAddress = new TNetworkAddress(frontend.getHost(), frontend.getRpcPort());
                TListSessionResponse response = FrontendServiceProxy.call(
                        thriftAddress, 1000, Config.thrift_rpc_retry_times, client -> client.listSessions(request));
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
                LOG.info("listSessions return error from {}:{}, skip clean session",
                        frontend.getHost(), frontend.getRpcPort(), e);
                return;
            }
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
        Set<UUID> recordSessions = temporaryTableMgr.listSessions();
        for (UUID sessionId : recordSessions) {
            if (!aliveSessions.contains(sessionId)) {
                LOG.debug("cannot find alive session {}, should clean all temporary tables on it", sessionId);
                metadataMgr.cleanTemporaryTables(sessionId);
            }
        }
    }
}
