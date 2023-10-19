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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/HeartbeatMgr.java

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

package com.starrocks.system;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Check the connectivity of a port, currently for edit log port and rpc_port
 */
public class PortConnectivityChecker extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(PortConnectivityChecker.class);
    private final ExecutorService executor;
    private final Map<Pair<String, Integer>, Boolean> currentPortStates = new HashMap<>();

    private enum PortType {
        RPC_PORT,
        EDIT_LOG_PORT
    }

    public PortConnectivityChecker() {
        super("PortConnectivityChecker");
        executor = ThreadPoolManager.newDaemonFixedThreadPool(4,
                64, "port-connectivity-checker", true);
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(Config.port_connectivity_check_interval_sec * 1000L);

        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();
        List<Frontend> allFrontends = nodeMgr.getFrontends(null);
        Map<Pair<String, Integer>, Future<Boolean>> frontendFutureMap = new HashMap<>();
        for (Frontend frontend : allFrontends) {
            Frontend myself = nodeMgr.getMySelf();
            if (Objects.equals(myself.getHost(), frontend.getHost())) {
                // ignore checking self
                continue;
            }
            for (PortType portType : PortType.values()) {
                int port = -1;
                if (portType.equals(PortType.RPC_PORT)) {
                    port = frontend.getRpcPort();
                } else if (portType.equals(PortType.EDIT_LOG_PORT)) {
                    port = frontend.getEditLogPort();
                }
                final int finalPort = port;
                frontendFutureMap.put(new Pair<>(frontend.getHost(), finalPort),
                        executor.submit(() -> isPortConnectable(frontend.getHost(), finalPort)));
            }
        }

        for (Map.Entry<Pair<String, Integer>, Future<Boolean>> entry : frontendFutureMap.entrySet()) {
            String host = entry.getKey().first;
            int port = entry.getKey().second;
            Future<Boolean> future = entry.getValue();
            boolean isOpen = false;
            try {
                isOpen = future.get();
                if (!isOpen) {
                    LOG.warn("checking for connectivity of {}:{} failed, not open", host, port);
                }
            } catch (Exception e) {
                LOG.warn("checking for connectivity of {}:{} failed, reason: {}", host, port, e.getMessage());
            }
            currentPortStates.put(entry.getKey(), isOpen);
        }
    }

    private boolean isPortConnectable(String host, int port) {
        long maxRetries = Config.port_connectivity_check_retry_times;
        for (int retry = 1; retry <= maxRetries; retry++) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), Config.port_connectivity_check_timeout_ms);
                return true;
            } catch (IOException e) {
                if (retry < maxRetries) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    LOG.warn("socket connection to {}:{} failed, reason: {}", host, port, e.getMessage());
                }
            }
        }
        return false;
    }

    @VisibleForTesting
    public Map<Pair<String, Integer>, Boolean> getCurrentPortStates() {
        return currentPortStates;
    }
}

