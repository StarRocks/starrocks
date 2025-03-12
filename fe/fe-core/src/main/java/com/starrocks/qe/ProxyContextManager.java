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

package com.starrocks.qe;

import com.google.common.collect.Maps;

import java.util.Map;

// When a query is forwarded from the follower FE to the leader FE, this ConnectContext on the leader is
// not managed by the ConnectScheduler. These ConnectContexts are managed by the ProxyContextManager.
// We can find these ConnectContexts on the leader by hostname and connection id, and perform operations such as kill.
public class ProxyContextManager {
    private final Map<String, Map<Integer, ConnectContext>> connectionMaps = Maps.newConcurrentMap();

    static ProxyContextManager instance = new ProxyContextManager();

    public static ProxyContextManager getInstance() {
        return instance;
    }

    public ScopeGuard guard(String hostName, int connectionId, ConnectContext context, boolean set) {
        return new ScopeGuard(this, hostName, connectionId, context, set);
    }

    public synchronized void addContext(String hostname, Integer connectionId, ConnectContext context) {
        final Map<Integer, ConnectContext> contextMap =
                connectionMaps.computeIfAbsent(hostname, (String host) -> Maps.newConcurrentMap());
        contextMap.put(connectionId, context);
        contextMap.computeIfAbsent(connectionId, cid -> context);
    }

    public ConnectContext getContext(String hostname, Integer connectionId) {
        final Map<Integer, ConnectContext> contextMap = connectionMaps.get(hostname);
        if (contextMap == null) {
            return null;
        }
        return contextMap.get(connectionId);
    }

    public ConnectContext getContextByQueryId(String queryId) {
        return connectionMaps.values().stream().flatMap(item -> item.values().stream()).filter(ctx ->
                ctx.getQueryId() != null && queryId.equals(ctx.getQueryId().toString())).findFirst().orElse(null);
    }

    public synchronized void remove(String hostname, Integer connectionId) {
        final Map<Integer, ConnectContext> contextMap = connectionMaps.get(hostname);
        if (contextMap != null) {
            contextMap.remove(connectionId);
            if (contextMap.isEmpty()) {
                connectionMaps.remove(hostname);
            }
        }
    }

    public int getTotalConnCount() {
        return connectionMaps.size();
    }

    public static class ScopeGuard implements AutoCloseable {
        private ProxyContextManager manager;
        private boolean set = false;
        private String hostName;
        private int connectionId;

        public ScopeGuard(ProxyContextManager manager, String hostName, int connectionId, ConnectContext context,
                          boolean set) {
            if (set) {
                this.manager = manager;
                this.hostName = hostName;
                this.connectionId = connectionId;
                manager.addContext(hostName, connectionId, context);
            }
            this.set = set;
        }

        @Override
        public void close() throws Exception {
            if (set) {
                manager.remove(hostName, connectionId);
            }
        }
    }
}
