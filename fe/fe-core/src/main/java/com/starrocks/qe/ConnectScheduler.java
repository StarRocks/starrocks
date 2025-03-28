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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectScheduler.java

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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.system.Frontend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectScheduler {
    private static final Logger LOG = LogManager.getLogger(ConnectScheduler.class);
    private final AtomicInteger maxConnections;
    private final AtomicInteger numberConnection;
    private final ConnectionIdGenerator connectionIdGenerator;

    private final Map<Long, ConnectContext> connectionMap = Maps.newConcurrentMap();
    private final Map<String, ArrowFlightSqlConnectContext> arrowFlightSqlConnectContextMap = Maps.newConcurrentMap();

    private final Map<String, AtomicInteger> connCountByUser = Maps.newConcurrentMap();
    private final ReentrantLock connStatsLock = new ReentrantLock();

    public ConnectScheduler(int maxConnections) {
        this.maxConnections = new AtomicInteger(maxConnections);
        numberConnection = new AtomicInteger(0);
        connectionIdGenerator = new ConnectionIdGenerator();
        // Use a thread to check whether connection is timeout. Because
        // 1. If use a scheduler, the task maybe a huge number when query is messy.
        //    Let timeout is 10m, and 5000 qps, then there are up to 3000000 tasks in scheduler.
        // 2. Use a thread to poll maybe lose some accurate, but is enough to us.
        ScheduledExecutorService checkTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
                "Connect-Scheduler-Check-Timer", true);
        checkTimer.scheduleAtFixedRate(new TimeoutChecker(), 0, 1000L, TimeUnit.MILLISECONDS);
    }

    private class TimeoutChecker extends TimerTask {
        @Override
        public void run() {
            try {
                long now = System.currentTimeMillis();
                synchronized (ConnectScheduler.this) {
                    //Because unregisterConnection will be callback in NMysqlChannel's close,
                    //unregisterConnection will remove connectionMap (in the same thread)
                    //This will result in a concurrentModifyException.
                    //So here we copied the connectionIds to avoid removing iterator during operate iterator
                    ArrayList<Long> connectionIds = new ArrayList<>(connectionMap.keySet());
                    for (Long connectId : connectionIds) {
                        ConnectContext connectContext = connectionMap.get(connectId);
                        try (var guard = connectContext.bindScope()) {
                            connectContext.checkTimeout(now);
                        }
                    }

                    // remove arrow flight sql timeout connect
                    ArrayList<String> arrowFlightSqlConnections =
                            new ArrayList<>(arrowFlightSqlConnectContextMap.keySet());
                    for (String token : arrowFlightSqlConnections) {
                        ConnectContext connectContext = arrowFlightSqlConnectContextMap.get(token);
                        try (var guard = connectContext.bindScope()) {
                            connectContext.checkTimeout(now);
                        }
                    }
                }
            } catch (Throwable e) {
                // Catch Exception to avoid thread exit
                LOG.warn("Timeout checker exception, Internal error:", e);
            }
        }
    }

    /**
     * Register one connection with its connection id.
     *
     * @param ctx connection context
     * @return a pair, first is success or not, second is error message(if any)
     */
    public Pair<Boolean, String> registerConnection(ConnectContext ctx) {
        try {
            connStatsLock.lock();
            if (numberConnection.get() >= maxConnections.get()) {
                return new Pair<>(false, "Reach cluster-wide connection limit, qe_max_connection=" + maxConnections +
                        ", connectionMap.size=" + connectionMap.size() +
                        ", node=" + ctx.getGlobalStateMgr().getNodeMgr().getSelfNode());
            }
            // Check user
            connCountByUser.computeIfAbsent(ctx.getQualifiedUser(), k -> new AtomicInteger(0));
            AtomicInteger currentConnAtomic = connCountByUser.get(ctx.getQualifiedUser());
            int currentConn = currentConnAtomic.get();
            long currentUserMaxConn =
                    ctx.getGlobalStateMgr().getAuthenticationMgr().getMaxConn(ctx.getCurrentUserIdentity());
            if (currentConn >= currentUserMaxConn) {
                String userErrMsg = "Reach user-level(qualifiedUser: " + ctx.getQualifiedUser() +
                        ", currUserIdentity: " + ctx.getCurrentUserIdentity() + ") connection limit, " +
                        "currentUserMaxConn=" + currentUserMaxConn + ", connectionMap.size=" + connectionMap.size() +
                        ", connByUser.totConn=" + connCountByUser.values().stream().mapToInt(AtomicInteger::get).sum() +
                        ", user.currConn=" + currentConn +
                        ", node=" + ctx.getGlobalStateMgr().getNodeMgr().getSelfNode();
                LOG.info(userErrMsg + ", details: connectionId={}, connByUser={}",
                        ctx.getConnectionId(), connCountByUser);
                return new Pair<>(false, userErrMsg);
            }
            numberConnection.incrementAndGet();
            currentConnAtomic.incrementAndGet();
            connectionMap.put((long) ctx.getConnectionId(), ctx);

            if (ctx instanceof ArrowFlightSqlConnectContext) {
                ArrowFlightSqlConnectContext context = (ArrowFlightSqlConnectContext) ctx;
                arrowFlightSqlConnectContextMap.put(context.getToken(), context);
            }

            return new Pair<>(true, null);
        } finally {
            connStatsLock.unlock();
        }
    }

    public void unregisterConnection(ConnectContext ctx) {
        boolean removed;
        try {
            connStatsLock.lock();
            removed = connectionMap.remove((long) ctx.getConnectionId()) != null;
            if (removed) {
                numberConnection.decrementAndGet();
                AtomicInteger conns = connCountByUser.get(ctx.getQualifiedUser());
                if (conns != null) {
                    conns.decrementAndGet();
                }
                LOG.info("Connection closed. remote={}, connectionId={}, qualifiedUser={}, user.currConn={}",
                        ctx.getMysqlChannel().getRemoteHostPortString(), ctx.getConnectionId(),
                        ctx.getQualifiedUser(), conns != null ? Integer.toString(conns.get()) : "nil");
            }

            if (ctx instanceof ArrowFlightSqlConnectContext) {
                ArrowFlightSqlConnectContext context = (ArrowFlightSqlConnectContext) ctx;
                arrowFlightSqlConnectContextMap.remove(context.getToken());
            }
        } finally {
            connStatsLock.unlock();
        }

        if (removed) {
            ctx.cleanTemporaryTable();
        }
    }

    public ConnectContext getContext(long connectionId) {
        return connectionMap.get(connectionId);
    }

    public ConnectContext getContext(String token) {
        return connectionMap.get(token);
    }

    public ArrowFlightSqlConnectContext getArrowFlightSqlConnectContext(String token) {
        return arrowFlightSqlConnectContextMap.get(token);
    }

    public ConnectContext findContextByQueryId(String queryId) {
        return connectionMap.values().stream().filter(
                        (Predicate<ConnectContext>) c ->
                                c.getQueryId() != null
                                        && queryId.equals(c.getQueryId().toString())
                )
                .findFirst().orElse(null);
    }

    public ConnectContext findContextByCustomQueryId(String customQueryId) {
        return connectionMap.values().stream().filter(
                (Predicate<ConnectContext>) c -> customQueryId.equals(c.getCustomQueryId())).findFirst().orElse(null);
    }

    public Map<String, AtomicInteger> getUserConnectionMap() {
        return connCountByUser;
    }

    private List<ConnectContext.ThreadInfo> getAllConnThreadInfoByUser(ConnectContext connectContext,
                                                                       String currUser,
                                                                       String forUser) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();
        ConnectContext currContext = connectContext == null ? ConnectContext.get() : connectContext;

        for (ConnectContext ctx : connectionMap.values()) {
            // Check authorization first.
            if (!ctx.getQualifiedUser().equals(currUser)) {
                try {
                    Authorizer.checkSystemAction(currContext, PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    continue;
                }
            }

            // Check whether it's the connection for the specified user.
            if (forUser != null && !ctx.getQualifiedUser().equals(forUser)) {
                continue;
            }

            infos.add(ctx.toThreadInfo());
        }
        return infos;
    }

    public List<ConnectContext.ThreadInfo> listConnection(String currUser, String forUser) {
        return getAllConnThreadInfoByUser(null, currUser, forUser);
    }

    public List<ConnectContext.ThreadInfo> listConnection(ConnectContext context, String currUser) {
        return getAllConnThreadInfoByUser(context, currUser, null);
    }

    public Set<UUID> listAllSessionsId() {
        Set<UUID> sessionIds = new HashSet<>();
        try (CloseableLock ignored = CloseableLock.lock(this.connStatsLock)) {
            connectionMap.values().forEach(ctx -> {
                sessionIds.add(ctx.getSessionId());
            });
        }
        return sessionIds;
    }

    public int getTotalConnCount() {
        return connectionMap.size();
    }

    public void closeAllIdleConnection() {
        try (CloseableLock ignored = CloseableLock.lock(this.connStatsLock)) {
            connectionMap.values().forEach(context -> {
                if (context.isIdleLastFor(1000)) {
                    context.cleanup();
                }
            });
        }
    }

    /**
     * Generates a unique connection ID by combining the frontend node's GID and an atomic counter.
     * <p>
     * The connection ID structure:
     * - The higher 8 bits (bits 24-31) represent the frontend node's GID (masked to 8 bits).
     * - The lower 24 bits (bits 0-23) represent an incrementing counter that resets at 2^24.
     *
     * @return a unique connection ID
     */
    public int getNextConnectionId() {
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        return (frontend.getFid() & 0xFF) << 24 | (connectionIdGenerator.incrementAndGet() & 0xFFFFFF);
    }

    public static class ConnectionIdGenerator {
        // Atomic counter to ensure thread-safe increments
        private final AtomicInteger counter;
        // Threshold value at which the counter resets
        private final int threshold;

        /**
         * Default constructor, setting the threshold to 2^24 (16,777,216).
         * This ensures that the counter cycles within 24-bit range.
         */
        public ConnectionIdGenerator() {
            this.counter = new AtomicInteger(0);
            this.threshold = 1 << 24;
        }

        /**
         * Atomically increments the counter and resets it when the threshold is reached.
         * Ensures the counter remains within the valid 24-bit range.
         *
         * @return the updated counter value after incrementing
         */
        public int incrementAndGet() {
            return counter.updateAndGet(currentValue -> (currentValue + 1 >= threshold) ? 0 : currentValue + 1
            );
        }
    }
}
