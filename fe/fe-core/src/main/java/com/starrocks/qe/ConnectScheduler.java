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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.system.Frontend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectScheduler {
    private static final Logger LOG = LogManager.getLogger(ConnectScheduler.class);
    private static final int CONNECTION_ID_SPACE = 1 << 24;
    private static final int CONNECTION_ID_MASK = CONNECTION_ID_SPACE - 1;
    private final AtomicInteger maxConnections;
    private final ConnectionIdGenerator connectionIdGenerator;

    // mysql connectContext/ http connectContext/ arrowFlight connectContext all stored in connectionMap
    private final Map<Long, ConnectContext> connectionMap = Maps.newConcurrentMap();
    private final Map<String, ArrowFlightSqlConnectContext> arrowFlightSqlConnectContextMap = Maps.newConcurrentMap();

    private final Map<String, AtomicInteger> connCountByUser = Maps.newConcurrentMap();
    private final ReentrantLock connStatsLock = new ReentrantLock();

    public static final class ConnectionIdExhaustedException extends Exception {
        public ConnectionIdExhaustedException(String message) {
            super(message);
        }
    }

    public ConnectScheduler(int maxConnections) {
        this(maxConnections, CONNECTION_ID_SPACE);
    }

    @VisibleForTesting
    ConnectScheduler(int maxConnections, int connectionIdSpace) {
        if (connectionIdSpace <= 0 || connectionIdSpace > CONNECTION_ID_SPACE) {
            throw new IllegalArgumentException("connection ID space must be between 1 and " + CONNECTION_ID_SPACE);
        }
        this.maxConnections = new AtomicInteger(maxConnections);
        connectionIdGenerator = new ConnectionIdGenerator(connectionIdSpace);
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
                    // ConcurrentHashMap's iterator is weakly consistent, safe to iterate directly
                    // even when modifications occur during iteration
                    for (ConnectContext connectContext : connectionMap.values()) {
                        if (connectContext != null) {
                            try (var guard = connectContext.bindScope()) {
                                connectContext.checkTimeout(now);
                            }
                        }
                    }

                    // remove arrow flight sql timeout connect
                    for (ConnectContext connectContext : arrowFlightSqlConnectContextMap.values()) {
                        if (connectContext != null) {
                            try (var guard = connectContext.bindScope()) {
                                connectContext.checkTimeout(now);
                            }
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
            ConnectContext existing = connectionMap.get((long) ctx.getConnectionId());
            if (existing != null) {
                return handleExistingConnection(ctx, existing);
            }

            if (connectionMap.size() >= maxConnections.get()) {
                return new Pair<>(false, "Reach cluster-wide connection limit, qe_max_connection=" + maxConnections +
                        ", connectionMap.size=" + connectionMap.size() +
                        ", node=" + ctx.getGlobalStateMgr().getNodeMgr().getSelfNode());
            }
            // Check user
            AtomicInteger currentConnAtomic = connCountByUser.get(ctx.getQualifiedUser());
            int currentConn = currentConnAtomic == null ? 0 : currentConnAtomic.get();
            long currentUserMaxConn = ctx.getGlobalStateMgr().getAuthenticationMgr().getMaxConn(ctx.getQualifiedUser());
            if (currentConn >= currentUserMaxConn) {
                String userErrMsg = "Reach user-level(qualifiedUser: " + ctx.getQualifiedUser() + ") connection limit, " +
                        "currentUserMaxConn=" + currentUserMaxConn + ", connectionMap.size=" + connectionMap.size() +
                        ", connByUser.totConn=" + connCountByUser.values().stream().mapToInt(AtomicInteger::get).sum() +
                        ", user.currConn=" + currentConn +
                        ", node=" + ctx.getGlobalStateMgr().getNodeMgr().getSelfNode();
                LOG.info(userErrMsg + ", details: connectionId={}, connByUser={}",
                        ctx.getConnectionId(), connCountByUser);
                return new Pair<>(false, userErrMsg);
            }

            existing = connectionMap.putIfAbsent((long) ctx.getConnectionId(), ctx);
            if (existing != null) {
                return handleExistingConnection(ctx, existing);
            }
            connCountByUser.computeIfAbsent(ctx.getQualifiedUser(), ignored -> new AtomicInteger()).incrementAndGet();

            if (ctx.isArrowFlightSql()) {
                ArrowFlightSqlConnectContext context = (ArrowFlightSqlConnectContext) ctx;
                arrowFlightSqlConnectContextMap.put(context.getArrowFlightSqlToken(), context);
            }

            return new Pair<>(true, null);
        } finally {
            connStatsLock.unlock();
        }
    }

    private Pair<Boolean, String> handleExistingConnection(ConnectContext ctx, ConnectContext existing) {
        if (existing == ctx) {
            return Pair.create(true, null);
        }
        String message = "Connection ID " + ctx.getConnectionId() + " is already in use";
        LOG.warn("{}. existingUser={}, newUser={}", message, existing.getQualifiedUser(), ctx.getQualifiedUser());
        return Pair.create(false, message);
    }

    public Pair<Boolean, String> onUserChanged(ConnectContext ctx, String oldQualifiedUser, String newQualifiedUser) {
        if (Objects.equals(oldQualifiedUser, newQualifiedUser)) {
            return new Pair<>(true, null);
        }

        if (newQualifiedUser == null) {
            return new Pair<>(false, "new qualifiedUser is null");
        }

        try {
            connStatsLock.lock();
            AtomicInteger newCounter = connCountByUser.computeIfAbsent(newQualifiedUser, k -> new AtomicInteger(0));
            int currentNewCount = newCounter.get();
            long currentUserMaxConn = GlobalStateMgr.getCurrentState().getAuthenticationMgr().getMaxConn(newQualifiedUser);

            if (currentNewCount >= currentUserMaxConn) {
                int totalConn = connCountByUser.values().stream().mapToInt(AtomicInteger::get).sum();
                String userErrMsg = "Reach user-level(qualifiedUser: " + newQualifiedUser
                        + ", currUserIdentity: " + ctx.getCurrentUserIdentity() + ") connection limit, "
                        + "currentUserMaxConn=" + currentUserMaxConn + ", connectionMap.size="
                        + connectionMap.size() + ", connByUser.totConn=" + totalConn
                        + ", user.currConn=" + currentNewCount;
                LOG.info("{}, details: connectionId={}, connByUser={}", userErrMsg, ctx.getConnectionId(), connCountByUser);
                return new Pair<>(false, userErrMsg);
            }

            newCounter.incrementAndGet();

            if (oldQualifiedUser != null) {
                AtomicInteger oldCounter = connCountByUser.get(oldQualifiedUser);
                if (oldCounter != null) {
                    int oldCountAfterDecrement = oldCounter.decrementAndGet();
                    if (oldCountAfterDecrement < 0) {
                        LOG.warn("Negative connection count detected for user {} during user change of connection {}",
                                oldQualifiedUser, ctx.getConnectionId());
                        oldCounter.set(0);
                        oldCountAfterDecrement = 0;
                    }
                    if (oldCountAfterDecrement == 0) {
                        connCountByUser.remove(oldQualifiedUser, oldCounter);
                    }
                } else {
                    LOG.warn("Missing connection counter for user {} during user change of connection {}",
                            oldQualifiedUser, ctx.getConnectionId());
                }
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
            // Identity-aware remove: only drop the mapping if the entry at this
            // connectionId is this exact context. A context can hold a stale
            // connectionId after a failed registerConnection(), and the 24-bit
            // counter in ConnectionIdGenerator wraps after 2^24 connections, so
            // a blind remove(key) risks evicting a different live session that
            // happens to own the same id.
            removed = connectionMap.remove((long) ctx.getConnectionId(), ctx);
            if (removed) {
                AtomicInteger conns = connCountByUser.get(ctx.getQualifiedUser());
                if (conns != null) {
                    if (conns.decrementAndGet() <= 0) {
                        connCountByUser.remove(ctx.getQualifiedUser());
                    }
                }
                LOG.info("Connection closed. remote={}, connectionId={}, qualifiedUser={}, user.currConn={}",
                        ctx.getMysqlChannel().getRemoteHostPortString(), ctx.getConnectionId(),
                        ctx.getQualifiedUser(), conns != null ? Integer.toString(conns.get()) : "nil");
            }

            if (ctx.isArrowFlightSql()) {
                ArrowFlightSqlConnectContext context = (ArrowFlightSqlConnectContext) ctx;
                arrowFlightSqlConnectContextMap.remove(context.getArrowFlightSqlToken());
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

    public int getConnectionNum() {
        return connectionMap.size();
    }

    public Map<String, AtomicInteger> getUserConnectionMap() {
        return connCountByUser;
    }

    public Map<Long, ConnectContext> getCurrentConnectionMap() {
        return connectionMap;
    }

    public List<ConnectContext.ThreadInfo> listConnection(ConnectContext currentContext, String forUser) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();
        for (ConnectContext contextToShow : connectionMap.values()) {
            // Check authorization first.
            if (!contextToShow.getQualifiedUser().equals(currentContext.getCurrentUserIdentity().getUser())) {
                try {
                    Authorizer.checkSystemAction(currentContext, PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    continue;
                }
            }

            // Check whether it's the connection for the specified user.
            if ((forUser != null && !contextToShow.getQualifiedUser().equals(forUser)) ||
                    (Config.authorization_enable_admin_user_protection &&
                            contextToShow.getQualifiedUser().equals(AuthenticationMgr.ROOT_USER))) {
                continue;
            }

            infos.add(contextToShow.toThreadInfo());
        }
        return infos;
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
        // Only select candidates under the lock; run cleanup() after releasing it. A follower
        // cleanup may forward an explicit-txn rollback to the leader, a synchronous Thrift RPC,
        // and doing that while holding connStatsLock would stall register/unregisterConnection
        // (both take the same lock), keeping totalConns above 0 and blocking the graceful-exit
        // drain until the hard timeout.
        List<ConnectContext> toCleanup = Lists.newArrayList();
        try (CloseableLock ignored = CloseableLock.lock(this.connStatsLock)) {
            connectionMap.values().forEach(context -> {
                // Skip connections with an active explicit transaction while the graceful-exit drain
                // window is still open, so a transaction in flight gets a chance to commit/abort.
                // Once the window elapses, close it too: disconnecting an idle explicit transaction
                // rolls it back, which is required for totalConns to reach 0 and graceful shutdown
                // to finish instead of hitting the hard timeout.
                boolean explicitTxnExempt = context.inActiveExplicitTransaction()
                        && !GracefulExitFlag.isDrainWindowElapsed();
                if (!explicitTxnExempt && context.isIdleLastFor(1000)) {
                    toCleanup.add(context);
                }
            });
        }
        toCleanup.forEach(context -> {
            // Recheck idleness immediately before cleanup. Between collecting candidates under
            // the lock and reaching this context in the loop, the connection may have received
            // and started a new statement (the window can be long when an earlier follower
            // cleanup waits on a synchronous rollback RPC). Cleanup without rechecking would
            // close an active client's socket and roll back its explicit transaction
            // mid-statement.
            if (context.isIdleLastFor(1000)) {
                context.cleanup();
            }
        });
    }

    public void printAllRunningQuery() {
        connectionMap.values().stream().forEach(ctx -> {
            if (ctx.getCommand() == MysqlCommand.COM_QUERY || ctx.getCommand() == MysqlCommand.COM_STMT_EXECUTE ||
                    ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
                if (ctx.getExecutor() != null && ctx.getExecutor().getParsedStmt() != null &&
                        ctx.getExecutor().getParsedStmt().getOrigStmt() != null) {
                    long threadId = ctx.getCurrentThreadId();
                    long theadAllocatedBytes = 0;
                    if (threadId != 0) {
                        theadAllocatedBytes = ConnectProcessor.getThreadAllocatedBytes(threadId) -
                                ctx.getCurrentThreadAllocatedMemory();
                    }
                    LOG.warn("FE ShutDown! Running Query:{},  QueryFEAllocatedMemory: {}",
                            ctx.getExecutor().getParsedStmt().getOrigStmt().getOrigStmt(), theadAllocatedBytes);
                }
            }
        });
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
    public int getNextConnectionId() throws ConnectionIdExhaustedException {
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        int fePrefix = (frontend.getFid() & 0xFF) << 24;
        for (int attempt = 0; attempt < connectionIdGenerator.getThreshold(); attempt++) {
            int candidate = fePrefix | (connectionIdGenerator.incrementAndGet() & CONNECTION_ID_MASK);
            if (!connectionMap.containsKey((long) candidate)) {
                return candidate;
            }
        }
        throw new ConnectionIdExhaustedException(
                "No available connection ID on frontend " + frontend.getNodeName());
    }

    @VisibleForTesting
    public void setNextConnectionId(int connectionId) {
        connectionIdGenerator.counter.set(connectionId);
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
            this(CONNECTION_ID_SPACE);
        }

        public ConnectionIdGenerator(int threshold) {
            this.counter = new AtomicInteger(0);
            this.threshold = threshold;
        }

        public int getThreshold() {
            return threshold;
        }

        /**
         * Atomically increments the counter and resets it when the threshold is reached.
         * Ensures the counter remains within the valid 24-bit range.
         *
         * @return the updated counter value after incrementing
         */
        public int incrementAndGet() {
            return counter.updateAndGet(currentValue -> (currentValue + 1 >= threshold) ? 0 : currentValue + 1);
        }
    }
}
