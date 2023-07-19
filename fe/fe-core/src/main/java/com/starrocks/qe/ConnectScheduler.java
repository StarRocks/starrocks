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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.LogUtil;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.nio.NConnectContext;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectScheduler {
    private static final Logger LOG = LogManager.getLogger(ConnectScheduler.class);
    private final AtomicInteger maxConnections;
    private final AtomicInteger numberConnection;
    private final AtomicInteger nextConnectionId;

    private final Map<Long, ConnectContext> connectionMap = Maps.newConcurrentMap();
    private final Map<String, AtomicInteger> connCountByUser = Maps.newConcurrentMap();
    private final ExecutorService executor = ThreadPoolManager
            .newDaemonCacheThreadPool(Config.max_connection_scheduler_threads_num, "connect-scheduler-pool", true);

    public ConnectScheduler(int maxConnections) {
        this.maxConnections = new AtomicInteger(maxConnections);
        numberConnection = new AtomicInteger(0);
        nextConnectionId = new AtomicInteger(0);
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
                        connectContext.checkTimeout(now);
                    }
                }
            } catch (Throwable e) {
                //Catch Exception to avoid thread exit
                LOG.warn("Timeout checker exception, Internal error : " + e.getMessage());
            }
        }
    }

    // submit one MysqlContext to this scheduler.
    // return true, if this connection has been successfully submitted, otherwise return false.
    // Caller should close ConnectContext if return false.
    public boolean submit(ConnectContext context) {
        if (context == null) {
            return false;
        }

        context.setConnectionId(nextConnectionId.getAndAdd(1));
        context.resetConnectionStartTime();
        // no necessary for nio or Http.
        if (context instanceof NConnectContext || context instanceof HttpConnectContext) {
            return true;
        }
        if (executor.submit(new LoopHandler(context)) == null) {
            LOG.warn("Submit one thread failed.");
            return false;
        }
        return true;
    }

    // Register one connection with its connection id.
    public boolean registerConnection(ConnectContext ctx) {
        if (numberConnection.get() >= maxConnections.get()) {
            return false;
        }
        // Check user
        if (connCountByUser.get(ctx.getQualifiedUser()) == null) {
            connCountByUser.put(ctx.getQualifiedUser(), new AtomicInteger(0));
        }
        int currentConns = connCountByUser.get(ctx.getQualifiedUser()).get();
        long currentMaxConns = ctx.getGlobalStateMgr().getAuthenticationMgr().getMaxConn(ctx.getQualifiedUser());
        if (currentConns >= currentMaxConns) {
            return false;
        }
        numberConnection.incrementAndGet();
        connCountByUser.get(ctx.getQualifiedUser()).incrementAndGet();
        connectionMap.put((long) ctx.getConnectionId(), ctx);
        return true;
    }

    public void unregisterConnection(ConnectContext ctx) {
        if (connectionMap.remove((long) ctx.getConnectionId()) != null) {
            numberConnection.decrementAndGet();
            AtomicInteger conns = connCountByUser.get(ctx.getQualifiedUser());
            if (conns != null) {
                conns.decrementAndGet();
            }
        }
    }

    public ConnectContext getContext(long connectionId) {
        return connectionMap.get(connectionId);
    }

    public int getConnectionNum() {
        return numberConnection.get();
    }

    private List<ConnectContext.ThreadInfo> getAllConnThreadInfoByUser(ConnectContext connectContext, String user) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();
        ConnectContext currContext = connectContext == null ? ConnectContext.get() : connectContext;

        for (ConnectContext ctx : connectionMap.values()) {
            if (!ctx.getQualifiedUser().equals(user)) {
                try {
                    PrivilegeChecker.checkSystemAction(currContext.getCurrentUserIdentity(),
                            currContext.getCurrentRoleIds(), PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    continue;
                }
            }

            infos.add(ctx.toThreadInfo());
        }
        return infos;
    }

    public List<ConnectContext.ThreadInfo> listConnection(String user) {
        return getAllConnThreadInfoByUser(null, user);
    }

    public List<ConnectContext.ThreadInfo> listConnection(ConnectContext context, String user) {
        return getAllConnThreadInfoByUser(context, user);
    }

    private class LoopHandler implements Runnable {
        ConnectContext context;

        LoopHandler(ConnectContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            try {
                // Set thread local info
                context.setThreadLocalInfo();
                context.setConnectScheduler(ConnectScheduler.this);
                // authenticate check failed.
                MysqlProto.NegotiateResult result = null;
                try {
                    result = MysqlProto.negotiate(context);
                    if (!result.isSuccess()) {
                        return;
                    }

                    if (registerConnection(context)) {
                        MysqlProto.sendResponsePacket(context);
                    } else {
                        context.getState().setError("Reach limit of connections");
                        MysqlProto.sendResponsePacket(context);
                        return;
                    }
                } finally {
                    LogUtil.logConnectionInfoToAuditLogAndQueryQueue(context,
                            result == null ? null : result.getAuthPacket());
                }

                context.setStartTime();
                ConnectProcessor processor = new ConnectProcessor(context);
                processor.loop();
            } catch (Exception e) {
                // for unauthorized access such lvs probe request, may cause exception, just log it in debug level
                if (context.getCurrentUserIdentity() != null) {
                    LOG.warn("connect processor exception because ", e);
                } else {
                    LOG.debug("connect processor exception because ", e);
                }
            } finally {
                unregisterConnection(context);
                context.cleanup();
            }
        }
    }
}
