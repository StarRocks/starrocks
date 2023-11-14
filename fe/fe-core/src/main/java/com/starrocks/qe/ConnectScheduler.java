// This file is made available under Elastic License 2.0.
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
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.nio.NConnectContext;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.server.GlobalStateMgr;
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
    private final Map<String, AtomicInteger> connByUser = Maps.newConcurrentMap();
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
        // no necessary for nio.
        if (context instanceof NConnectContext) {
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
        if (connByUser.get(ctx.getQualifiedUser()) == null) {
            connByUser.put(ctx.getQualifiedUser(), new AtomicInteger(0));
        }
        int conns = connByUser.get(ctx.getQualifiedUser()).get();
        long currentConns;
        if (ctx.getGlobalStateMgr().isUsingNewPrivilege()) {
            currentConns = ctx.getGlobalStateMgr().getAuthenticationManager().getMaxConn(ctx.getQualifiedUser());
        } else {
            currentConns = ctx.getGlobalStateMgr().getAuth().getMaxConn(ctx.getQualifiedUser());
        }
        if (conns >= currentConns) {
            return false;
        }
        numberConnection.incrementAndGet();
        connByUser.get(ctx.getQualifiedUser()).incrementAndGet();
        connectionMap.put((long) ctx.getConnectionId(), ctx);
        return true;
    }

    public void unregisterConnection(ConnectContext ctx) {
        if (connectionMap.remove((long) ctx.getConnectionId()) != null) {
            numberConnection.decrementAndGet();
            AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
            if (conns != null) {
                conns.decrementAndGet();
            }
            LOG.info("Connection closed. remote={}, connectionId={}",
                    ctx.getMysqlChannel().getRemoteHostPortString(), ctx.getConnectionId());
        }
    }

    public ConnectContext getContext(long connectionId) {
        return connectionMap.get(connectionId);
    }

    public int getConnectionNum() {
        return numberConnection.get();
    }

    public List<ConnectContext.ThreadInfo> listConnection(String user) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();

        for (ConnectContext ctx : connectionMap.values()) {
            // Check auth
            if (!ctx.getQualifiedUser().equals(user) &&
                    !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                            PrivPredicate.GRANT)) {
                continue;
            }

            infos.add(ctx.toThreadInfo());
        }
        return infos;
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
                if (!MysqlProto.negotiate(context)) {
                    return;
                }

                if (registerConnection(context)) {
                    MysqlProto.sendResponsePacket(context);
                } else {
                    context.getState().setError("Reach limit of connections");
                    MysqlProto.sendResponsePacket(context);
                    return;
                }

                context.setStartTime();
                ConnectProcessor processor = new ConnectProcessor(context);
                processor.loop();
            } catch (Exception e) {
                // for unauthrorized access such lvs probe request, may cause exception, just log it in debug level
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
