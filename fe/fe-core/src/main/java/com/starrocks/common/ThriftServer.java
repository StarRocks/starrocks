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

package com.starrocks.common;

import com.google.common.collect.Sets;
import com.starrocks.common.util.NetUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

public class ThriftServer {
    private static final Logger LOG = LogManager.getLogger(ThriftServer.class);
    private int port;
    private TProcessor processor;
    private SRTThreadPoolServer server;
    private Thread serverThread;
    private Set<TNetworkAddress> connects;
    private static ThreadPoolExecutor executor;
    // Read by the acceptor-stall gauge. MetricRepo.init() registers that gauge once per process,
    // during GlobalStateMgr role transfer, which runs inside waitForReady() before StarRocksFEServer
    // constructs the thrift server -- so there is no ThriftServer for the gauge to hold a reference to
    // at registration time. It resolves this handle on each scrape instead, and reads 0 while the
    // handle is null (before start, after stop).
    private static volatile SRTThreadPoolServer activeServer;

    public ThriftServer(int port, TProcessor processor) {
        this.port = port;
        this.processor = processor;
        this.connects = Sets.newConcurrentHashSet();
    }

    private void createThreadPoolServer() throws TTransportException {
        TServerSocket.ServerSocketTransportArgs socketTransportArgs = new TServerSocket.ServerSocketTransportArgs()
                .bindAddr(NetUtils.getSockAddrBasedOnCurrIpVersion(port))
                .clientTimeout(Config.thrift_client_timeout_ms)
                .backlog(Config.thrift_backlog_num);

        TBinaryProtocol.Factory factory =
                new TBinaryProtocol.Factory(Config.thrift_rpc_strict_mode, true, Config.thrift_rpc_max_body_size, -1);
        SRTThreadPoolServer.Args serverArgs =
                new SRTThreadPoolServer.Args(new TServerSocket(socketTransportArgs)).protocolFactory(
                        factory).processor(processor);
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolManager
                .newDaemonFixedThreadPoolWithAbortPolicy(Config.thrift_server_max_worker_threads,
                        Config.thrift_server_queue_size,
                        "thrift-server-pool", true);
        // allow core thread time out so that the thread can be released
        // if not used for ThreadPoolManager.KEEP_ALIVE_TIME
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        serverArgs.executorService(threadPoolExecutor);
        executor = threadPoolExecutor;
        server = new SRTThreadPoolServer(serverArgs);
        activeServer = server;

        GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(() -> {
            ThreadPoolManager.setFixedThreadPoolSize(threadPoolExecutor, Config.thrift_server_max_worker_threads);
        });
    }

    public void start() throws IOException {
        try {
            createThreadPoolServer();
        } catch (TTransportException ex) {
            LOG.warn("create thrift server failed.", ex);
            throw new IOException("create thrift server failed.", ex);
        }

        ThriftServerEventProcessor eventProcessor = new ThriftServerEventProcessor(this);
        server.setServerEventHandler(eventProcessor);

        serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        });
        serverThread.setName("thrift-server-accept");
        serverThread.setDaemon(true);
        serverThread.start();
    }

    public void stop() {
        if (server != null) {
            server.stop();
            // The accept loop has exited, so its heartbeat is frozen. Drop the handle instead of
            // letting the stall gauge climb forever against a server that is no longer accepting.
            if (activeServer == server) {
                activeServer = null;
            }
        }
    }

    public void join() throws InterruptedException {
        if (server != null && server.isServing()) {
            stop();
        }
        serverThread.join();
    }

    public void addConnect(TNetworkAddress clientAddress) {
        connects.add(clientAddress);
    }

    public void removeConnect(TNetworkAddress clientAddress) {
        connects.remove(clientAddress);
    }

    public static ThreadPoolExecutor getExecutor() {
        return executor;
    }

    public static long getAcceptorStallTimeMs() {
        SRTThreadPoolServer currentServer = activeServer;
        return currentServer == null ? 0 : currentServer.getAcceptorStallTimeMs();
    }
}
