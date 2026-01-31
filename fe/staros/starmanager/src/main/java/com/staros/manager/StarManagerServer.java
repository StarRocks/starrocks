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


package com.staros.manager;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.staros.journal.DummyJournalSystem;
import com.staros.journal.JournalSystem;
import com.staros.provisioner.StarProvisionServer;
import com.staros.util.Config;
import com.staros.util.LogConfig;
import com.staros.util.Utils;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class StarManagerServer {
    private static final Logger LOG = LogManager.getLogger(StarManagerServer.class);

    private StarManager manager;
    private StarManagerService service;
    private HttpService httpService;
    private Server server;
    private AtomicBoolean started; // FOR TEST
    // the executor for grpc server builder
    private Executor executor;
    // our own managed executor service, responsible for a clean shut down
    private ExecutorService executorService;

    // FOR TEST
    public StarManagerServer() {
        this(new DummyJournalSystem());
    }

    public StarManagerServer(JournalSystem journalSystem) {
        this.manager = new StarManager(journalSystem);
        this.service = new StarManagerService(manager);
        this.httpService = new HttpService(manager);
        this.started = new AtomicBoolean(false);
    }

    public void start(int port) throws IOException {
        // detect local address
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        start(localAddress, port, null);
    }

    /**
     * @param address  Starmgr local listen address, used to let follower known how to contact the leader
     * @param port     starmgr local listen address
     * @param executor Optional, if not provided, starmgr will create its own executor. If provided,
     *                 it will be the caller's responsibility to shut down the executor.
     * @throws IOException
     */
    public void start(String address, int port, Executor executor) throws IOException {
        LOG.info("starting star manager ({}) on port {} ...", address, port);

        ServerBuilder<?> builder = ServerBuilder.forPort(port);
        builder.addService(service);
        if (Config.ENABLE_BUILTIN_RESOURCE_PROVISIONER_FOR_TEST) {
            // serve StarProvisionServer related service as well
            StarProvisionServer provisionServer = new StarProvisionServer();
            StarProvisionServer.getServices(provisionServer)
                    .forEach(builder::addService);
        }
        builder.maxInboundMessageSize(Config.GRPC_CHANNEL_MAX_MESSAGE_SIZE);
        if (executor != null) {
            // use the executor but don't take the ownership
            this.executor = executor;
        } else {
            LOG.info("create default executor for grpc server ...");
            executorService = createDefaultExecutor();
            this.executor = executorService;
        }
        builder.executor(this.executor);

        server = builder.build();
        server.start();
        started.set(true);
        // tell starmgr its address and port info
        manager.setListenAddressInfo(address, server.getPort());
        if (port == 0) { // auto-detect available port
            LOG.info("started star manager ({}) on port {} ...", address, server.getPort());
        }
    }

    public void shutdown() {
        LOG.info("star manager shutting down...");
        server.shutdown();
        getStarManager().stopBackgroundThreads();
        if (this.executorService != null) {
            Utils.shutdownExecutorService(executorService);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    // FOR TEST ONLY
    public int getServerPort() {
        return server.getPort();
    }

    // FOR TEST
    public void blockUntilStart() {
        int times = 1;
        while (!started.get()) {
            if (times++ >= 100) {
                System.out.println("wait star manager server to start for 5 seconds, abort test!");
                System.exit(-1);
            }
            try {
                Thread.sleep(50); // 50ms
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }

    public StarManager getStarManager() {
        return manager;
    }

    public HttpService getHttpService() {
        return httpService;
    }

    private ExecutorService createDefaultExecutor() {
        final String name = "starmgr-grpc-default-executor";
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(name + "-%d")
                .build();
        return Executors.newCachedThreadPool(factory);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String confFile = "./starmgr.conf";
        // process config file
        new Config().init(confFile);

        // start log4j based on config
        LogConfig.initLogging();

        // start server
        StarManagerServer server = new StarManagerServer();
        server.getStarManager().becomeLeader();
        server.start(Config.STARMGR_RPC_PORT);

        server.blockUntilShutdown();
    }
}
