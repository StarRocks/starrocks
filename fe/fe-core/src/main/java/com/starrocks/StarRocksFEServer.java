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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/StarRocksFE.java

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

package com.starrocks;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.starrocks.common.CommandLineOptions;
import com.starrocks.common.Config;
import com.starrocks.common.Log4jConfig;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.Version;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.Util;
import com.starrocks.failpoint.FailPoint;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.StateChangeExecutor;
import com.starrocks.http.HttpServer;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.BootstrapFinishAction;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalWriter;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.BDBTool;
import com.starrocks.lake.snapshot.RestoreClusterSnapshotMgr;
import com.starrocks.leader.MetaHelper;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.qe.ProxyContextManager;
import com.starrocks.qe.QeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.server.RunMode;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendOptions;
import com.starrocks.service.FrontendThriftServer;
import com.starrocks.service.GroovyUDSServer;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlService;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Frontend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Signal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.channels.FileLock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class StarRocksFEServer {
    private static final Logger LOG = LogManager.getLogger(StarRocksFEServer.class);

    public static volatile boolean stopped = false;

    // entrance for starrocks frontend
    public static void start(CommandLineOptions cmdLineOpts) {
        String starRocksDir = Config.STARROCKS_HOME_DIR;
        String pidDir = System.getenv("PID_DIR");

        if (Strings.isNullOrEmpty(starRocksDir)) {
            System.err.println("env STARROCKS_HOME is not set.");
            return;
        }

        if (Strings.isNullOrEmpty(pidDir)) {
            System.err.println("env PID_DIR is not set.");
            return;
        }

        try {
            // pid file
            if (!createAndLockPidFile(pidDir + "/fe.pid")) {
                throw new IOException("pid file is already locked.");
            }

            // init config
            new Config().init(starRocksDir + "/conf/fe.conf");

            // run command line options
            // NOTE: do it before init log4jConfig to avoid unnecessary stdout messages
            runCommandLineOptions(cmdLineOpts);

            Log4jConfig.initLogging();
            // We have already output the caffine's error message to Log4j2.
            // we turn off the java.util.logging.Logger of caffine to reduce the output log of the console
            java.util.logging.Logger.getLogger("com.github.benmanes.caffeine").setLevel(java.util.logging.Level.OFF);

            // set dns cache ttl
            java.security.Security.setProperty("networkaddress.cache.ttl", "60");

            RestoreClusterSnapshotMgr.init(starRocksDir + "/conf/cluster_snapshot.yaml", cmdLineOpts.isStartFromSnapshot());

            // check meta dir
            MetaHelper.checkMetaDir();

            LOG.info("StarRocks FE starting, version: {}-{}", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH);

            FrontendOptions.init(cmdLineOpts.getHostType());
            ExecuteEnv.setup();

            // init globalStateMgr
            GlobalStateMgr.getCurrentState().initialize(cmdLineOpts.getHelpers());

            if (RunMode.isSharedDataMode()) {
                Journal journal = GlobalStateMgr.getCurrentState().getJournal();
                if (journal instanceof BDBJEJournal) {
                    BDBEnvironment bdbEnvironment = ((BDBJEJournal) journal).getBdbEnvironment();
                    StarMgrServer.getCurrentState().initialize(bdbEnvironment, GlobalStateMgr.getImageDirPath());
                } else {
                    LOG.error("journal type should be BDBJE for star mgr!");
                    System.exit(-1);
                }

                StateChangeExecutor.getInstance().registerStateChangeExecution(
                        StarMgrServer.getCurrentState().getStateChangeExecution());
            }

            StateChangeExecutor.getInstance().registerStateChangeExecution(
                    GlobalStateMgr.getCurrentState().getStateChangeExecution());
            // start state change executor
            StateChangeExecutor.getInstance().start();

            // wait globalStateMgr to be ready
            GlobalStateMgr.getCurrentState().waitForReady();

            FrontendOptions.saveStartType();

            CoordinatorMonitor.getInstance().start();

            // init and start:
            // 1. QeService for MySQL Server
            // 2. FrontendThriftServer for Thrift Server
            // 3. HttpServer for HTTP Server and optionally for HTTPS Server
            // 4. ArrowFlightSqlService for Arrow Flight SQL Server
            QeService qeService = new QeService(Config.query_port, ExecuteEnv.getInstance().getScheduler());
            FrontendThriftServer frontendThriftServer = new FrontendThriftServer(Config.rpc_port);
            HttpServer httpServer = new HttpServer(Config.http_port);
            Optional<HttpServer> httpsServer = Optional.ofNullable(
                    Config.enable_https ? new HttpServer(Config.https_port, true) : null);
            ArrowFlightSqlService arrowFlightSqlService = new ArrowFlightSqlService(Config.arrow_flight_port);
            // Setup HTTP and HTTPS (optional).
            httpServer.setup();
            if (httpsServer.isPresent()) {
                httpsServer.get().setup();
            }
            frontendThriftServer.start();
            // Start HTTP and HTTPS (optional).
            httpServer.start();
            if (httpsServer.isPresent()) {
                httpsServer.get().start();
            }
            qeService.start();
            arrowFlightSqlService.start();

            if (Config.enable_groovy_debug_server) {
                GroovyUDSServer.getInstance().start();
            }

            ThreadPoolManager.registerAllThreadPoolMetric();

            addShutdownHook();

            RestoreClusterSnapshotMgr.finishRestoring();

            handleGracefulExit();

            LOG.info("FE started successfully");

            while (!stopped) {
                Thread.sleep(2000);
            }

        } catch (Throwable e) {
            LOG.error("StarRocksFE start failed", e);
            System.exit(-1);
        }

        System.exit(0);
    }

    private static void handleGracefulExit() {
        // Since the normal exit is using SIGTERM(15),
        // so we have to choose another signal for the graceful exit, use SIGUSR1(10) here.
        Signal.handle(new Signal("USR1"), sig -> {
            Thread t = new Thread(() -> {
                if (canGracefulExit()) {
                    long startTime = System.nanoTime();
                    LOG.info("start to handle graceful exit");
                    GracefulExitFlag.markGracefulExit();

                    // transfer leader if current node is leader
                    try {
                        transferLeader();
                    } catch (Exception e) {
                        LOG.warn("handle graceful exit failed", e);
                        System.exit(-1);
                    }

                    // Wait for queries to complete
                    try {
                        waitForDraining(startTime);
                    } catch (Exception e) {
                        LOG.warn("handle graceful exit failed", e);
                        System.exit(-1);
                    }

                    LOG.info("handle graceful exit successfully");
                    System.exit(0);
                } else {
                    LOG.info("The current number of FEs that are alive cannot match graceful exit condition, " +
                            "and can only exit forcefully.");
                    System.exit(-1);
                }
            }, "graceful-exit");
            t.start();

            try {
                t.join(Config.max_graceful_exit_time_second * 1000);
            } catch (InterruptedException e) {
                LOG.warn("An exception thrown while waiting for graceful-exit thread to complete", e);
            }
            if (t.isAlive()) {
                LOG.warn("graceful exit timeout");
                System.exit(-1);
            } else {
                System.exit(0);
            }
        });
    }

    private static void transferLeader() throws InterruptedException {
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            LOG.info("start to transfer leader");
            JournalWriter journalWriter = GlobalStateMgr.getCurrentState().getJournalWriter();
            // stop journal writer
            journalWriter.stopAndWait();
            Journal journal = GlobalStateMgr.getCurrentState().getJournal();

            // transfer leader
            if (journal instanceof BDBJEJournal) {
                BDBEnvironment bdbEnvironment = ((BDBJEJournal) journal).getBdbEnvironment();
                if (bdbEnvironment != null) {
                    // close bdb env, leader election will be triggered
                    bdbEnvironment.close();
                    // wait for new leader
                    while (true) {
                        try {
                            InetSocketAddress address = GlobalStateMgr.getCurrentState().getHaProtocol().getLeader();
                            // wait for new leader to be ready
                            if (isNewLeaderReady(address.getHostString())) {
                                LOG.info("leader is transferred to {}", address);
                                break;
                            }
                        } catch (Exception e) {
                            Thread.sleep(300L);
                        }
                    }
                }

                GlobalStateMgr.getCurrentState().markLeaderTransferred();
            }
        }
    }

    private static boolean isNewLeaderReady(String leaderHost) {
        String accessibleHostPort = NetUtils.getHostPortInAccessibleFormat(leaderHost, Config.http_port);
        String url = "http://" + accessibleHostPort
                + "/api/bootstrap"
                + "?cluster_id=" + GlobalStateMgr.getCurrentState().getNodeMgr().getClusterId()
                + "&token=" + GlobalStateMgr.getCurrentState().getNodeMgr().getToken();
        try {
            String resultStr = Util.getResultForUrl(url, null,
                    Config.heartbeat_timeout_second * 1000,
                    Config.heartbeat_timeout_second * 1000);
            BootstrapFinishAction.BootstrapResult result = BootstrapFinishAction.BootstrapResult.fromJson(resultStr);
            return result.getStatus() == ActionStatus.OK;
        } catch (Exception e) {
            LOG.warn("call leader bootstrap api failed", e);
        }
        return false;
    }

    private static void waitForDraining(long startTimeNano) throws InterruptedException {
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        final long waitInterval = 1000L;
        while (true) {
            connectScheduler.closeAllIdleConnection();
            int totalConns = connectScheduler.getTotalConnCount()
                    + ProxyContextManager.getInstance().getTotalConnCount();
            if (totalConns > 0) {
                LOG.info("waiting for {} connections to drain", totalConns);
            } else if (System.nanoTime() - startTimeNano
                    > TimeUnit.SECONDS.toNanos(Config.min_graceful_exit_time_second)) {
                break;
            }
            Thread.sleep(waitInterval);
        }
    }

    private static boolean canGracefulExit() {
        List<Frontend> frontends = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(FrontendNodeType.FOLLOWER);
        long aliveCnt = frontends.stream().filter(Frontend::isAlive).count();
        // We need to ensure that after the node is shut down, there are still enough followers alive
        // so that the traffic can be switched to other normal nodes.
        return (aliveCnt - 1) >= (frontends.size()) / 2 + 1;
    }

    private static void runCommandLineOptions(CommandLineOptions cmdLineOpts) {
        if (cmdLineOpts == null) {
            System.err.println("invalid command line options");
            System.exit(-1);
        }
        if (cmdLineOpts.isVersion()) {
            System.out.println("Build version: " + Version.STARROCKS_VERSION);
            System.out.println("Commit hash: " + Version.STARROCKS_COMMIT_HASH);
            System.out.println("Build type: " + Version.STARROCKS_BUILD_TYPE);
            System.out.println("Build time: " + Version.STARROCKS_BUILD_TIME);
            System.out.println("Build distributor id: " + Version.STARROCKS_BUILD_DISTRO_ID);
            System.out.println("Build arch: " + Version.STARROCKS_BUILD_ARCH);
            System.out.println("Build user: " + Version.STARROCKS_BUILD_USER + "@" + Version.STARROCKS_BUILD_HOST);
            System.out.println("Java compile version: " + Version.STARROCKS_JAVA_COMPILE_VERSION);
            System.exit(0);
        }
        if (cmdLineOpts.getBdbToolOpts() != null) {

            BDBTool bdbTool = new BDBTool(BDBEnvironment.getBdbDir(), cmdLineOpts.getBdbToolOpts());
            if (bdbTool.run()) {
                System.exit(0);
            } else {
                System.exit(-1);
            }
        }

        if (cmdLineOpts.isEnableFailPoint()) {
            LOG.info("failpoint is enabled");
            FailPoint.enable();
        }

        // go on
    }

    private static boolean createAndLockPidFile(String pidFilePath) {
        File pid = new File(pidFilePath);
        for (int i = 0; i < 3; i++) {
            try (RandomAccessFile file = new RandomAccessFile(pid, "rws")) {
                if (i > 0) {
                    Thread.sleep(10000);
                }
                FileLock lock = file.getChannel().tryLock();
                if (lock == null) {
                    throw new Exception("get pid file lock failed, lock is null");
                }

                pid.deleteOnExit();

                String name = ManagementFactory.getRuntimeMXBean().getName();
                file.setLength(0);
                file.write(name.split("@")[0].getBytes(Charsets.UTF_8));

                return true;
            } catch (Throwable t) {
                LOG.warn("get pid file lock failed, retried: {}", i, t);
            }
        }

        return false;
    }

    // Some cleanup work can be done here.
    // Currently, only one log is printed to distinguish whether it is a normal exit or killed by the operating system.
    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("start to execute shutdown hook");
            try {
                Thread t = new Thread(() -> {
                    try {
                        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
                        connectScheduler.printAllRunningQuery();
                    } catch (Throwable e) {
                        LOG.warn("printing running query failed when fe shut down", e);
                    }
                });

                t.start();

                // it is necessary to set shutdown timeout,
                // because in addition to kill by user, System.exit(-1) will trigger the shutdown hook too,
                // if no timeout and shutdown hook blocked indefinitely, Fe will fall into a catastrophic state.
                t.join(30000);
            } catch (Throwable e) {
                LOG.warn("shut down hook failed", e);
            }
            LOG.info("shutdown hook end");
        }));
    }
}
