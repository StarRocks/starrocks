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
import com.starrocks.ha.StateChangeExecutor;
import com.starrocks.http.HttpServer;
import com.starrocks.journal.Journal;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.BDBTool;
import com.starrocks.journal.bdbje.BDBToolOptions;
import com.starrocks.lake.snapshot.RestoreClusterSnapshotMgr;
import com.starrocks.leader.MetaHelper;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.qe.QeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendOptions;
import com.starrocks.service.FrontendThriftServer;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlService;
import com.starrocks.staros.StarMgrServer;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;

public class StarRocksFE {
    private static final Logger LOG = LogManager.getLogger(StarRocksFE.class);

    public static final String STARROCKS_HOME_DIR = System.getenv("STARROCKS_HOME");
    public static final String PID_DIR = System.getenv("PID_DIR");

    public static volatile boolean stopped = false;

    public static void main(String[] args) {
        start(STARROCKS_HOME_DIR, PID_DIR, args);
    }


    // entrance for starrocks frontend
    public static void start(String starRocksDir, String pidDir, String[] args) {
        if (Strings.isNullOrEmpty(starRocksDir)) {
            System.err.println("env STARROCKS_HOME is not set.");
            return;
        }

        if (Strings.isNullOrEmpty(pidDir)) {
            System.err.println("env PID_DIR is not set.");
            return;
        }

        CommandLineOptions cmdLineOpts = parseArgs(args);

        try {
            // pid file
            if (!createAndLockPidFile(pidDir + "/fe.pid")) {
                throw new IOException("pid file is already locked.");
            }

            // init config
            new Config().init(starRocksDir + "/conf/fe.conf");

            // check command line options
            // NOTE: do it before init log4jConfig to avoid unnecessary stdout messages
            checkCommandLineOptions(cmdLineOpts);

            Log4jConfig.initLogging();

            // set dns cache ttl
            java.security.Security.setProperty("networkaddress.cache.ttl", "60");

            RestoreClusterSnapshotMgr.init(starRocksDir + "/conf/cluster_snapshot.yaml", args);

            // check meta dir
            MetaHelper.checkMetaDir();

            LOG.info("StarRocks FE starting, version: {}-{}", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH);

            FrontendOptions.init(args);
            ExecuteEnv.setup();

            // init globalStateMgr
            GlobalStateMgr.getCurrentState().initialize(args);

            if (RunMode.isSharedDataMode()) {
                Journal journal = GlobalStateMgr.getCurrentState().getJournal();
                if (journal instanceof BDBJEJournal) {
                    BDBEnvironment bdbEnvironment = ((BDBJEJournal) journal).getBdbEnvironment();
                    StarMgrServer.getCurrentState().initialize(bdbEnvironment,
                            GlobalStateMgr.getCurrentState().getImageDir());
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
            // 3. HttpServer for HTTP Server
            // 4. ArrowFlightSqlService for Arrow Flight Sql Server
            QeService qeService = new QeService(Config.query_port, Config.mysql_service_nio_enabled,
                    ExecuteEnv.getInstance().getScheduler());
            FrontendThriftServer frontendThriftServer = new FrontendThriftServer(Config.rpc_port);
            HttpServer httpServer = new HttpServer(Config.http_port);
            ArrowFlightSqlService arrowFlightSqlService = new ArrowFlightSqlService(Config.arrow_flight_port);

            httpServer.setup();

            frontendThriftServer.start();
            httpServer.start();
            qeService.start();
            arrowFlightSqlService.start();

            ThreadPoolManager.registerAllThreadPoolMetric();

            addShutdownHook();

            RestoreClusterSnapshotMgr.finishRestoring();

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

    /*
     * -v --version
     *      Print the version of StarRocks Frontend
     * -h --helper
     *      Specify the helper node when joining a bdb je replication group
     * -b --bdb
     *      Run bdbje debug tools
     *
     *      -l --listdb
     *          List all database names in bdbje
     *      -d --db
     *          Specify a database in bdbje
     *
     *          -s --stat
     *              Print statistic of a database, including count, first key, last key
     *          -f --from
     *              Specify the start scan key
     *          -t --to
     *              Specify the end scan key
     *          -m --metaversion
     *              Specify the meta version to decode log value, separated by ',', first
     *              is community meta version, second is StarRocks meta version
     *
     */
    private static CommandLineOptions parseArgs(String[] args) {
        CommandLineParser commandLineParser = new BasicParser();
        Options options = new Options();
        options.addOption("ht", "host_type", false, "Specify fe start use ip or fqdn");
        options.addOption("rs", "cluster_snapshot", false, "Specify fe start to restore from a cluster snapshot");
        options.addOption("v", "version", false, "Print the version of StarRocks Frontend");
        options.addOption("h", "helper", true, "Specify the helper node when joining a bdb je replication group");
        options.addOption("b", "bdb", false, "Run bdbje debug tools");
        options.addOption("l", "listdb", false, "Print the list of databases in bdbje");
        options.addOption("d", "db", true, "Specify a database in bdbje");
        options.addOption("s", "stat", false, "Print statistic of a database, including count, first key, last key");
        options.addOption("f", "from", true, "Specify the start scan key");
        options.addOption("t", "to", true, "Specify the end scan key");
        options.addOption("m", "metaversion", true,
                "Specify the meta version to decode log value, separated by ',', first is community meta" +
                        " version, second is StarRocks meta version");

        CommandLine cmd = null;
        try {
            cmd = commandLineParser.parse(options, args);
        } catch (final ParseException e) {
            LOG.error(e.getMessage(), e);
            System.err.println("Failed to parse command line. exit now");
            System.exit(-1);
        }

        // version
        if (cmd.hasOption('v') || cmd.hasOption("version")) {
            return new CommandLineOptions(true, null);
        } else if (cmd.hasOption('b') || cmd.hasOption("bdb")) {
            if (cmd.hasOption('l') || cmd.hasOption("listdb")) {
                // list bdb je databases
                BDBToolOptions bdbOpts = new BDBToolOptions(true, "", false, "", "", 0, 0);
                return new CommandLineOptions(false, bdbOpts);
            } else if (cmd.hasOption('d') || cmd.hasOption("db")) {
                // specify a database
                String dbName = cmd.getOptionValue("db");
                if (Strings.isNullOrEmpty(dbName)) {
                    System.err.println("BDBJE database name is missing");
                    System.exit(-1);
                }

                if (cmd.hasOption('s') || cmd.hasOption("stat")) {
                    BDBToolOptions bdbOpts = new BDBToolOptions(false, dbName, true, "", "", 0, 0);
                    return new CommandLineOptions(false, bdbOpts);
                } else {
                    String fromKey = "";
                    String endKey = "";
                    int metaVersion = 0;
                    int starrocksMetaVersion = 0;
                    if (cmd.hasOption('f') || cmd.hasOption("from")) {
                        fromKey = cmd.getOptionValue("from");
                        if (Strings.isNullOrEmpty(fromKey)) {
                            System.err.println("from key is missing");
                            System.exit(-1);
                        }
                    }
                    if (cmd.hasOption('t') || cmd.hasOption("to")) {
                        endKey = cmd.getOptionValue("to");
                        if (Strings.isNullOrEmpty(endKey)) {
                            System.err.println("end key is missing");
                            System.exit(-1);
                        }
                    }
                    if (cmd.hasOption('m') || cmd.hasOption("metaversion")) {
                        try {
                            String version = cmd.getOptionValue("metaversion");
                            String[] vs = version.split(",");
                            if (vs.length != 2) {
                                System.err.println("invalid meta version format");
                                System.exit(-1);
                            }
                            metaVersion = Integer.parseInt(vs[0]);
                            starrocksMetaVersion = Integer.parseInt(vs[1]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid meta version format");
                            System.exit(-1);
                        }
                    }

                    BDBToolOptions bdbOpts =
                            new BDBToolOptions(false, dbName, false, fromKey, endKey, metaVersion,
                                    starrocksMetaVersion);
                    return new CommandLineOptions(false, bdbOpts);
                }
            } else {
                System.err.println("Invalid options when running bdb je tools");
                System.exit(-1);
            }
        } else if (cmd.hasOption('h') || cmd.hasOption("helper")) {
            String helperNode = cmd.getOptionValue("helper");
            if (Strings.isNullOrEmpty(helperNode)) {
                System.err.println("Missing helper node");
                System.exit(-1);
            }
        }

        // helper node is null, means no helper node is specified
        return new CommandLineOptions(false, null);
    }

    private static void checkCommandLineOptions(CommandLineOptions cmdLineOpts) {
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
        } else if (cmdLineOpts.runBdbTools()) {

            BDBTool bdbTool = new BDBTool(BDBEnvironment.getBdbDir(), cmdLineOpts.getBdbToolOpts());
            if (bdbTool.run()) {
                System.exit(0);
            } else {
                System.exit(-1);
            }
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

    // NOTE: To avoid dead lock
    //      1. never call System.exit in shutdownHook
    //      2. shutdownHook cannot have lock conflict with the function calling System.exit
    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("start to execute shutdown hook");
            try {
                Thread t = new Thread(() -> {
                    try {
                        Journal journal = GlobalStateMgr.getCurrentState().getJournal();
                        if (journal instanceof BDBJEJournal) {
                            BDBEnvironment bdbEnvironment = ((BDBJEJournal) journal).getBdbEnvironment();
                            if (bdbEnvironment != null) {
                                bdbEnvironment.flushVLSNMapping();
                            }
                        }
                    } catch (Throwable e) {
                        LOG.warn("flush vlsn mapping failed", e);
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
