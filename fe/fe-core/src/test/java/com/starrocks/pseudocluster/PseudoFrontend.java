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

package com.starrocks.pseudocluster;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.Log4jConfig;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.StateChangeExecutor;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalFactory;
import com.starrocks.qe.QeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendOptions;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.thrift.FrontendService;
import com.starrocks.utframe.MockJournal;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PseudoFrontend {
    public static final String FE_PROCESS = "fe";

    // the running dir of this mocked frontend.
    // log/ starrocks-meta/ and conf/ dirs will be created under this dir.
    private String runningDir;

    private boolean fakeJournal = true;

    // the min set of fe.conf.
    private static final Map<String, String> MIN_FE_CONF;

    private FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());

    static {
        MIN_FE_CONF = Maps.newHashMap();
        MIN_FE_CONF.put("sys_log_level", "INFO");
        MIN_FE_CONF.put("http_port", "8030");
        MIN_FE_CONF.put("rpc_port", "9020");
        MIN_FE_CONF.put("query_port", "9030");
        MIN_FE_CONF.put("edit_log_port", "9010");
        MIN_FE_CONF.put("priority_networks", "127.0.0.1/24");
        MIN_FE_CONF.put("frontend_address", "127.0.0.1");

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.start();
    }

    private boolean isInit = false;

    private final Lock initLock = new ReentrantLock();

    // init the fe process. This must be called before starting the frontend process.
    // 1. check if all neccessary environment variables are set.
    // 2. clear and create 3 dirs: runningDir/log/, runningDir/starrocks-meta/, runningDir/conf/
    // 3. init fe.conf
    //      The content of "fe.conf" is a merge set of input `feConf` and MIN_FE_CONF
    public void init(boolean fakeJournal, String runningDir, Map<String, String> feConf)
            throws EnvVarNotSetException, IOException {
        initLock.lock();
        if (isInit) {
            return;
        }

        if (Strings.isNullOrEmpty(runningDir)) {
            System.err.println("running dir is not set for mocked frontend");
            throw new EnvVarNotSetException("running dir is not set for mocked frontend");
        }

        this.runningDir = runningDir;
        this.fakeJournal = fakeJournal;
        System.out.println("pseudo frontend running in dir: " + new File(this.runningDir).getAbsolutePath());

        // root running dir
        createAndClearDir(this.runningDir);
        // clear and create meta dir
        createAndClearDir(runningDir + "/starrocks-meta/");
        // clear and create conf dir
        createAndClearDir(runningDir + "/conf/");
        // init fe.conf
        initFeConf(runningDir + "/conf/", feConf);

        isInit = true;
        initLock.unlock();
    }

    private void initFeConf(String confDir, Map<String, String> feConf) throws IOException {
        Map<String, String> finalFeConf = Maps.newHashMap(MIN_FE_CONF);
        // these 2 configs depends on running dir, so set them here.
        finalFeConf.put("LOG_DIR", this.runningDir);
        finalFeConf.put("sys_log_dir", this.runningDir);
        finalFeConf.put("audit_log_dir", this.runningDir);
        finalFeConf.put("meta_dir", this.runningDir + "/starrocks-meta");
        finalFeConf.put("tmp_dir", this.runningDir + "/temp_dir");
        // use custom config to add or override default config
        finalFeConf.putAll(feConf);

        PrintableMap<String, String> map = new PrintableMap<>(finalFeConf, "=", false, true, "");
        File confFile = new File(confDir + "fe.conf");
        if (!confFile.exists()) {
            confFile.createNewFile();
        }
        try (PrintWriter printWriter = new PrintWriter(confFile)) {
            printWriter.print(map.toString());
            printWriter.flush();
        }
    }

    public static void createAndClearDir(String dir) throws IOException {
        File localDir = new File(dir);
        if (!localDir.exists()) {
            localDir.mkdirs();
        } else {
            Files.walk(Paths.get(dir)).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            if (!localDir.exists()) {
                localDir.mkdirs();
            }
        }
    }

    public String getRunningDir() {
        return runningDir;
    }

    public FrontendService.Iface getFrontendService() {
        return service;
    }

    private static class FERunnable implements Runnable {
        private final PseudoFrontend frontend;
        private final String[] args;

        public FERunnable(PseudoFrontend frontend, String[] args) {
            this.frontend = frontend;
            this.args = args;
        }

        @Override
        public void run() {
            if (Strings.isNullOrEmpty(frontend.getRunningDir())) {
                System.err.println("env STARROCKS_HOME is not set.");
                return;
            }

            try {
                // init config
                new Config().init(frontend.getRunningDir() + "/conf/fe.conf");
                Config.statistic_collect_query_timeout = 60;

                Log4jConfig.initLogging();

                // set dns cache ttl
                java.security.Security.setProperty("networkaddress.cache.ttl", "60");

                FrontendOptions.init(new String[0]);
                ExecuteEnv.setup();

                if (frontend.fakeJournal) {
                    new MockUp<JournalFactory>() {
                        @Mock
                        public Journal create(String name) throws JournalException {
                            GlobalStateMgr.getCurrentState().setHaProtocol(new MockJournal.MockProtocol());
                            return new MockJournal();
                        }
                    };
                }

                GlobalStateMgr.getCurrentState().initialize(args);
                GlobalStateMgr.getCurrentState().setStatisticStorage(new EmptyStatisticStorage());
                StateChangeExecutor.getInstance().registerStateChangeExecution(
                        GlobalStateMgr.getCurrentState().getStateChangeExecution());
                StateChangeExecutor.getInstance().start();
                StateChangeExecutor.getInstance().notifyNewFETypeTransfer(FrontendNodeType.LEADER);
                FrontendOptions.saveStartType();

                GlobalStateMgr.getCurrentState().waitForReady();

                QeService qeService = new QeService(Config.query_port, ExecuteEnv.getInstance().getScheduler());
                qeService.start();

                ThreadPoolManager.registerAllThreadPoolMetric();

                while (true) {
                    Thread.sleep(2000);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    // must call init() before start.
    public void start(String[] args) throws FeStartException, NotInitException {
        initLock.lock();
        if (!isInit) {
            throw new NotInitException("fe process is not initialized");
        }
        initLock.unlock();

        Thread feThread = new Thread(new FERunnable(this, args), FE_PROCESS);
        feThread.start();
        waitForCatalogReady();
        System.out.println("Fe process is started");
    }

    private void waitForCatalogReady() throws FeStartException {
        int tryCount = 0;
        while (!GlobalStateMgr.getCurrentState().isReady() && tryCount < 600) {
            try {
                tryCount++;
                Thread.sleep(1000);
                System.out.println("globalStateMgr is not ready, wait for 1 second");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (!GlobalStateMgr.getCurrentState().isReady()) {
            System.err.println("globalStateMgr is not ready");
            throw new FeStartException("fe start failed");
        }
    }

    public static class FeStartException extends Exception {
        public FeStartException(String msg) {
            super(msg);
        }
    }

    public static class EnvVarNotSetException extends Exception {
        public EnvVarNotSetException(String msg) {
            super(msg);
        }
    }

    public static class NotInitException extends Exception {
        public NotInitException(String msg) {
            super(msg);
        }
    }

}
