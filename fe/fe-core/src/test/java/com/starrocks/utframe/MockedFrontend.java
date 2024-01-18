// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/utframe/MockedFrontend.java

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

package com.starrocks.utframe;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.util.JdkUtils;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.StateChangeExecutor;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalFactory;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendOptions;
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

/*
 * This class is used to start a Frontend process locally, for unit test.
 * This is a singleton class. There can be only one instance of this class globally.
 * Usage:
 *      MockedFrontend mockedFrontend = MockedFrontend.getInstance();
 *      mockedFrontend.init(confMap);
 *      mockedFrontend.start(new String[0]);
 *
 *      ...
 *
 * confMap is a instance of Map<String, String>.
 * Here you can add any FE configuration you want to add. For example:
 *      confMap.put("http_port", "8032");
 *
 * FrontendProcess already contains a minimal set of FE configurations.
 * Any configuration in confMap will form the final fe.conf file with this minimal set.
 *
 * 1 environment variable must be set:
 *      STARROCKS_HOME/
 *
 * The running dir is set when calling init();
 * There will be 3 directories under running dir/:
 *      running dir/conf/
 *      running dir/log/
 *      running dir/starrocks-meta/
 *
 *  All these 3 directories will be cleared first.
 *
 */
public class MockedFrontend {
    public static final String FE_PROCESS = "fe";

    // the running dir of this mocked frontend.
    // log/ starrocks-meta/ and conf/ dirs will be created under this dir.
    private String runningDir;
    // the min set of fe.conf.
    private static final Map<String, String> MIN_FE_CONF;

    static {
        MIN_FE_CONF = Maps.newHashMap();
        MIN_FE_CONF.put("sys_log_level", "INFO");
        MIN_FE_CONF.put("http_port", "8030");
        MIN_FE_CONF.put("rpc_port", "9020");
        MIN_FE_CONF.put("query_port", "9030");
        MIN_FE_CONF.put("edit_log_port", "9010");
        MIN_FE_CONF.put("priority_networks", "127.0.0.1/24");
        MIN_FE_CONF.put("sys_log_verbose_modules", "org");

        // UT don't need log
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.start();
    }

    private static class SingletonHolder {
        private static final MockedFrontend INSTANCE = new MockedFrontend();
    }

    public static MockedFrontend getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private boolean isInit = false;

    private final Lock initLock = new ReentrantLock();

    // init the fe process. This must be called before starting the frontend process.
    // 1. check if all neccessary environment variables are set.
    // 2. clear and create 3 dirs: runningDir/log/, runningDir/starrocks-meta/, runningDir/conf/
    // 3. init fe.conf
    //      The content of "fe.conf" is a merge set of input `feConf` and MIN_FE_CONF
    public void init(String runningDir, Map<String, String> feConf) throws EnvVarNotSetException, IOException {
        initLock.lock();
        if (isInit) {
            return;
        }

        if (Strings.isNullOrEmpty(runningDir)) {
            System.err.println("running dir is not set for mocked frontend");
            throw new EnvVarNotSetException("running dir is not set for mocked frontend");
        }

        this.runningDir = runningDir;
        System.out.println("mocked frontend running in dir: " + this.runningDir);

        // root running dir
        createAndClearDir(this.runningDir);
        // clear and create log dir
        createAndClearDir(runningDir + "/log/");
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
        finalFeConf.put("LOG_DIR", this.runningDir + "/log");
        finalFeConf.put("meta_dir", this.runningDir + "/starrocks-meta");
        finalFeConf.put("sys_log_dir", this.runningDir + "/log");
        finalFeConf.put("audit_log_dir", this.runningDir + "/log");
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

    // clear the specified dir, and create a empty one
    private void createAndClearDir(String dir) throws IOException {
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

    private static class FERunnable implements Runnable {
        private final MockedFrontend frontend;
        private final String[] args;
        private final boolean startBDB;

        public FERunnable(MockedFrontend frontend, boolean startBDB, String[] args) {
            this.frontend = frontend;
            this.startBDB = startBDB;
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

                // check it after Config is initialized, otherwise the config 'check_java_version' won't work.
                if (!JdkUtils.checkJavaVersion()) {
                // throw new IllegalArgumentException("Java version doesn't match");
                }

                // set dns cache ttl
                java.security.Security.setProperty("networkaddress.cache.ttl", "60");

                FrontendOptions.init(new String[0]);
                ExecuteEnv.setup();

                if (!startBDB) {
                    // init globalStateMgr and wait it be ready
                    new MockUp<JournalFactory>() {
                        @Mock
                        public Journal create(String name) throws JournalException {
                            GlobalStateMgr.getCurrentState().setHaProtocol(new MockJournal.MockProtocol());
                            return new MockJournal();
                        }

                    };
                }

                new MockUp<NetUtils>() {
                    @Mock
                    public boolean isPortUsing(String host, int port) {
                        return false;
                    }
                };

                GlobalStateMgr.getCurrentState().initialize(args);
                StateChangeExecutor.getInstance().setMetaContext(
                        GlobalStateMgr.getCurrentState().getMetaContext());
                StateChangeExecutor.getInstance().registerStateChangeExecution(
                        GlobalStateMgr.getCurrentState().getStateChangeExecution());
                StateChangeExecutor.getInstance().start();
                StateChangeExecutor.getInstance().notifyNewFETypeTransfer(FrontendNodeType.LEADER);

                GlobalStateMgr.getCurrentState().waitForReady();

                while (true) {
                    Thread.sleep(2000);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    // must call init() before start.
    public void start(boolean startBDB, String[] args) throws FeStartException, NotInitException, InterruptedException {
        initLock.lock();
        if (!isInit) {
            throw new NotInitException("fe process is not initialized");
        }
        initLock.unlock();
        Thread feThread = new Thread(new FERunnable(this, startBDB, args), FE_PROCESS);
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
