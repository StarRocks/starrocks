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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/cluster/SystemInfoServiceTest.java

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

package com.starrocks.cluster;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SystemInfoServiceTest {

    @Mocked
    private EditLog editLog;
    @Mocked
    private GlobalStateMgr globalStateMgr;
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;
    @Mocked
    private Database db;

    private Analyzer analyzer;

    private String hostPort;

    private long backendId = 10000L;

    @Before
    public void setUp() throws IOException {
        new Expectations() {
            {
                editLog.logAddBackend((Backend) any);
                minTimes = 0;

                editLog.logDropBackend((Backend) any);
                minTimes = 0;

                editLog.logBackendStateChange((Backend) any);
                minTimes = 0;

                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = backendId;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getCluster();
                minTimes = 0;
                result = new Cluster("cluster", 1);

                globalStateMgr.clear();
                minTimes = 0;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                systemInfoService = new SystemInfoService();
                GlobalStateMgr.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                invertedIndex = new TabletInvertedIndex();
                GlobalStateMgr.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        analyzer = new Analyzer(globalStateMgr, new ConnectContext(null));
    }

    public void mkdir(String dirString) {
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void deleteDir(String metaDir) {
        File dir = new File(metaDir);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    public void createHostAndPort(int type) {
        switch (type) {
            case 1:
                // missing ip
                hostPort = "12346";
                break;
            case 2:
                // invalid ip
                hostPort = "asdasd:12345";
                break;
            case 3:
                // invalid port
                hostPort = "10.1.2.3:123467";
                break;
            case 4:
                // normal
                hostPort = "127.0.0.1:12345";
                break;
            default:
                break;
        }
    }

    public void clearAllBackend() {
        GlobalStateMgr.getCurrentSystemInfo().dropAllBackend();
    }

    @Test(expected = AnalysisException.class)
    public void validHostAndPortTest1() throws Exception {
        createHostAndPort(1);
        systemInfoService.validateHostAndPort(hostPort, false);
    }

    @Test(expected = AnalysisException.class)
    public void validHostAndPortTest3() throws Exception {
        createHostAndPort(3);
        systemInfoService.validateHostAndPort(hostPort, false);
    }

    @Test
    public void validHostAndPortTest4() throws Exception {
        createHostAndPort(4);
        systemInfoService.validateHostAndPort(hostPort, false);
    }

    @Test
    public void addBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"));
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext(null));
        try {
            GlobalStateMgr.getCurrentSystemInfo().addBackends(stmt.getHostPortPairs());
        } catch (DdlException e) {
            Assert.fail();
        }

        try {
            GlobalStateMgr.getCurrentSystemInfo().addBackends(stmt.getHostPortPairs());
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("already exists"));
        }

        Assert.assertNotNull(GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId));
        Assert.assertNotNull(GlobalStateMgr.getCurrentSystemInfo().getBackendWithHeartbeatPort("192.168.0.1", 1234));

        Assert.assertTrue(GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber() == 1);
        Assert.assertTrue(GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false).get(0) == backendId);

        Assert.assertTrue(GlobalStateMgr.getCurrentSystemInfo().getBackendReportVersion(backendId) == 0L);

        GlobalStateMgr.getCurrentSystemInfo().updateBackendReportVersion(backendId, 2L, 20000L);
        Assert.assertTrue(GlobalStateMgr.getCurrentSystemInfo().getBackendReportVersion(backendId) == 2L);
    }

    @Test
    public void removeBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"));
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext(null));
        try {
            GlobalStateMgr.getCurrentSystemInfo().addBackends(stmt.getHostPortPairs());
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendClause dropStmt = new DropBackendClause(Lists.newArrayList("192.168.0.1:1234"));
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(dropStmt), new ConnectContext(null));
        try {
            GlobalStateMgr.getCurrentSystemInfo().dropBackends(dropStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            GlobalStateMgr.getCurrentSystemInfo().dropBackends(dropStmt);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        StarOSAgent starosAgent = new StarOSAgent();
        new Expectations(starosAgent) {
            {
                try {
                    starosAgent.removeWorker("192.168.0.1:1235");
                    minTimes = 0;
                    result = null;
                } catch (DdlException e) {
                    e.printStackTrace();
                }
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            StarOSAgent getStarOSAgent() {
                return starosAgent;
            }
        };

        AddBackendClause stmt2 = new AddBackendClause(Lists.newArrayList("192.168.0.1:1235"));
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt2), new ConnectContext(null));

        try {
            GlobalStateMgr.getCurrentSystemInfo().addBackends(stmt2.getHostPortPairs());
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendClause dropStmt2 = new DropBackendClause(Lists.newArrayList("192.168.0.1:1235"));
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(dropStmt2), new ConnectContext(null));

        try {
            GlobalStateMgr.getCurrentSystemInfo().dropBackends(dropStmt2);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage()
                    .contains("starletPort has not been updated by heartbeat from this backend"));
        }

        try {
            GlobalStateMgr.getCurrentSystemInfo().dropBackends(dropStmt2);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testSaveLoadBackend() throws Exception {
        clearAllBackend();
        String dir = "testLoadBackend";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        Backend back1 = new Backend(1L, "localhost", 3);
        back1.updateOnce(4, 6, 8);
        systemInfoService.replayAddBackend(back1);
        long checksum1 = systemInfoService.saveBackends(dos, 0);
        globalStateMgr.clear();
        globalStateMgr = null;
        dos.close();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        long checksum2 = systemInfoService.loadBackends(dis, 0);
        Assert.assertEquals(checksum1, checksum2);
        Assert.assertEquals(1, systemInfoService.getIdToBackend().size());
        Backend back2 = systemInfoService.getBackend(1);
        Assert.assertTrue(back1.equals(back2));
        dis.close();

        deleteDir(dir);
    }

}
