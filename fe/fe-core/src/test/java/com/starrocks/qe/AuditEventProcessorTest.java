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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/AuditEventProcessorTest.java

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

import com.starrocks.common.Config;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.EventType;
import com.starrocks.plugin.AuditPlugin;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
<<<<<<< HEAD
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
=======
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
>>>>>>> 46e144a221 ([BugFix] Fix AuditEventProcessor thread exit caused by OutOfMemoryException (#70206))

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AuditEventProcessorTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testAuditEvent() {
        AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setTimestamp(System.currentTimeMillis())
                .setClientIp("127.0.0.1")
                .setUser("user1")
                .setAuthorizedUser("user2")
                .setDb("db1")
                .setState("EOF")
                .setQueryTime(2000)
                .setScanBytes(100000)
                .setScanRows(200000)
                .setReturnRows(1)
                .setStmtId(1234)
                .setStmt("select * from tbl1")
                .setCatalog("catalog1").build();

        Assert.assertEquals("127.0.0.1", event.clientIp);
        Assert.assertEquals(200000, event.scanRows);
        Assert.assertEquals("catalog1", event.catalog);
        Assert.assertEquals("user2", event.authorizedUser);
    }

    @Test
    public void testAuditLogBuilderCONNECTION() throws IOException {
        try (AuditLogBuilder auditLogBuilder = new AuditLogBuilder()) {
            AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.CONNECTION)
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp("127.0.0.1")
                    .setUser("user1")
                    .setAuthorizedUser("user2")
                    .setDb("db1")
                    .setState("EOF")
                    .setQueryTime(2000)
                    .setScanBytes(100000)
                    .setScanRows(200000)
                    .setReturnRows(1)
                    .setStmtId(1234)
                    .setStmt("select * from tbl1").build();

            if (auditLogBuilder.eventFilter(event.type)) {
                auditLogBuilder.exec(event);
                Config.audit_log_json_format = true;
                auditLogBuilder.exec(event);
            }
            Assert.assertEquals(EventType.CONNECTION,  event.type);
        }
    }

    @Test
    public void testAuditLogBuilderBigQuery() throws IOException {
        try (AuditLogBuilder auditLogBuilder = new AuditLogBuilder()) {
            AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp("127.0.0.1")
                    .setUser("user1")
                    .setAuthorizedUser("user2")
                    .setDb("db1")
                    .setState("EOF")
                    .setQueryTime(2000)
                    .setScanBytes(100000)
                    .setScanRows(200000)
                    .setReturnRows(1)
                    .setStmtId(1234)
                    .setStmt("select * from tbl1")
                    .setBigQueryLogCPUSecondThreshold(5)
                    .setCpuCostNs(6 * 1000000000L).build();
            if (auditLogBuilder.eventFilter(event.type)) {
                auditLogBuilder.exec(event);
                Config.audit_log_json_format = true;
                auditLogBuilder.exec(event);
            }
            Assert.assertEquals(6 * 1000000000L, event.cpuCostNs);
            Assert.assertEquals(5, event.bigQueryLogCPUSecondThreshold);
        }
    }

    @Test
    public void testAuditLogBuilder() throws IOException {
        try (AuditLogBuilder auditLogBuilder = new AuditLogBuilder()) {
            PluginInfo pluginInfo = auditLogBuilder.getPluginInfo();
            Assert.assertEquals(DigitalVersion.fromString("0.12.0"), pluginInfo.getVersion());
            Assert.assertEquals(DigitalVersion.fromString("1.8.31"), pluginInfo.getJavaVersion());
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                        .setTimestamp(System.currentTimeMillis())
                        .setClientIp("127.0.0.1")
                        .setUser("user1")
                        .setAuthorizedUser("user2")
                        .setDb("db1")
                        .setState("EOF")
                        .setQueryTime(2000)
                        .setScanBytes(100000)
                        .setScanRows(200000)
                        .setReturnRows(i)
                        .setStmtId(1234)
                        .setStmt("select * from tbl1").build();
                if (auditLogBuilder.eventFilter(event.type)) {
                    auditLogBuilder.exec(event);
                }
            }
            long total = System.currentTimeMillis() - start;
            System.out.println("total(ms): " + total + ", avg: " + total / 10000.0);
        }
    }

    // A plugin that throws OOM on the first exec() call, succeeds afterwards.
    private static class OomOnFirstExecPlugin extends Plugin implements AuditPlugin {
        private final CountDownLatch processedLatch;
        private boolean firstCall = true;

        OomOnFirstExecPlugin(CountDownLatch processedLatch) {
            this.processedLatch = processedLatch;
        }

        @Override
        public boolean eventFilter(AuditEvent.EventType type) {
            return true;
        }

        @Override
        public void exec(AuditEvent event) {
            if (firstCall) {
                firstCall = false;
                throw new OutOfMemoryError("simulated OOM in audit plugin");
            }
            processedLatch.countDown();
        }
    }

    @Test
    public void testWorkerSurvivesOOMInPluginExec(@Mocked PluginMgr mockPluginMgr) throws Exception {
        CountDownLatch processedLatch = new CountDownLatch(2);
        try (OomOnFirstExecPlugin fakePlugin = new OomOnFirstExecPlugin(processedLatch)) {
            new Expectations() {{
                    mockPluginMgr.getActivePluginList(PluginType.AUDIT);
                    result = Collections.singletonList(fakePlugin);
                }};

            AuditEventProcessor processor = new AuditEventProcessor(mockPluginMgr);
            processor.start();
            try {
                AuditEvent event = new AuditEvent.AuditEventBuilder()
                        .setEventType(EventType.AFTER_QUERY)
                        .setTimestamp(System.currentTimeMillis())
                        .setClientIp("127.0.0.1")
                        .setUser("user1")
                        .setDb("db1")
                        .setStmt("select 1")
                        .build();
                for (int i = 0; i < 5; i++) {
                    processor.handleAuditEvent(event);
                }
                Assertions.assertTrue(processedLatch.await(10, TimeUnit.SECONDS),
                        "Worker should survive OOM and continue processing subsequent events");
            } finally {
                processor.stop();
            }
        }
    }

    @Test
    public void testAuditEventProcessor() throws IOException {
        AuditEventProcessor processor = GlobalStateMgr.getCurrentState().getAuditEventProcessor();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp("127.0.0.1")
                    .setUser("user1")
                    .setAuthorizedUser("user2")
                    .setDb("db1")
                    .setState("EOF")
                    .setQueryTime(2000)
                    .setScanBytes(100000)
                    .setScanRows(200000)
                    .setReturnRows(i)
                    .setStmtId(1234)
                    .setStmt("select * from tbl1").build();
            processor.handleAuditEvent(event);
        }
        long total = System.currentTimeMillis() - start;
        System.out.println("total(ms): " + total + ", avg: " + total / 10000.0);
    }
}
