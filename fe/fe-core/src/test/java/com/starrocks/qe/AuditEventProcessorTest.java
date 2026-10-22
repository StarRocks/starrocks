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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.common.AuditLog;
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
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AuditEventProcessorTest extends StarRocksTestBase {

    @BeforeAll
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

        Assertions.assertEquals("127.0.0.1", event.clientIp);
        Assertions.assertEquals(200000, event.scanRows);
        Assertions.assertEquals("catalog1", event.catalog);
        Assertions.assertEquals("user2", event.authorizedUser);
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
            Assertions.assertEquals(EventType.CONNECTION,  event.type);
        }
    }

    @Test
    public void testAuditLogBuilderWritesMemUsageGiBHours() throws IOException {
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        new MockUp<AuditLog>() {
            @Mock
            public void log(String message) {
                if (message.contains(MEM_USAGE_STMT)) {
                    lines.add(message);
                }
            }
        };
        boolean jsonFormat = Config.audit_log_json_format;
        try (AuditLogBuilder auditLogBuilder = new AuditLogBuilder()) {
            AuditEvent metered = memUsageEvent().addMemUsageGiBHours(0.75).build();
            AuditEvent tiny = memUsageEvent().addMemUsageGiBHours(1e-5).build();
            AuditEvent unset = memUsageEvent().build();

            Config.audit_log_json_format = false;
            auditLogBuilder.exec(metered);
            Assertions.assertTrue(lines.get(lines.size() - 1).contains("|MemUsageGiBHours=0.75"));
            auditLogBuilder.exec(tiny);
            Assertions.assertTrue(lines.get(lines.size() - 1).contains("|MemUsageGiBHours=1.0E-5"));
            auditLogBuilder.exec(unset);
            Assertions.assertFalse(lines.get(lines.size() - 1).contains("MemUsageGiBHours"));

            Config.audit_log_json_format = true;
            ObjectMapper mapper = new ObjectMapper();
            auditLogBuilder.exec(metered);
            JsonNode meteredJson = mapper.readTree(lines.get(lines.size() - 1));
            Assertions.assertTrue(meteredJson.get("MemUsageGiBHours").isNumber());
            Assertions.assertEquals(0.75, meteredJson.get("MemUsageGiBHours").asDouble(), 1e-12);
            auditLogBuilder.exec(tiny);
            JsonNode tinyJson = mapper.readTree(lines.get(lines.size() - 1));
            Assertions.assertTrue(tinyJson.get("MemUsageGiBHours").isNumber());
            Assertions.assertEquals(1e-5, tinyJson.get("MemUsageGiBHours").asDouble(), 1e-12);
            auditLogBuilder.exec(unset);
            Assertions.assertFalse(mapper.readTree(lines.get(lines.size() - 1)).has("MemUsageGiBHours"));
        } finally {
            Config.audit_log_json_format = jsonFormat;
        }
    }

    private static final String MEM_USAGE_STMT = "select 'audit_mem_usage_gib_hours_test'";

    private static AuditEvent.AuditEventBuilder memUsageEvent() {
        return new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setTimestamp(System.currentTimeMillis())
                .setClientIp("127.0.0.1")
                .setUser("user1")
                .setDb("db1")
                .setState("EOF")
                .setQueryTime(1)
                .setStmt(MEM_USAGE_STMT);
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
            Assertions.assertEquals(6 * 1000000000L, event.cpuCostNs);
            Assertions.assertEquals(5, event.bigQueryLogCPUSecondThreshold);
        }
    }

    @Test
    public void testAuditLogBuilder() throws IOException {
        try (AuditLogBuilder auditLogBuilder = new AuditLogBuilder()) {
            PluginInfo pluginInfo = auditLogBuilder.getPluginInfo();
            Assertions.assertEquals(DigitalVersion.fromString("0.12.0"), pluginInfo.getVersion());
            Assertions.assertEquals(DigitalVersion.fromString("1.8.31"), pluginInfo.getJavaVersion());
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
            logSysInfo("total(ms): " + total + ", avg: " + total / 10000.0);
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
        logSysInfo("total(ms): " + total + ", avg: " + total / 10000.0);
    }
}
