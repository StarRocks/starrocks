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

package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.metric.PrometheusMetricVisitor;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// removed unused concurrency/imports after test consolidation
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionMetricRegistryTest {

    private String originalGroups;

    @BeforeEach
    public void setUp() {
        originalGroups = Config.txn_latency_metric_report_groups;
    }

    @AfterEach
    public void tearDown() {
        Config.txn_latency_metric_report_groups = originalGroups;
    }

    @Test
    public void testConfigParsingEnableDisable(@Mocked GlobalStateMgr globalStateMgr) {
        setLeader(true, globalStateMgr);
        TransactionMetricRegistry registry = newRegistry();
        // enable multiple groups with case/space variants
        setReportGroups(registry, " Stream_Load , ROUTINE_LOAD  ");
        String out1 = renderReport(registry);
        Assertions.assertTrue(out1.contains("type=\"stream_load\""), out1);
        Assertions.assertTrue(out1.contains("type=\"routine_load\""), out1);
        Assertions.assertTrue(out1.contains("type=\"all\""), out1);
        // idempotent when unchanged
        registry.updateConfig();
        String out2 = renderReport(registry);
        Assertions.assertEquals(out1, out2);
        Assertions.assertTrue(out2.contains("type=\"all\""), out2);
        // unknown ignored, all remains
        setReportGroups(registry, "unknown,another_unknown");
        String out3 = renderReport(registry);
        Assertions.assertFalse(out3.contains("type=\"unknown\""), out3);
        Assertions.assertTrue(out3.contains("type=\"all\""), out3);
        // disable clears enabled groups
        setReportGroups(registry, "");
        String out4 = renderReport(registry);
        Assertions.assertFalse(out4.contains("type=\"stream_load\""), out4);
        Assertions.assertFalse(out4.contains("type=\"routine_load\""), out4);
        Assertions.assertTrue(out4.contains("type=\"all\""), out4);
    }

    @Test
    public void testReportAllAndEnabledGroups(@Mocked GlobalStateMgr globalStateMgr) {
        setLeader(true, globalStateMgr);
        TransactionMetricRegistry registry = newRegistry();
        // all group always reported
        String out = renderReport(registry);
        Assertions.assertTrue(out.contains("type=\"all\""), out);
        // enable one group then disable
        setReportGroups(registry, "broker_load");
        String out1 = renderReport(registry);
        Assertions.assertTrue(out1.contains("type=\"broker_load\""), out1);
        Assertions.assertTrue(out1.contains("type=\"all\""), out1);
        setReportGroups(registry, "");
        String out2 = renderReport(registry);
        Assertions.assertFalse(out2.contains("type=\"broker_load\""), out2);
        Assertions.assertTrue(out2.contains("type=\"all\""), out2);
    }

    @Test
    public void testAllGroupUpdatesAndDisabledGroup(@Mocked GlobalStateMgr globalStateMgr,
                                                    @Mocked TransactionState t1,
                                                    @Mocked TransactionState t2,
                                                    @Mocked TransactionState t3) {
        setLeader(true, globalStateMgr);
        TransactionMetricRegistry registry = newRegistry();
        // no specific groups enabled
        setReportGroups(registry, "");
        setupVisibleTxn(t1, TransactionState.LoadJobSourceType.INSERT_STREAMING, 10, 20, 30, 40, 50);
        registry.update(t1);
        String outA = renderReport(registry);
        Assertions.assertEquals(1, extractCount(outA, "txn_total_latency_ms", "all", true));
        // specific group enabled and updated
        setReportGroups(registry, "stream_load");
        setupVisibleTxn(t2, TransactionState.LoadJobSourceType.BACKEND_STREAMING, 11, 21, 31, 41, 51);
        registry.update(t2);
        String outB = renderReport(registry);
        Assertions.assertEquals(2, extractCount(outB, "txn_total_latency_ms", "all", true));
        Assertions.assertEquals(1, extractCount(outB, "txn_total_latency_ms", "stream_load", true));
        // disable group; updates should not create stream_load reporting
        setReportGroups(registry, "");
        setupVisibleTxn(t3, TransactionState.LoadJobSourceType.BACKEND_STREAMING, 12, 22, 32, 42, 52);
        registry.update(t3);
        String outC = renderReport(registry);
        Assertions.assertEquals(3, extractCount(outC, "txn_total_latency_ms", "all", true));
        Assertions.assertFalse(outC.contains("type=\"stream_load\""), outC);
    }

    @Test
    public void testSourceTypeGroupingMappings(@Mocked GlobalStateMgr globalStateMgr,
                                               @Mocked TransactionState s1,
                                               @Mocked TransactionState s2,
                                               @Mocked TransactionState s3,
                                               @Mocked TransactionState r1,
                                               @Mocked TransactionState b1,
                                               @Mocked TransactionState i1,
                                               @Mocked TransactionState c1) {
        setLeader(true, globalStateMgr);
        TransactionMetricRegistry registry = newRegistry();
        // stream_load accumulates across three source types
        setReportGroups(registry, "stream_load");
        setupVisibleTxn(s1, TransactionState.LoadJobSourceType.BACKEND_STREAMING, 10, 20, 30, 40, 50);
        setupVisibleTxn(s2, TransactionState.LoadJobSourceType.FRONTEND_STREAMING, 11, 21, 31, 41, 51);
        setupVisibleTxn(s3, TransactionState.LoadJobSourceType.MULTI_STATEMENT_STREAMING, 12, 22, 32, 42, 52);
        registry.update(s1);
        registry.update(s2);
        registry.update(s3);
        String outS = renderReport(registry);
        Assertions.assertEquals(3, extractCount(outS, "txn_total_latency_ms", "stream_load", true));
        Assertions.assertEquals(3, extractCount(outS, "txn_total_latency_ms", "all", true));
        // routine_load
        setReportGroups(registry, "routine_load");
        setupVisibleTxn(r1, TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, 10, 20, 30, 40, 50);
        registry.update(r1);
        String outR = renderReport(registry);
        Assertions.assertEquals(1, extractCount(outR, "txn_total_latency_ms", "routine_load", true));
        Assertions.assertEquals(4, extractCount(outR, "txn_total_latency_ms", "all", true));
        // broker_load
        setReportGroups(registry, "broker_load");
        setupVisibleTxn(b1, TransactionState.LoadJobSourceType.BATCH_LOAD_JOB, 10, 20, 30, 40, 50);
        registry.update(b1);
        String outB = renderReport(registry);
        Assertions.assertEquals(1, extractCount(outB, "txn_total_latency_ms", "broker_load", true));
        Assertions.assertEquals(5, extractCount(outB, "txn_total_latency_ms", "all", true));
        // insert
        setReportGroups(registry, "insert");
        setupVisibleTxn(i1, TransactionState.LoadJobSourceType.INSERT_STREAMING, 10, 20, 30, 40, 50);
        registry.update(i1);
        String outI = renderReport(registry);
        Assertions.assertEquals(1, extractCount(outI, "txn_total_latency_ms", "insert", true));
        Assertions.assertEquals(6, extractCount(outI, "txn_total_latency_ms", "all", true));
        // compaction
        setReportGroups(registry, "compaction");
        setupVisibleTxn(c1, TransactionState.LoadJobSourceType.LAKE_COMPACTION, 10, 20, 30, 40, 50);
        registry.update(c1);
        String outC = renderReport(registry);
        Assertions.assertEquals(1, extractCount(outC, "txn_total_latency_ms", "compaction", true));
        Assertions.assertEquals(7, extractCount(outC, "txn_total_latency_ms", "all", true));
    }

    @Test
    public void testLabelsAndLeaderToggle(@Mocked GlobalStateMgr globalStateMgr) {
        setLeader(true, globalStateMgr);
        TransactionMetricRegistry registry = newRegistry();
        setReportGroups(registry, "insert");
        String out1 = renderReport(registry);
        Assertions.assertTrue(out1.contains("type=\"insert\""), out1);
        Assertions.assertTrue(out1.contains("type=\"all\""), out1);
        Assertions.assertTrue(out1.contains("is_leader=\"true\""), out1);
        setLeader(false, globalStateMgr);
        String out2 = renderReport(registry);
        Assertions.assertTrue(out2.contains("type=\"all\""), out2);
        Assertions.assertTrue(out2.contains("is_leader=\"false\""), out2);
    }

    private void setLeader(boolean leader, @Mocked GlobalStateMgr globalStateMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = leader;
            }
        };
    }

    private TransactionMetricRegistry newRegistry() {
        return TransactionMetricRegistry.createForTest();
    }

    private void setReportGroups(TransactionMetricRegistry registry, String groups) {
        Config.txn_latency_metric_report_groups = groups;
        registry.updateConfig();
    }

    private String renderReport(TransactionMetricRegistry registry) {
        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("");
        registry.report(visitor);
        return visitor.build();
    }

    private void setupVisibleTxn(@Mocked TransactionState txn,
                                 TransactionState.LoadJobSourceType type,
                                 long prepare, long commit, long pub, long pubFinish, long finish) {
        new Expectations() {
            {
                txn.getTransactionStatus();
                result = TransactionStatus.VISIBLE;
                txn.getPrepareTime();
                result = prepare;
                txn.getCommitTime();
                result = commit;
                txn.getPublishVersionTime();
                result = pub;
                txn.getPublishVersionFinishTime();
                result = pubFinish;
                txn.getFinishTime();
                result = finish;
                txn.getSourceType();
                result = type;
            }
        };
    }

    private int extractCount(String output, String metric, String typeName, boolean leader) {
        String leaderTag = leader ? "true" : "false";
        String pattern =
                ".*_(" + Pattern.quote(metric) + ")_count\\{([^}]*type=\"" + Pattern.quote(typeName) + "\"[^}]*)}\\s+(\\d+)";
        Pattern p = Pattern.compile(pattern, Pattern.DOTALL);
        Matcher m = p.matcher(output);
        int last = -1;
        while (m.find()) {
            if (m.group(2).contains("is_leader=\"" + leaderTag + "\"")) {
                last = Integer.parseInt(m.group(3));
            }
        }
        return last;
    }
}
