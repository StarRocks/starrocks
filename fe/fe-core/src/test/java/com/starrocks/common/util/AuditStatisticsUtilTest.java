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

package com.starrocks.common.util;

import com.starrocks.proto.AIExecutionStatisticsPB;
import com.starrocks.proto.NodeExecStatsItemPB;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.thrift.TAIExecutionStatistics;
import com.starrocks.thrift.TAuditStatistics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

public class AuditStatisticsUtilTest {
    @Test
    public void testCopyProtobufPreservesAllFieldsAndDetachesNestedState() {
        TAIExecutionStatistics ai = new TAIExecutionStatistics()
                .setTask_count(1).setRequest_count(2).setRetry_count(3).setTimeout_count(4)
                .setError_count(5).setHttp_time_ns(6).setPrompt_tokens(7).setCompletion_tokens(8)
                .setTotal_tokens(9).setPrompt_usage_count(10).setCompletion_usage_count(11).setTotal_usage_count(12);
        PQueryStatistics source = AuditStatisticsUtil.toProtobuf(new TAuditStatistics().setAi_statistics(ai));
        source.scanRows = 1L;
        source.scanBytes = 2L;
        source.returnedRows = 3L;
        source.cpuCostNs = 4L;
        source.memCostBytes = 5L;
        source.spillBytes = 6L;
        source.transmittedBytes = 7L;
        source.readLocalCnt = 8L;
        source.readRemoteCnt = 9L;
        QueryStatisticsItemPB item = new QueryStatisticsItemPB();
        item.scanRows = 10L;
        item.scanBytes = 11L;
        item.tableId = 12L;
        source.statsItems = new ArrayList<>(Collections.singletonList(item));
        NodeExecStatsItemPB node = new NodeExecStatsItemPB();
        node.pushRows = 13L;
        node.pullRows = 14L;
        node.predFilterRows = 15L;
        node.indexFilterRows = 16L;
        node.rfFilterRows = 17L;
        node.nodeId = 18;
        source.nodeExecStatsItems = new ArrayList<>(Collections.singletonList(node));

        PQueryStatistics copy = AuditStatisticsUtil.copyProtobuf(source);
        Assertions.assertNotSame(source, copy);
        Assertions.assertArrayEquals(new Long[] {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L},
                queryValues(copy));
        Assertions.assertNotSame(source.statsItems, copy.statsItems);
        Assertions.assertNotSame(item, copy.statsItems.get(0));
        QueryStatisticsItemPB copiedItem = copy.statsItems.get(0);
        Assertions.assertArrayEquals(new Long[] {10L, 11L, 12L},
                new Long[] {copiedItem.scanRows, copiedItem.scanBytes, copiedItem.tableId});
        Assertions.assertNotSame(source.nodeExecStatsItems, copy.nodeExecStatsItems);
        Assertions.assertNotSame(node, copy.nodeExecStatsItems.get(0));
        NodeExecStatsItemPB copiedNode = copy.nodeExecStatsItems.get(0);
        Assertions.assertArrayEquals(new Long[] {13L, 14L, 15L, 16L, 17L}, new Long[] {
                copiedNode.pushRows, copiedNode.pullRows, copiedNode.predFilterRows,
                copiedNode.indexFilterRows, copiedNode.rfFilterRows});
        Assertions.assertEquals(18, copiedNode.nodeId);
        Assertions.assertNotSame(source.aiStatistics, copy.aiStatistics);
        Assertions.assertArrayEquals(aiValues(source.aiStatistics), aiValues(copy.aiStatistics));

        item.scanRows = 99L;
        node.pushRows = 99L;
        source.aiStatistics.promptTokens = 99L;
        source.statsItems.clear();
        source.nodeExecStatsItems.clear();
        Assertions.assertEquals(10L, copy.statsItems.get(0).scanRows);
        Assertions.assertEquals(13L, copy.nodeExecStatsItems.get(0).pushRows);
        Assertions.assertEquals(7L, copy.aiStatistics.promptTokens);
        copy.aiStatistics.totalTokens = 99L;
        Assertions.assertEquals(9L, source.aiStatistics.totalTokens);
    }

    @Test
    public void testCopyProtobufPreservesNullZeroAndEmptyPresence() {
        Assertions.assertNull(AuditStatisticsUtil.copyProtobuf(null));
        PQueryStatistics absent = AuditStatisticsUtil.copyProtobuf(new PQueryStatistics());
        Assertions.assertArrayEquals(new Long[9], queryValues(absent));
        Assertions.assertNull(absent.statsItems);
        Assertions.assertNull(absent.nodeExecStatsItems);
        Assertions.assertNull(absent.aiStatistics);

        PQueryStatistics source = new PQueryStatistics();
        source.scanRows = 0L;
        source.statsItems = new ArrayList<>();
        source.nodeExecStatsItems = new ArrayList<>();
        source.aiStatistics = new AIExecutionStatisticsPB();
        PQueryStatistics empty = AuditStatisticsUtil.copyProtobuf(source);
        Assertions.assertEquals(0L, empty.scanRows);
        Assertions.assertNull(empty.scanBytes);
        Assertions.assertNotSame(source.statsItems, empty.statsItems);
        Assertions.assertTrue(empty.statsItems.isEmpty());
        Assertions.assertNotSame(source.nodeExecStatsItems, empty.nodeExecStatsItems);
        Assertions.assertTrue(empty.nodeExecStatsItems.isEmpty());
        Assertions.assertNotNull(empty.aiStatistics);
        Assertions.assertArrayEquals(new Long[12], aiValues(empty.aiStatistics));
    }

    @Test
    public void testCopyProtobufDoesNotNormalizeTelemetry() {
        PQueryStatistics source = aiStatistics(-1);
        source.scanRows = -1L;
        source.aiStatistics.promptTokens = 0L;
        source.aiStatistics.promptUsageCount = null;
        PQueryStatistics copy = AuditStatisticsUtil.copyProtobuf(source);
        Assertions.assertEquals(-1L, copy.scanRows);
        Assertions.assertArrayEquals(aiValues(source.aiStatistics), aiValues(copy.aiStatistics));
    }

    private static Long[] queryValues(PQueryStatistics statistics) {
        return new Long[] {statistics.scanRows, statistics.scanBytes, statistics.returnedRows, statistics.cpuCostNs,
                statistics.memCostBytes, statistics.spillBytes, statistics.transmittedBytes,
                statistics.readLocalCnt, statistics.readRemoteCnt};
    }

    @Test
    public void testAIStatisticsRoundTrip() {
        TAIExecutionStatistics ai = new TAIExecutionStatistics()
                .setTask_count(1).setRequest_count(2).setRetry_count(3).setTimeout_count(4)
                .setError_count(5).setHttp_time_ns(6).setPrompt_tokens(7).setCompletion_tokens(8)
                .setTotal_tokens(9).setPrompt_usage_count(10).setCompletion_usage_count(11).setTotal_usage_count(12);
        TAuditStatistics thrift = new TAuditStatistics().setAi_statistics(ai);
        PQueryStatistics protobuf = AuditStatisticsUtil.toProtobuf(thrift);
        Assertions.assertNotNull(protobuf.aiStatistics);
        Assertions.assertArrayEquals(new Long[] {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L},
                aiValues(protobuf.aiStatistics));
        Assertions.assertEquals(ai, AuditStatisticsUtil.toThrift(protobuf).getAi_statistics());
    }

    @Test
    public void testAIStatisticsPreserveOptionalPresence() {
        Assertions.assertNull(AuditStatisticsUtil.toProtobuf(new TAuditStatistics()).aiStatistics);
        Assertions.assertFalse(AuditStatisticsUtil.toThrift(new PQueryStatistics()).isSetAi_statistics());

        TAIExecutionStatistics ai = new TAIExecutionStatistics()
                .setTask_count(0).setPrompt_tokens(0).setPrompt_usage_count(1);
        PQueryStatistics protobuf = AuditStatisticsUtil.toProtobuf(new TAuditStatistics().setAi_statistics(ai));
        Assertions.assertNotNull(protobuf.aiStatistics);
        Assertions.assertArrayEquals(new Long[] {0L, null, null, null, null, null, 0L, null, null, 1L, null, null},
                aiValues(protobuf.aiStatistics));
        Assertions.assertEquals(ai, AuditStatisticsUtil.toThrift(protobuf).getAi_statistics());

        PQueryStatistics empty = AuditStatisticsUtil.toProtobuf(
                new TAuditStatistics().setAi_statistics(new TAIExecutionStatistics()));
        Assertions.assertNotNull(empty.aiStatistics);
        Assertions.assertArrayEquals(new Long[12], aiValues(empty.aiStatistics));
        Assertions.assertEquals(new TAIExecutionStatistics(), AuditStatisticsUtil.toThrift(empty).getAi_statistics());
    }

    @Test
    public void testAIStatisticsMergeDoesNotAllocateForNonAIQueries() {
        PQueryStatistics to = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(new PQueryStatistics(), to);
        AuditStatisticsUtil.mergeProtobuf(null, to);
        Assertions.assertNull(to.aiStatistics);
    }

    @Test
    public void testAIStatisticsMergeCopiesAndAddsAllCounters() {
        PQueryStatistics from = aiStatistics(2);
        PQueryStatistics to = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(from, to);
        Assertions.assertNotNull(to.aiStatistics);
        Assertions.assertNotSame(from.aiStatistics, to.aiStatistics);
        assertAIValues(2, to.aiStatistics);
        AuditStatisticsUtil.mergeProtobuf(from, to);
        assertAIValues(4, to.aiStatistics);
        assertAIValues(2, from.aiStatistics);
        from.aiStatistics.taskCount = 99L;
        Assertions.assertEquals(4L, to.aiStatistics.taskCount);
    }

    @Test
    public void testAIStatisticsMergePreservesMissingCounters() {
        PQueryStatistics from = new PQueryStatistics();
        from.aiStatistics = new AIExecutionStatisticsPB();
        from.aiStatistics.promptTokens = 0L;
        from.aiStatistics.promptUsageCount = 1L;
        PQueryStatistics to = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(from, to);
        Assertions.assertNotNull(to.aiStatistics);
        Assertions.assertArrayEquals(new Long[] {null, null, null, null, null, null, 0L, null, null, 1L, null, null},
                aiValues(to.aiStatistics));
    }

    @Test
    public void testAIStatisticsMergeSaturatesAndRejectsNegativeCounters() {
        PQueryStatistics to = aiStatistics(Long.MAX_VALUE - 1);
        AuditStatisticsUtil.mergeProtobuf(aiStatistics(2), to);
        assertAIValues(Long.MAX_VALUE, to.aiStatistics);
        AuditStatisticsUtil.mergeProtobuf(aiStatistics(-1), to);
        assertAIValues(Long.MAX_VALUE, to.aiStatistics);

        PQueryStatistics invalid = aiStatistics(-1);
        AuditStatisticsUtil.mergeProtobuf(aiStatistics(2), invalid);
        assertAIValues(2, invalid.aiStatistics);
        PQueryStatistics empty = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(aiStatistics(-1), empty);
        Assertions.assertNotNull(empty.aiStatistics);
        Assertions.assertArrayEquals(new Long[12], aiValues(empty.aiStatistics));
    }

    @Test
    public void testAIUsageCannotCombineIncompleteReportsIntoValidUsage() {
        PQueryStatistics incomplete = new PQueryStatistics();
        incomplete.aiStatistics = new AIExecutionStatisticsPB();
        incomplete.aiStatistics.promptTokens = 5L;
        incomplete.aiStatistics.completionUsageCount = 1L;
        incomplete.aiStatistics.totalTokens = -1L;
        incomplete.aiStatistics.totalUsageCount = 1L;
        PQueryStatistics accumulated = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(incomplete, accumulated);
        Assertions.assertArrayEquals(new Long[12], aiValues(accumulated.aiStatistics));
        Assertions.assertEquals(5L, incomplete.aiStatistics.promptTokens);

        PQueryStatistics zero = aiStatistics(0);
        zero.aiStatistics.promptUsageCount = 1L;
        zero.aiStatistics.completionUsageCount = 1L;
        zero.aiStatistics.totalUsageCount = 1L;
        AuditStatisticsUtil.mergeProtobuf(zero, accumulated);
        Assertions.assertArrayEquals(new Long[] {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L, 1L, 1L},
                aiValues(accumulated.aiStatistics));

        // The coordinator can initialize its accumulator directly from a converted first report.
        AuditStatisticsUtil.mergeProtobuf(zero, incomplete);
        Assertions.assertArrayEquals(aiValues(accumulated.aiStatistics), aiValues(incomplete.aiStatistics));
    }

    @Test
    public void testAIUsageWithZeroCoverageDoesNotContributeTokens() {
        PQueryStatistics from = aiStatistics(3);
        from.aiStatistics.promptUsageCount = 0L;
        from.aiStatistics.completionUsageCount = -1L;
        from.aiStatistics.totalUsageCount = null;
        PQueryStatistics to = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(from, to);
        Assertions.assertNull(to.aiStatistics.promptTokens);
        Assertions.assertEquals(0L, to.aiStatistics.promptUsageCount);
        Assertions.assertNull(to.aiStatistics.completionTokens);
        Assertions.assertNull(to.aiStatistics.completionUsageCount);
        Assertions.assertNull(to.aiStatistics.totalTokens);
        Assertions.assertNull(to.aiStatistics.totalUsageCount);
    }

    @Test
    public void testAIStatisticsConversionRejectsNegativeCounters() {
        TAuditStatistics thrift = AuditStatisticsUtil.toThrift(aiStatistics(-1));
        Assertions.assertEquals(new TAIExecutionStatistics(), thrift.getAi_statistics());
        TAIExecutionStatistics invalid = new TAIExecutionStatistics()
                .setTask_count(-1).setRequest_count(-1).setRetry_count(-1).setTimeout_count(-1)
                .setError_count(-1).setHttp_time_ns(-1).setPrompt_tokens(-1).setCompletion_tokens(-1)
                .setTotal_tokens(-1).setPrompt_usage_count(-1).setCompletion_usage_count(-1).setTotal_usage_count(-1);
        PQueryStatistics protobuf = AuditStatisticsUtil.toProtobuf(new TAuditStatistics().setAi_statistics(invalid));
        Assertions.assertNotNull(protobuf.aiStatistics);
        Assertions.assertArrayEquals(new Long[12], aiValues(protobuf.aiStatistics));
    }

    private static PQueryStatistics aiStatistics(long value) {
        PQueryStatistics statistics = new PQueryStatistics();
        AIExecutionStatisticsPB ai = new AIExecutionStatisticsPB();
        ai.taskCount = value;
        ai.requestCount = value;
        ai.retryCount = value;
        ai.timeoutCount = value;
        ai.errorCount = value;
        ai.httpTimeNs = value;
        ai.promptTokens = value;
        ai.completionTokens = value;
        ai.totalTokens = value;
        ai.promptUsageCount = value;
        ai.completionUsageCount = value;
        ai.totalUsageCount = value;
        statistics.aiStatistics = ai;
        return statistics;
    }

    private static Long[] aiValues(AIExecutionStatisticsPB ai) {
        return new Long[] {ai.taskCount, ai.requestCount, ai.retryCount, ai.timeoutCount, ai.errorCount, ai.httpTimeNs,
                ai.promptTokens, ai.completionTokens, ai.totalTokens,
                ai.promptUsageCount, ai.completionUsageCount, ai.totalUsageCount};
    }

    private static void assertAIValues(long expected, AIExecutionStatisticsPB ai) {
        Assertions.assertNotNull(ai);
        for (Long value : aiValues(ai)) {
            Assertions.assertEquals(expected, value);
        }
    }

    @Test
    public void test() {
        TAuditStatistics ts = new TAuditStatistics();
        ts.setScan_rows(1);
        ts.setScan_bytes(2);
        ts.setReturned_rows(3);
        ts.setCpu_cost_ns(4);
        ts.setMem_cost_bytes(5);
        ts.setSpill_bytes(6);
        ts.setTransmitted_bytes(7);
        ts.setRead_local_cnt(8);
        ts.setRead_remote_cnt(9);
        PQueryStatistics ps = AuditStatisticsUtil.toProtobuf(ts);
        Assertions.assertEquals(ps.scanRows, 1);
        Assertions.assertEquals(ps.scanBytes, 2);
        Assertions.assertEquals(ps.returnedRows, 3);
        Assertions.assertEquals(ps.cpuCostNs, 4);
        Assertions.assertEquals(ps.memCostBytes, 5);
        Assertions.assertEquals(ps.spillBytes, 6);
        Assertions.assertEquals(ps.transmittedBytes, 7);
        Assertions.assertEquals(ps.readLocalCnt, 8);
        Assertions.assertEquals(ps.readRemoteCnt, 9);

        PQueryStatistics ps2 = new PQueryStatistics();
        AuditStatisticsUtil.mergeProtobuf(ps, ps2);
        Assertions.assertEquals(ps2.scanRows, 1);
        Assertions.assertEquals(ps2.scanBytes, 2);
        Assertions.assertEquals(ps2.returnedRows, 3);
        Assertions.assertEquals(ps2.cpuCostNs, 4);
        Assertions.assertEquals(ps2.memCostBytes, 5);
        Assertions.assertEquals(ps2.spillBytes, 6);
        Assertions.assertEquals(ps2.transmittedBytes, 7);
        Assertions.assertEquals(ps2.readLocalCnt, 8);
        Assertions.assertEquals(ps2.readRemoteCnt, 9);

        TAuditStatistics ts2 = AuditStatisticsUtil.toThrift(ps);
        Assertions.assertEquals(ts2.getScan_rows(), 1);
        Assertions.assertEquals(ts2.getScan_bytes(), 2);
        Assertions.assertEquals(ts2.getReturned_rows(), 3);
        Assertions.assertEquals(ts2.getCpu_cost_ns(), 4);
        Assertions.assertEquals(ts2.getMem_cost_bytes(), 5);
        Assertions.assertEquals(ts2.getSpill_bytes(), 6);
        Assertions.assertEquals(ts2.getTransmitted_bytes(), 7);
        Assertions.assertEquals(ts2.getRead_local_cnt(), 8);
        Assertions.assertEquals(ts2.getRead_remote_cnt(), 9);
    }
}
