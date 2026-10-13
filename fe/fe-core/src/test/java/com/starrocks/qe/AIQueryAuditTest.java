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

package com.starrocks.qe;

import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.proto.AIExecutionStatisticsPB;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

public class AIQueryAuditTest {
    @Test
    public void testNonAIStatisticsDoNotAddAuditFields() {
        ConnectContext context = new ConnectContext();
        PQueryStatistics statistics = new PQueryStatistics();
        record(context, statistics);
        Assertions.assertNull(statistics.aiStatistics);
        Assertions.assertFalse(render(context.getAuditEventBuilder().buildSnapshot()).contains("|AI"));
    }

    @Test
    public void testUnknownUsageIsOmittedButReportedZeroIsLogged() {
        PQueryStatistics statistics = new PQueryStatistics();
        AIExecutionStatisticsPB ai = new AIExecutionStatisticsPB();
        statistics.aiStatistics = ai;
        ai.taskCount = 1L;
        ai.promptTokens = 0L;
        ai.promptUsageCount = 0L;
        ai.completionTokens = 9L;
        ai.totalUsageCount = 1L;

        ConnectContext unknown = new ConnectContext();
        record(unknown, statistics);
        String unknownLog = render(unknown.getAuditEventBuilder().buildSnapshot());
        Assertions.assertTrue(unknownLog.contains("|AITaskCount=1"), unknownLog);
        Assertions.assertTrue(unknownLog.contains("|AIPromptUsageCount=0"), unknownLog);
        Assertions.assertFalse(unknownLog.contains("|AIPromptTokens="), unknownLog);
        Assertions.assertFalse(unknownLog.contains("|AICompletionTokens="), unknownLog);
        Assertions.assertFalse(unknownLog.contains("|AITotalTokens="), unknownLog);

        ai.promptUsageCount = 1L;
        ConnectContext reportedZero = new ConnectContext();
        record(reportedZero, statistics);
        String zeroLog = render(reportedZero.getAuditEventBuilder().buildSnapshot());
        Assertions.assertTrue(zeroLog.contains("|AIPromptTokens=0"), zeroLog);
        Assertions.assertTrue(zeroLog.contains("|AIPromptUsageCount=1"), zeroLog);
    }

    @Test
    public void testMalformedUsageDoesNotIncreaseCoverage() {
        PQueryStatistics incomplete = new PQueryStatistics();
        incomplete.aiStatistics = new AIExecutionStatisticsPB();
        incomplete.aiStatistics.taskCount = 1L;
        incomplete.aiStatistics.promptTokens = 5L;
        incomplete.aiStatistics.completionUsageCount = 1L;
        incomplete.aiStatistics.totalTokens = -1L;
        incomplete.aiStatistics.totalUsageCount = 1L;
        ConnectContext context = new ConnectContext();
        record(context, incomplete);
        String unknownLog = render(context.getAuditEventBuilder().buildSnapshot());
        Assertions.assertFalse(unknownLog.contains("|AICompletionUsageCount="), unknownLog);
        Assertions.assertFalse(unknownLog.contains("|AITotalUsageCount="), unknownLog);

        PQueryStatistics zero = statistics(0);
        zero.aiStatistics.promptUsageCount = 1L;
        zero.aiStatistics.completionUsageCount = 1L;
        zero.aiStatistics.totalUsageCount = 1L;
        record(context, zero);
        String log = render(context.getAuditEventBuilder().buildSnapshot());
        for (String name : new String[] {"Prompt", "Completion", "Total"}) {
            Assertions.assertTrue(log.contains("|AI" + name + "Tokens=0"), log);
            Assertions.assertTrue((log + "|").contains("|AI" + name + "UsageCount=1|"), log);
        }
    }

    @Test
    public void testStatementAccumulationAndForwardedSnapshotsAreIndependent() {
        ConnectContext context = new ConnectContext();
        PQueryStatistics first = statistics(2);
        record(context, first);
        AuditEvent snapshot = context.getAuditEventBuilder().buildSnapshot();
        record(context, statistics(3));
        assertCounters(render(snapshot), 2);
        assertCounters(render(context.getAuditEventBuilder().buildSnapshot()), 5);

        AuditEvent.AuditEventBuilder forwarded = new AuditEvent.AuditEventBuilder();
        forwarded.copyExecStatsFrom(snapshot);
        first.aiStatistics.taskCount = 99L;
        context.getAuditEventBuilder().reset();
        assertCounters(render(snapshot), 2);
        assertCounters(render(forwarded.buildSnapshot()), 2);
        Assertions.assertFalse(render(context.getAuditEventBuilder().buildSnapshot()).contains("|AI"));
    }

    @Test
    public void testAuditCountersSaturateAndIgnoreNegativeInput() {
        ConnectContext context = new ConnectContext();
        record(context, statistics(Long.MAX_VALUE - 1));
        record(context, statistics(2));
        record(context, statistics(-1));
        assertCounters(render(context.getAuditEventBuilder().buildSnapshot()), Long.MAX_VALUE);
    }

    private static void record(ConnectContext context, PQueryStatistics statistics) {
        StmtExecutor executor = new StmtExecutor(context, new ShowFrontendsStmt());
        executor.setQueryStatistics(statistics);
        executor.recordExecStatsIntoContext();
    }

    private static String render(AuditEvent event) {
        AtomicReference<String> logged = new AtomicReference<>();
        new MockUp<AuditLog>() {
            @Mock
            public void log(String message) {
                logged.set(message);
            }
        };
        boolean jsonFormat = Config.audit_log_json_format;
        try {
            Config.audit_log_json_format = false;
            event.type = AuditEvent.EventType.AFTER_QUERY;
            new AuditLogBuilder().exec(event);
        } finally {
            Config.audit_log_json_format = jsonFormat;
        }
        Assertions.assertNotNull(logged.get());
        return logged.get();
    }

    private static void assertCounters(String log, long value) {
        for (String field : new String[] {"AITaskCount", "AIRequestCount", "AIRetryCount", "AITimeoutCount",
                "AIErrorCount", "AIHttpTimeNs", "AIPromptTokens", "AICompletionTokens", "AITotalTokens",
                "AIPromptUsageCount", "AICompletionUsageCount", "AITotalUsageCount"}) {
            Assertions.assertTrue((log + "|").contains("|" + field + "=" + value + "|"), field + ": " + log);
        }
    }

    private static PQueryStatistics statistics(long value) {
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
}
