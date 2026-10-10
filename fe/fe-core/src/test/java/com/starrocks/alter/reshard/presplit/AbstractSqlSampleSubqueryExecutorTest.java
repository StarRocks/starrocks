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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractSqlSampleSubqueryExecutorTest {

    @Test
    void loadTimeZoneReachesTheSamplePlanAfterWarehouseSwitchAndKeepsTheTimeout() {
        SessionVariable initialDefaults = new SessionVariable();
        initialDefaults.setTimeZone("UTC");
        SessionVariable warehouseDefaults = new SessionVariable();
        warehouseDefaults.setTimeZone("Asia/Shanghai");
        AtomicReference<SessionVariable> current = new AtomicReference<>(initialDefaults);
        ConnectContext sampleContext = mock(ConnectContext.class);
        when(sampleContext.getSessionVariable()).thenAnswer(ignored -> current.get());
        doAnswer(ignored -> {
            current.set((SessionVariable) warehouseDefaults.clone());
            return null;
        }).when(sampleContext).setCurrentWarehouseId(4242L);
        doAnswer(invocation -> {
            current.set(invocation.getArgument(0));
            return null;
        }).when(sampleContext).setSessionVariable(any());
        ComputeResource resource = mock(ComputeResource.class);
        when(resource.getWarehouseId()).thenReturn(4242L);
        AtomicReference<String> plannedTimeZone = new AtomicReference<>();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor();
        SampleRequest request = new SampleRequest(new InsertFromTableScanContext(
                mock(OlapTable.class), "`db`.`src`", Map.of("k", "k"), null, resource, 1024L, 100L,
                Map.of(), Map.of(), "America/Los_Angeles", SampleSessionSemantics.NONE),
                List.of(bigintColumn("k")), Long.MAX_VALUE, 0L)
                .withQueryTimeoutSeconds(137);

        try (MockedStatic<StatisticUtils> statistics = mockStatic(StatisticUtils.class);
                MockedStatic<StatementPlanner> planner = mockStatic(StatementPlanner.class)) {
            statistics.when(StatisticUtils::buildConnectContext).thenReturn(sampleContext);
            planner.when(() -> StatementPlanner.plan(any(), eq(sampleContext), eq(TResultSinkType.HTTP_PROTOCAL)))
                    .thenAnswer(ignored -> {
                        plannedTimeZone.set(sampleContext.getSessionVariable().getTimeZone());
                        Assertions.assertEquals(137, sampleContext.getSessionVariable().getQueryTimeoutS());
                        throw new SemanticException("stop before executing the sample plan");
                    });

            Assertions.assertThrows(StarRocksException.class, () -> executor.execute(request));
        }

        Assertions.assertEquals("America/Los_Angeles", plannedTimeZone.get());
        // This must hold even when SimpleExecutor ignores SET_VAR hints (as on branch-26.2).
        Assertions.assertEquals("America/Los_Angeles", sampleContext.getSessionVariable().getTimeZone());
        Assertions.assertEquals("Asia/Shanghai", warehouseDefaults.getTimeZone(), "warehouse defaults stay untouched");
        Assertions.assertEquals("UTC", initialDefaults.getTimeZone());
    }

    /**
     * Regression guard: {@code ConnectContext.setCurrentWarehouseId} delegates to
     * {@code setCurrentWarehouse}, which REPLACES the session-variable object with a fresh
     * warehouse-defaulted one (re-applying only tracked SET variables). The pre-submit-budget
     * {@code query_timeout} is applied via a direct setter (not a tracked SET), so it must be set
     * AFTER the warehouse switch or it is silently dropped — letting an over-budget sample run to
     * the warehouse/default timeout and block the load past the pre-submit budget.
     */
    @Test
    void queryTimeoutIsAppliedAfterWarehouseSwitchSoItSurvives() throws Exception {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(4242L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 137, /*loadTimeZone=*/ null, SampleSessionSemantics.NONE);

        // The warehouse switch (which swaps the session variable) MUST precede the timeout setter.
        InOrder inOrder = inOrder(context, sessionVariable);
        inOrder.verify(context).setCurrentWarehouseId(4242L);
        inOrder.verify(sessionVariable).setQueryTimeoutS(137);
    }

    @Test
    void nonPositiveQueryTimeoutLeavesSessionTimeoutUntouched() throws Exception {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(7L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 0, /*loadTimeZone=*/ null, SampleSessionSemantics.NONE);

        verify(context).setCurrentWarehouseId(7L);
        verify(sessionVariable, never()).setQueryTimeoutS(anyInt());
    }

    /**
     * The load's semantic variables must land on the session variable the sub-query runs with: after the warehouse
     * switch, which replaces that object, and before the sampler's own overrides, which a carried value must not undo.
     */
    @Test
    void loadSessionSemanticsAreAppliedAfterTheWarehouseSwitchAndBeforeTheSamplerOverrides() throws Exception {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(5L);
        long loadSqlMode = SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL;
        SampleSessionSemantics semantics = new SampleSessionSemantics(loadSqlMode,
                Map.of(SessionVariable.TIME_ZONE, "Asia/Shanghai"));

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 137, /*loadTimeZone=*/ null, semantics);

        InOrder inOrder = inOrder(context, sessionVariable);
        inOrder.verify(context).setCurrentWarehouseId(5L);
        inOrder.verify(context).setCurrentComputeResource(computeResource);
        inOrder.verify(sessionVariable).setSqlMode(loadSqlMode);
        inOrder.verify(sessionVariable).setTimeZone("Asia/Shanghai");
        inOrder.verify(sessionVariable).setQueryTimeoutS(137);
    }

    // ---------------------------------------------------------------------------
    // Scanned-bytes-drive-the-rate / Estimates-keep-the-total tests.
    // ---------------------------------------------------------------------------

    @Test
    void partitionProjectionReusesAnIdentifierCaseInsensitivelyButAnExpressionOnlyExactly() {
        // Column names are case-insensitive, so `k` reuses `K`; a string literal is data, so
        // concat('A', r) must get its own cell rather than read concat('a', r)'s value.
        String sql = AbstractSqlSampleSubqueryExecutor.buildSampleSql("t", null,
                List.of("`K`", "CAST(concat('a', `r`) AS varchar(8))"),
                List.of("`k`", "CAST(concat('A', `r`) AS varchar(8))", "CAST(concat('a', `r`) AS varchar(8))"),
                1.0, 10, 0L);

        Assertions.assertTrue(sql.startsWith("SELECT `K`, CAST(concat('a', `r`) AS varchar(8)), "
                + "CAST(concat('A', `r`) AS varchar(8)) FROM t WHERE"), sql);
    }

    @Test
    void longQuotedIdentifierCanBeReusedWithoutRegexRecursion() {
        String lower = "`" + "a".repeat(20_000) + "``b`";
        String upper = "`" + "A".repeat(20_000) + "``B`";

        String sql = AbstractSqlSampleSubqueryExecutor.buildSampleSql("t", null,
                List.of(lower), List.of(upper), 1.0, 10, 0L);

        Assertions.assertTrue(sql.startsWith("SELECT " + lower + " FROM t WHERE"));
    }

    @Test
    void samplingRateFollowsTheScannedBytesWhileEstimatesKeepTheWholeInput() throws Exception {
        long totalBytes = 100L << 30;
        long scannedBytes = 1L << 30;
        List<Estimates.PartitionSourceBytes> breakdown = List.of(new Estimates.PartitionSourceBytes(
                List.of(Variant.of(VarcharType.VARCHAR, "a")), totalBytes));
        StringBuilder capturedSql = new StringBuilder();
        AbstractSqlSampleSubqueryExecutor executor = fixedSpecExecutor(
                new AbstractSqlSampleSubqueryExecutor.SampleSpec("t", null, totalBytes, mock(ComputeResource.class),
                        List.of("`k`"), List.of(), List.of(PresplitTestSupport.bigintColumn("k")), List.of(), 0L, false,
                        scannedBytes, breakdown, SampleSessionSemantics.NONE),
                capturedSql);

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(new SampleRequest(
                PresplitTestSupport.DUMMY_CONTEXT, List.of(PresplitTestSupport.bigintColumn("k")), Long.MAX_VALUE, 0L));

        Assertions.assertTrue(capturedSql.toString().contains("rand(0) < "
                        + AbstractSqlSampleSubqueryExecutor.pickSamplingRate(scannedBytes) + " ORDER BY"),
                "the rate targets the bytes the sample reads: " + capturedSql);
        Assertions.assertEquals(totalBytes, execution.estimates().totalBytes(), "tablet sizing sees every file");
        Assertions.assertEquals(breakdown, execution.estimates().partitionSourceBytes());
    }

    @Test
    void existingSpecConstructorsScanTheWholeInput() {
        AbstractSqlSampleSubqueryExecutor.SampleSpec spec = new AbstractSqlSampleSubqueryExecutor.SampleSpec(
                "t", null, 42L, mock(ComputeResource.class), List.of("`k`"), List.of(),
                List.of(PresplitTestSupport.bigintColumn("k")), List.of());

        Assertions.assertEquals(42L, spec.scannedInputBytes());
        Assertions.assertTrue(spec.partitionSourceBytes().isEmpty());
    }

    @Test
    void theFilteredInputEstimateCannotBeCombinedWithAFileSubset() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new AbstractSqlSampleSubqueryExecutor.SampleSpec(
                "t", "k > 1", 100L, mock(ComputeResource.class), List.of("`k`"), List.of(),
                List.of(PresplitTestSupport.bigintColumn("k")), List.of(), 10L, true, 50L, List.of(),
                SampleSessionSemantics.NONE));
    }

    private static AbstractSqlSampleSubqueryExecutor fixedSpecExecutor(
            AbstractSqlSampleSubqueryExecutor.SampleSpec spec, StringBuilder capturedSql) {
        return new AbstractSqlSampleSubqueryExecutor("test ", (sql, computeResource, ignoredTimeout) -> {
            capturedSql.append(sql);
            return List.of();
        }) {
            @Override
            protected SampleSpec resolveSampleSpec(SampleRequest request) {
                return spec;
            }
        };
    }
}
