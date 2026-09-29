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

import com.starrocks.catalog.Variant;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractSqlSampleSubqueryExecutorTest {

    /**
     * Regression guard: {@code ConnectContext.setCurrentWarehouseId} delegates to
     * {@code setCurrentWarehouse}, which REPLACES the session-variable object with a fresh
     * warehouse-defaulted one (re-applying only tracked SET variables). The pre-submit-budget
     * {@code query_timeout} is applied via a direct setter (not a tracked SET), so it must be set
     * AFTER the warehouse switch or it is silently dropped — letting an over-budget sample run to
     * the warehouse/default timeout and block the load past the pre-submit budget.
     */
    @Test
    void queryTimeoutIsAppliedAfterWarehouseSwitchSoItSurvives() {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(4242L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 137);

        // The warehouse switch (which swaps the session variable) MUST precede the timeout setter.
        InOrder inOrder = inOrder(context, sessionVariable);
        inOrder.verify(context).setCurrentWarehouseId(4242L);
        inOrder.verify(sessionVariable).setQueryTimeoutS(137);
    }

    @Test
    void nonPositiveQueryTimeoutLeavesSessionTimeoutUntouched() {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(7L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 0);

        verify(context).setCurrentWarehouseId(7L);
        verify(sessionVariable, never()).setQueryTimeoutS(anyInt());
    }

    // ---------------------------------------------------------------------------
    // Scanned-bytes-drive-the-rate / Estimates-keep-the-total tests.
    // ---------------------------------------------------------------------------

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
                        scannedBytes, breakdown),
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
                List.of(PresplitTestSupport.bigintColumn("k")), List.of(), 10L, true, 50L, List.of()));
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
