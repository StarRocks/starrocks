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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.jsonResultBatch;
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

    /**
     * The sample sub-query must read the BASE index, not a sibling rollup. Both async and sync
     * MV/rollup rewrite must be disabled, and (like the timeout) applied AFTER the warehouse switch that
     * re-clones the session variable — otherwise a coarser sibling rollup could be sampled once the
     * table carries rollups, skewing the tablet boundaries.
     */
    @Test
    void materializedViewRewriteDisabledAfterWarehouseSwitchSoItSurvives() {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(9L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 0);

        InOrder inOrder = inOrder(context, sessionVariable);
        inOrder.verify(context).setCurrentWarehouseId(9L);
        inOrder.verify(sessionVariable).setEnableMaterializedViewRewrite(false);
        inOrder.verify(sessionVariable).setEnableSyncMaterializedViewRewrite(false);
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
    // Secondary-index (multi-index) projection + decode tests. Driven through
    // InternalPartitionSampleSubqueryExecutor -- a concrete AbstractSqlSampleSubqueryExecutor
    // subclass in this package -- since these seams (buildSampleSql's secondary slice,
    // decodeRow's cumulative-arity secondary decode) are shared scaffolding exercised
    // via execute(), not overridden per subclass.
    // ---------------------------------------------------------------------------

    @Test
    void projectsSecondaryIndexColumns() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });
        SecondaryIndexSpec rollupOne = new SecondaryIndexSpec(101L, List.of(bigintColumn("r1")));
        SecondaryIndexSpec rollupTwo = new SecondaryIndexSpec(102L, List.of(bigintColumn("r2a"), bigintColumn("r2b")));

        executor.execute(partitionRequest("db", "tbl", "p1",
                List.of("k"), List.of(rollupOne, rollupTwo), List.of("dt"), 0L,
                List.of(bigintColumn("k")), List.of(bigintColumn("dt"))));

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `k`, `r1`, `r2a`, `r2b`, `dt` FROM"),
                "projection must be sort-key, then each secondary spec's sort key in spec order, "
                        + "then partition-source: " + capturedSql);
    }

    @Test
    void decodesSecondaryTuplesTaggedByMetaId() throws Exception {
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of(
                        jsonResultBatch("{\"data\":[\"1\", \"10\", \"20\", \"21\", \"99\"]}")));
        SecondaryIndexSpec rollupOne = new SecondaryIndexSpec(101L, List.of(bigintColumn("r1")));
        SecondaryIndexSpec rollupTwo = new SecondaryIndexSpec(102L, List.of(bigintColumn("r2a"), bigintColumn("r2b")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(partitionRequest(
                "db", "tbl", "p1", List.of("k"), List.of(rollupOne, rollupTwo), List.of("dt"), 0L,
                List.of(bigintColumn("k")), List.of(bigintColumn("dt"))));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(1, rows.size());
        SampleRow row = rows.get(0);
        Assertions.assertEquals("1", row.sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("99", row.partitionSourceTuple().get(0).getStringValue());
        Assertions.assertEquals(2, row.secondaryIndexTuples().size());
        Assertions.assertEquals(101L, row.secondaryIndexTuples().get(0).indexMetaId());
        Assertions.assertEquals(1, row.secondaryIndexTuples().get(0).values().size());
        Assertions.assertEquals("10", row.secondaryIndexTuples().get(0).values().get(0).getStringValue());
        Assertions.assertEquals(102L, row.secondaryIndexTuples().get(1).indexMetaId());
        Assertions.assertEquals(2, row.secondaryIndexTuples().get(1).values().size());
        Assertions.assertEquals("20", row.secondaryIndexTuples().get(1).values().get(0).getStringValue());
        Assertions.assertEquals("21", row.secondaryIndexTuples().get(1).values().get(1).getStringValue());
    }

    @Test
    void emptySecondaryProjectionAndDecodeUnchanged() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch("{\"data\":[\"10\", \"20\"]}"));
                });

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(partitionRequest(
                "db", "tbl", "p1", List.of("k"), List.of(), List.of("dt"), 0L,
                List.of(bigintColumn("k")), List.of(bigintColumn("dt"))));

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `k`, `dt` FROM"),
                "empty secondary specs must not alter the projection: " + capturedSql);
        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(1, rows.size());
        Assertions.assertTrue(rows.get(0).secondaryIndexTuples().isEmpty(),
                "no secondary specs -> empty secondaryIndexTuples, never null");
        Assertions.assertEquals("10", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("20", rows.get(0).partitionSourceTuple().get(0).getStringValue());
    }

    private static SampleRequest partitionRequest(
            String dbName,
            String tableName,
            String partitionName,
            List<String> sortKeySourceColumnNames,
            List<SecondaryIndexSpec> secondaryIndexSortKeys,
            List<String> partitionSourceColumnNames,
            long partitionSizeBytes,
            List<Column> sortKeyColumns,
            List<Column> partitionSourceColumns) {
        ComputeResource computeResource = mock(ComputeResource.class);
        InternalPartitionScanContext scanContext = new InternalPartitionScanContext(
                dbName, tableName, partitionName,
                sortKeySourceColumnNames, partitionSourceColumnNames,
                partitionSizeBytes, computeResource);
        return new SampleRequest(
                scanContext, sortKeyColumns, secondaryIndexSortKeys, partitionSourceColumns,
                /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L);
    }
}
