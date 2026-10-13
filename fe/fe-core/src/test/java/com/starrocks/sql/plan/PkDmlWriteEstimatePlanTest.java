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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.OptimisticVersion;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticsCalcUtils;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

class PkDmlWriteEstimatePlanTest extends PlanTestBase {
    @BeforeAll
    static void setUpDmlWriteEstimateTable() throws Exception {
        // The inherited @BeforeAll initializes PlanTestBase before this table is created.
        starRocksAssert.withTable("CREATE TABLE dml_write_estimate (k bigint, v bigint, pad varchar(4096)) " +
                "PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @Test
    void testEstimatesReachSinkBeforeComplete() throws Exception {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        StatisticStorage originalStorage = state.getStatisticStorage();
        String originalMode = connectContext.getSessionVariable().getPartialUpdateMode();
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "dml_write_estimate");
        table.getPartitions().forEach(partition -> partition.getDefaultPhysicalPartition().updateVisibleVersion(2));
        setTableStatistics(table, 1000);
        try {
            // The test cluster has no real tablet-stat reports. Supply the source cardinality while
            // leaving predicate estimation and output-expression statistics to the real optimizer.
            new MockUp<StatisticsCalcUtils>() {
                @Mock
                public long getTableRowCount(Table table, Operator node, OptimizerContext context) {
                    return 1000;
                }
            };
            state.setStatisticStorage(new StatisticStorage() {
                @Override
                public ColumnStatistic getColumnStatistic(Table table, String column) {
                    return new ColumnStatistic(1, 1000, 0, column.equals("pad") ? 4096 : 8, 1000);
                }

                @Override
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(column -> getColumnStatistic(table, column)).toList();
                }

                @Override
                public void addColumnStatistic(Table table, String column, ColumnStatistic statistic) {
                    // This read-only stub returns fixed statistics without caching updates.
                }
            });
            long[] expected = {4_112_000};
            int[] completed = {0};
            new MockUp<OlapTableSink>() {
                @Mock
                public void complete(Invocation invocation) {
                    OlapTableSink sink = invocation.getInvokedInstance();
                    long estimate = Deencapsulation.getField(sink, "estimatedWriteBytes");
                    assertEquals(expected[0], estimate);
                    completed[0]++;
                    invocation.proceed();
                }
            };
            connectContext.getSessionVariable().setPartialUpdateMode("row");
            planWithRetry("update dml_write_estimate set v = 2 where k > 0");
            expected[0] = 16_000;
            connectContext.getSessionVariable().setPartialUpdateMode("column");
            plan("update dml_write_estimate set v = 2");
            expected[0] = 9_000;
            planWithRetry("delete from dml_write_estimate where k > 0");
            assertEquals(5, completed[0]);
        } finally {
            state.setStatisticStorage(originalStorage);
            connectContext.getSessionVariable().setPartialUpdateMode(originalMode);
            setTableStatistics(table, 0);
        }
    }

    @Test
    void testUnreportedRowCountKeepsUnknownEstimate() throws Exception {
        for (String sql : List.of("update dml_write_estimate set v = 2 where k = 1",
                "delete from dml_write_estimate where k = 1")) {
            OlapTableSink sink = (OlapTableSink) plan(sql).getTopFragment().getSink();
            long estimate = Deencapsulation.getField(sink, "estimatedWriteBytes");
            assertTrue(estimate <= 0);
        }
    }

    @Test
    void testReliableRowCountWithUnknownColumnWidthsReachesSink() throws Exception {
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "dml_write_estimate");
        String originalMode = connectContext.getSessionVariable().getPartialUpdateMode();
        setTableStatistics(table, 1000);
        try {
            new MockUp<StatisticsCalcUtils>() {
                @Mock
                public long getTableRowCount(Table table, Operator node, OptimizerContext context) {
                    return 1000;
                }
            };
            // Keep the default unknown column statistics. Cardinality is supplied by tablet
            // metadata; the optimizer still computes output expressions and the sink is real.
            connectContext.getSessionVariable().setPartialUpdateMode("column");
            OlapTableSink update = (OlapTableSink) plan("update dml_write_estimate set v = 2")
                    .getTopFragment().getSink();
            assertEquals(16_000L, (long) Deencapsulation.getField(update, "estimatedWriteBytes"));
            OlapTableSink delete = (OlapTableSink) plan("delete from dml_write_estimate where k > 0")
                    .getTopFragment().getSink();
            // The unknown-range predicate keeps the optimizer's 0.5 selectivity: size the final
            // 500 rows rather than the table's 1000 rows, with BIGINT key plus TINYINT operation.
            assertEquals(4_500L, (long) Deencapsulation.getField(delete, "estimatedWriteBytes"));
        } finally {
            connectContext.getSessionVariable().setPartialUpdateMode(originalMode);
            setTableStatistics(table, 0);
        }
    }

    private void planWithRetry(String sql) throws Exception {
        boolean originalUseDbLock = connectContext.getSessionVariable().isCboUseDBLock();
        connectContext.getSessionVariable().setCboUseDBLock(false);
        // Purely native DML normally holds the lock. Force the copy-safe path to exercise the same
        // retry loop used when a DML also reads through a connector, without adding an external source.
        try (MockedStatic<AnalyzerUtils> copySafety = Mockito.mockStatic(AnalyzerUtils.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<OptimisticVersion> version =
                        Mockito.mockStatic(OptimisticVersion.class, Mockito.CALLS_REAL_METHODS)) {
            copySafety.when(() -> AnalyzerUtils.areTablesCopySafe(any(StatementBase.class))).thenReturn(true);
            version.when(() -> OptimisticVersion.validateTableUpdate(any(OlapTable.class), anyLong()))
                    .thenReturn(false, true);
            plan(sql);
            version.verify(() -> OptimisticVersion.validateTableUpdate(any(OlapTable.class), anyLong()), Mockito.times(2));
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(originalUseDbLock);
        }
    }

    private ExecPlan plan(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
    }
}
