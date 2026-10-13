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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TStatisticData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class StatisticExecutorTest extends PlanTestBase {
    @Test
    public void testStatisticsDmlUsesInternalCatalogAndRestoresCaller() throws Exception {
        ConnectContext caller = new ConnectContext();
        caller.setCurrentCatalog("iceberg_catalog");
        caller.setDatabase("iceberg_db");
        ConnectContext internal = StatisticUtils.buildConnectContext();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.doAnswer(invocation -> {
            // The DELETE partition-pruning query resolves its unqualified catalog via the thread context.
            TableName table = new TableName(StatsConstants.STATISTICS_DB_NAME,
                    StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME);
            table.normalization(ConnectContext.get());
            Assertions.assertEquals(internal.getCurrentCatalog(), table.getCatalog());
            // A failed pruning attempt may be recovered by a successful DELETE.
            Assertions.assertThrows(SemanticException.class, () ->
                    ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, "pruning_probe"));
            internal.getState().setOk();
            return null;
        }).when(executor).execute();

        try (var scope = caller.bindScope();
                MockedStatic<StmtExecutor> factory = Mockito.mockStatic(StmtExecutor.class)) {
            factory.when(() -> StmtExecutor.newInternalExecutor(Mockito.same(internal),
                    Mockito.any(StatementBase.class))).thenReturn(executor);
            new StatisticExecutor().dropExternalTableStatistics(internal, "external_table_uuid");
            Mockito.verify(executor).execute();
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.getState().isError());
            Assertions.assertEquals("iceberg_catalog", caller.getCurrentCatalog());
            Assertions.assertEquals("iceberg_db", caller.getDatabase());
        }
    }

    @Test
    public void testStatisticsDmlExceptionRestoresCaller() throws Exception {
        ConnectContext caller = new ConnectContext();
        caller.setCurrentCatalog("iceberg_catalog");
        ConnectContext internal = StatisticUtils.buildConnectContext();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.doAnswer(invocation -> {
            ErrorReport.report(ErrorCode.ERR_BAD_DB_ERROR, "missing_statistics_db");
            throw new IllegalStateException("statistics delete failed");
        }).when(executor).execute();

        try (var scope = caller.bindScope();
                MockedStatic<StmtExecutor> factory = Mockito.mockStatic(StmtExecutor.class)) {
            factory.when(() -> StmtExecutor.newInternalExecutor(Mockito.same(internal),
                    Mockito.any(StatementBase.class))).thenReturn(executor);
            Assertions.assertFalse(new StatisticExecutor().dropTableStatistics(
                    internal, List.of(1L), StatsConstants.AnalyzeType.FULL));
            Mockito.verify(executor).execute();
            Assertions.assertTrue(internal.getState().isError());
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.getState().isError());
            Assertions.assertEquals("iceberg_catalog", caller.getCurrentCatalog());
        }
    }

    @Test
    public void testEmpty() throws Exception {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(db.getId(), olapTable.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        Assertions.assertThrows(SemanticException.class,
                () -> statisticExecutor.queryStatisticSync(
                        StatisticUtils.buildConnectContext(), db.getId(), olapTable.getId(), Lists.newArrayList("foo", "bar")));
    }

    @Test
    public void testDroppedDB() throws Exception {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(db.getId(), 1000, null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        List<TStatisticData> stats = statisticExecutor.queryStatisticSync(
                StatisticUtils.buildConnectContext(), null, 1000L, Lists.newArrayList("foo", "bar"));
        Assertions.assertEquals(0, stats.size());
    }
}
