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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.LanceTable;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.lance.LanceMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class LanceStatisticsCalculatorTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCatalogRoutingAndConnectorStatistics(boolean physical) {
        Fixture fixture = new Fixture(physical, -1);
        Statistics stats = Statistics.builder().setOutputRowCount(120)
                .addColumnStatistic(fixture.ref, fixture.columnStatistic)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        fixture.stub(stats);
        fixture.calculate();
        verify(fixture.metadata).getTableStatistics(eq(fixture.optimizer), eq("lance_stats"), eq(fixture.table),
                anyMap(), isNull(), isNull(), eq(-1L), any(TvrTableSnapshot.class));
        Assertions.assertEquals(120, fixture.result().getOutputRowCount());
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, fixture.result().getStatsSource());
        Assertions.assertEquals(fixture.columnStatistic, fixture.result().getColumnStatistic(fixture.ref));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMetadataDefaultStatistics(boolean physical) {
        Fixture fixture = new Fixture(physical, -1);
        fixture.stub(fixture.defaultStatistics());
        fixture.calculate();
        fixture.assertFallback();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPredicateAppliedToConnectorStatistics(boolean physical) {
        Fixture fixture = new Fixture(physical, -1);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ, fixture.ref, ConstantOperator.createInt(10));
        fixture.scan.setPredicate(predicate);
        Statistics stats = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(fixture.ref, fixture.columnStatistic).build();
        when(fixture.metadata.getTableStatistics(eq(fixture.optimizer), eq("lance_stats"), eq(fixture.table),
                anyMap(), isNull(), eq(predicate), eq(-1L), any(TvrTableSnapshot.class))).thenReturn(stats);
        fixture.calculate();
        Assertions.assertEquals(10, fixture.result().getOutputRowCount(), 0.001);
        Assertions.assertEquals(1000, stats.getOutputRowCount());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testProjectionPreservesConnectorStatistics(boolean physical) {
        Fixture fixture = new Fixture(physical, -1);
        ColumnRefOperator alias = fixture.factory.create("alias", fixture.ref.getType(), true);
        fixture.scan.setProjection(new Projection(Map.of(alias, fixture.ref)));
        fixture.stub(Statistics.builder().setOutputRowCount(120)
                .addColumnStatistic(fixture.ref, fixture.columnStatistic).build());
        fixture.calculate();
        Assertions.assertEquals(120, fixture.result().getOutputRowCount());
        Assertions.assertEquals(fixture.columnStatistic, fixture.result().getColumnStatistic(alias));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFallbackRespectsLimit(boolean physical) {
        long originalRowCount = Config.default_statistics_output_row_count;
        try {
            Config.default_statistics_output_row_count = 100;
            Fixture fixture = new Fixture(physical, 7);
            fixture.stub(fixture.defaultStatistics());
            fixture.calculate();
            Assertions.assertEquals(7, fixture.result().getOutputRowCount());
            Assertions.assertTrue(fixture.result().getColumnStatistic(fixture.ref).isUnknown());
        } finally {
            Config.default_statistics_output_row_count = originalRowCount;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testExistingStatisticsAvoidMetadataLookup(boolean physical) {
        Fixture fixture = new Fixture(physical, -1);
        fixture.expression.setStatistics(Statistics.builder().setOutputRowCount(42)
                .addColumnStatistic(fixture.ref, fixture.columnStatistic).build());
        fixture.calculate();
        Assertions.assertEquals(42, fixture.result().getOutputRowCount());
        verifyNoInteractions(fixture.metadata);
    }

    private static class Fixture {
        private final ColumnRefFactory factory = new ColumnRefFactory();
        private final OptimizerContext optimizer = OptimizerFactory.mockContext(factory);
        private final MetadataMgr metadata = mock(MetadataMgr.class);
        private final LanceMetadata catalog;
        private final LanceTable table;
        private final Operator scan;
        private final ColumnRefOperator ref;
        private final ExpressionContext expression;
        private final ColumnStatistic columnStatistic = new ColumnStatistic(0, 200, 0, 4, 100);

        private Fixture(boolean physical, long limit) {
            catalog = new LanceMetadata("lance_stats", Map.of(
                    "lance.catalog.warehouse", "file:///tmp"));
            table = new LanceTable(1, "rows", List.of(new Column("id", com.starrocks.type.IntegerType.INT, true)),
                    "file:///tmp/rows.lance", "lance_stats");
            Column column = table.getColumn("id");
            ref = factory.create("id", column.getType(), true);
            LogicalLanceScanOperator logical = new LogicalLanceScanOperator(table,
                    Map.of(ref, column), Map.of(column, ref), limit, null);
            scan = physical ? new PhysicalLanceScanOperator(logical) : logical;
            expression = new ExpressionContext(OptExpression.create(scan));
            GlobalStateMgr state = mock(GlobalStateMgr.class);
            StatisticStorage storage = mock(StatisticStorage.class);
            when(state.getMetadataMgr()).thenReturn(metadata);
            when(state.getStatisticStorage()).thenReturn(storage);
            when(storage.getColumnStatistics(eq(table), any())).thenReturn(List.of(ColumnStatistic.unknown()));
            when(storage.getHistogramStatistics(eq(table), any())).thenReturn(Map.of());
            new MockUp<GlobalStateMgr>() {
                @Mock
                public GlobalStateMgr getCurrentState() {
                    return state;
                }
            };
        }

        private void stub(Statistics stats) {
            when(metadata.getTableStatistics(eq(optimizer), eq("lance_stats"), eq(table),
                    anyMap(), isNull(), isNull(), eq(-1L), any(TvrTableSnapshot.class))).thenReturn(stats);
        }

        private Statistics defaultStatistics() {
            return catalog.getTableStatistics(optimizer, table, Map.of(ref, table.getColumn("id")),
                    List.of(), null, -1, null);
        }

        private void calculate() {
            new StatisticsCalculator(expression, factory, optimizer).estimatorStats();
        }

        private Statistics result() {
            return expression.getStatistics();
        }

        private void assertFallback() {
            Assertions.assertEquals(Config.default_statistics_output_row_count, result().getOutputRowCount());
            Assertions.assertEquals(Statistics.StatsSource.NONE, result().getStatsSource());
            Assertions.assertTrue(result().getColumnStatistic(ref).isUnknown());
        }
    }
}
