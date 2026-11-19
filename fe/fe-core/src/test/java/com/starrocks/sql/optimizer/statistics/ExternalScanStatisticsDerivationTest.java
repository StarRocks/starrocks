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
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.FlussTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFlussScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFlussScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.DeriveStatsTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExternalScanStatisticsDerivationTest {
    enum Connector {
        ICEBERG, DELTA_LAKE, PAIMON, ODPS, FLUSS
    }

    @Mocked
    private IcebergTable iceberg;
    @Mocked
    private DeltaLakeTable delta;
    @Mocked
    private PaimonTable paimon;
    @Mocked
    private OdpsTable odps;
    @Mocked
    private FlussTable fluss;
    @Mocked
    private GlobalStateMgr state;
    @Mocked
    private MetadataMgr metadata;

    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final OptimizerContext optimizer = OptimizerFactory.mockContext(factory);
    private final ColumnRefOperator id = factory.create("id", IntegerType.INT, false);
    private final ColumnRefOperator source = factory.create("source", IntegerType.INT, true);
    private final ColumnRefOperator cast = factory.create("cast", IntegerType.BIGINT, true);
    private final ColumnStatistic known = new ColumnStatistic(1, 100, 0, 4, 100);

    private void baseStatistics(Statistics base) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = state;
                minTimes = 0;
                state.getMetadataMgr();
                result = metadata;
                minTimes = 0;
                metadata.getTableStatistics((OptimizerContext) any, anyString, (Table) any,
                        (Map<ColumnRefOperator, Column>) any, (List<PartitionKey>) any,
                        (ScalarOperator) any, anyLong, (TvrVersionRange) any);
                result = base;
                minTimes = 0;
                metadata.getTableStatistics((OptimizerContext) any, anyString, (Table) any,
                        (Map<ColumnRefOperator, Column>) any, (List<PartitionKey>) any, (ScalarOperator) any);
                result = base;
                minTimes = 0;
            }
        };
        optimizer.setObtainedFromInternalStatistics(true);
    }

    private LogicalScanOperator scan(Connector connector) {
        Column idColumn = new Column("id", IntegerType.INT);
        Column sourceColumn = new Column("source", IntegerType.INT);
        Map<ColumnRefOperator, Column> refs = Map.of(id, idColumn, source, sourceColumn);
        Map<Column, ColumnRefOperator> columns = Map.of(idColumn, id, sourceColumn, source);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.NE, source, ConstantOperator.createInt(1));
        LogicalScanOperator scan = switch (connector) {
            case ICEBERG -> new LogicalIcebergScanOperator(iceberg, refs, columns, -1, predicate);
            case DELTA_LAKE -> new LogicalDeltaLakeScanOperator(delta, refs, columns, -1, predicate);
            case PAIMON -> new LogicalPaimonScanOperator(paimon, refs, columns, -1, predicate);
            case ODPS -> new LogicalOdpsScanOperator(odps, refs, columns, -1, predicate);
            case FLUSS -> new LogicalFlussScanOperator(fluss, refs, columns, -1, predicate);
        };
        scan.setProjection(new Projection(Map.of(id, id, cast, new CastOperator(IntegerType.BIGINT, source))));
        return scan;
    }

    private Operator physical(Connector connector, LogicalScanOperator scan) {
        return switch (connector) {
            case ICEBERG -> new PhysicalIcebergScanOperator((LogicalIcebergScanOperator) scan);
            case DELTA_LAKE -> new PhysicalDeltaLakeScanOperator((LogicalDeltaLakeScanOperator) scan);
            case PAIMON -> new PhysicalPaimonScanOperator((LogicalPaimonScanOperator) scan);
            case ODPS -> new PhysicalOdpsScanOperator((LogicalOdpsScanOperator) scan);
            case FLUSS -> new PhysicalFlussScanOperator((LogicalFlussScanOperator) scan);
        };
    }

    private Statistics derive(OptExpression expression) {
        ExpressionContext context = new ExpressionContext(expression);
        new StatisticsCalculator(context, factory, optimizer).estimatorStats();
        return context.getStatistics();
    }

    @ParameterizedTest
    @EnumSource(Connector.class)
    void memoDerivationDoesNotApplyPredicateTwice(Connector connector) {
        baseStatistics(Statistics.builder().setOutputRowCount(100)
                .addColumnStatistic(id, known).addColumnStatistic(source, known).build());
        OptExpression expression = OptExpression.create(scan(connector));
        expression.deriveLogicalPropertyItself();
        Statistics first = derive(expression);
        assertEquals(99, first.getOutputRowCount(), 0.000001);
        expression.setStatistics(first);

        GroupExpression copied = new Memo().init(expression);
        TaskContext task = new TaskContext(optimizer, PhysicalPropertySet.EMPTY,
                expression.getOutputColumns(), Double.POSITIVE_INFINITY);
        new DeriveStatsTask(task, copied).execute();
        assertEquals(99, copied.getGroup().getStatistics().getOutputRowCount(), 0.000001);
        assertFalse(copied.getGroup().getStatistics().getColumnStatistic(cast).isUnknown());
    }

    @ParameterizedTest
    @EnumSource(Connector.class)
    void logicalAndPhysicalDerivationRecoverProjectionInputs(Connector connector) {
        baseStatistics(Statistics.builder().setOutputRowCount(100)
                .addColumnStatistic(id, known).addColumnStatistic(source, known).build());
        LogicalScanOperator logical = scan(connector);
        for (Operator operator : List.of(logical, physical(connector, logical))) {
            OptExpression expression = OptExpression.create(operator);
            // This is a previous output view: it has the CAST result, but not its source or the
            // predicate input. Neither is a substitute for raw statistics of the current scan.
            expression.setStatistics(Statistics.builder().setOutputRowCount(99)
                    .addColumnStatistic(id, known).addColumnStatistic(cast, known).build());
            Statistics result = derive(expression);
            assertEquals(99, result.getOutputRowCount(), 0.000001);
            assertFalse(result.getColumnStatistic(cast).isUnknown());
            assertTrue(result.getColumnStatistics().containsKey(source));
        }
    }
}
