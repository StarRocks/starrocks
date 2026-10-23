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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStats;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReorderJoinStatisticsPruningTest {
    @Mocked
    private IcebergTable table;

    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final OptimizerContext optimizer = OptimizerFactory.mockContext(factory);
    private final ColumnRefOperator id = factory.create("id", IntegerType.INT, false);
    private final ColumnRefOperator source = factory.create("source", IntegerType.INT, true);
    private final ColumnRefOperator cast = factory.create("cast", IntegerType.BIGINT, true);
    private final ColumnRefOperator unused = factory.create("unused", IntegerType.INT, true);
    private final ColumnStatistic known = new ColumnStatistic(1, 100, 0, 4, 100);

    private OptExpression scan(ScalarOperator predicate, Map<ColumnRefOperator, ScalarOperator> projection) {
        Column idColumn = new Column("id", IntegerType.INT);
        Column sourceColumn = new Column("source", IntegerType.INT);
        Column unusedColumn = new Column("unused", IntegerType.INT);
        LogicalIcebergScanOperator op = new LogicalIcebergScanOperator(table,
                Map.of(id, idColumn, source, sourceColumn, unused, unusedColumn),
                Map.of(idColumn, id, sourceColumn, source, unusedColumn, unused), -1, predicate);
        op.setProjection(new Projection(projection));
        OptExpression expression = OptExpression.create(op);
        expression.deriveLogicalPropertyItself();
        expression.setStatistics(Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.ANALYZE)
                .setTableRowCountMayInaccurate(true).setShadowColumns(List.of(id))
                .addColumnStatistic(id, known).addColumnStatistic(source, known)
                .addColumnStatistic(cast, known).addColumnStatistic(unused, known)
                .addMultiColumnStatistics(Set.of(id, source), new MultiColumnCombinedStats(100)).build());
        optimizer.setObtainedFromInternalStatistics(true);
        return expression;
    }

    private OptExpression prune(OptExpression expression, ColumnRefOperator... required) {
        return new ReorderJoinRule.OutputColumnsPrune(optimizer)
                .rewrite(expression, new ColumnRefSet(List.of(required)));
    }

    @Test
    void projectionInputsDoNotInflateCostingStatistics() {
        OptExpression original = scan(null,
                Map.of(id, id, cast, new CastOperator(IntegerType.BIGINT, source), unused, unused));
        OptExpression result = prune(original, cast);
        Statistics stats = result.getStatistics();
        assertEquals(Set.of(cast), stats.getColumnStatistics().keySet());
        assertEquals(known.getAverageRowSize(), stats.getAvgRowSize());
        assertEquals(100 * known.getAverageRowSize(), stats.getComputeSize());
        assertEquals(new ColumnRefSet(List.of(cast)), result.getOutputColumns());
        assertEquals(4, original.getStatistics().getColumnStatistics().size());
    }

    @Test
    void preservesStatisticsMetadataWithoutMutatingOtherAlternatives() {
        OptExpression original = scan(null, Map.of(id, id, unused, unused));
        Statistics oldStats = original.getStatistics();
        OptExpression result = prune(original, id);
        Statistics stats = result.getStatistics();
        assertEquals(oldStats.getStatsSource(), stats.getStatsSource());
        assertEquals(oldStats.isTableRowCountMayInaccurate(), stats.isTableRowCountMayInaccurate());
        assertEquals(oldStats.getShadowColumns(), stats.getShadowColumns());
        assertEquals(oldStats.getMultiColumnCombinedStats(), stats.getMultiColumnCombinedStats());
        assertEquals(4, oldStats.getColumnStatistics().size());
        assertEquals(Set.of(id), stats.getColumnStatistics().keySet());
        assertEquals(new ColumnRefSet(List.of(id, unused)), original.getOutputColumns());
    }
}
