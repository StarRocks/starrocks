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

package com.starrocks.sql;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DmlWriteEstimatesTest {
    private final ColumnRefOperator key = new ColumnRefOperator(1, IntegerType.BIGINT, "k", false);
    private final ColumnRefOperator value = new ColumnRefOperator(2, IntegerType.BIGINT, "v", true);
    private final ColumnRefOperator untouched = new ColumnRefOperator(3, IntegerType.BIGINT, "pad", true);
    private final ColumnRefOperator op = new ColumnRefOperator(4, IntegerType.TINYINT, "__op", false);

    @Test
    void testSizesOnlyFinalOutputRowsAndColumns() {
        Statistics statistics = statistics(10)
                .addColumnStatistic(key, width(8))
                .addColumnStatistic(value, width(8))
                .addColumnStatistic(untouched, width(4096))
                .addColumnStatistic(op, width(1)).build();
        assertEquals(41_120, estimate(statistics, key, value, untouched));
        assertEquals(160, estimate(statistics, key, value));
        assertEquals(90, estimate(statistics, key, op));
    }

    @Test
    void testIgnoresUnknownColumnsOutsideOutput() {
        Statistics statistics = statistics(100)
                .addColumnStatistic(key, width(8))
                .addColumnStatistic(op, width(1))
                .addColumnStatistic(untouched, ColumnStatistic.unknown()).build();
        assertEquals(900, estimate(statistics, key, op));
    }

    @Test
    void testUnreportedRowCountIsUnknownEvenWithKnownOutputWidths() {
        Statistics statistics = statistics(0).setTableRowCountMayInaccurate(true)
                .addColumnStatistic(key, width(8)).build();
        assertEquals(-1, estimate(statistics, key));
    }

    @Test
    void testUnknownRowStatisticsRemainUnknown() {
        assertEquals(-1, estimate(null, key));
        assertEquals(-1, estimate(statistics(100).setTableRowCountMayInaccurate(true)
                .addColumnStatistic(key, ColumnStatistic.unknown()).build(), key));
        ExecPlan plan = Mockito.mock(ExecPlan.class);
        assertEquals(-1, DmlWriteEstimates.fromExecPlan(plan));
    }

    @Test
    void testReliableRowsUseTypeWidthsForMissingOrUnknownColumnStatistics() {
        assertEquals(800, estimate(statistics(100).build(), key));
        assertEquals(900, estimate(statistics(100)
                .addColumnStatistic(key, ColumnStatistic.unknown()).build(), key, op));
        assertEquals(410_400, estimate(statistics(100)
                .addColumnStatistic(untouched, width(4096)).build(), key, untouched));
    }

    @Test
    void testInvalidStatisticsRemainUnknown() {
        assertEquals(-1, estimate(statistics(Double.NaN).addColumnStatistic(key, width(8)).build(), key));
        for (double width : new double[] {Double.NaN, Double.POSITIVE_INFINITY, -1, 0}) {
            assertEquals(-1, estimate(statistics(100).addColumnStatistic(key, width(width)).build(), key));
        }
    }

    @Test
    void testRoundsUpAndSaturatesInsteadOfOverflowing() {
        assertEquals(9, estimate(statistics(1.1).addColumnStatistic(key, width(8)).build(), key));
        assertEquals(Long.MAX_VALUE,
                estimate(statistics(1_000_000).addColumnStatistic(key, width(1e20)).build(), key));
    }

    private Statistics.Builder statistics(double rows) {
        return Statistics.builder().setOutputRowCount(rows);
    }

    private ColumnStatistic width(double bytes) {
        return new ColumnStatistic(1, 100, 0, bytes, 100);
    }

    private long estimate(Statistics statistics, ColumnRefOperator... output) {
        OptExpression root = Mockito.mock(OptExpression.class);
        Mockito.when(root.getStatistics()).thenReturn(statistics);
        ExecPlan plan = Mockito.mock(ExecPlan.class);
        Mockito.when(plan.getPhysicalPlan()).thenReturn(root);
        Mockito.when(plan.getOutputColumns()).thenReturn(List.of(output));
        return DmlWriteEstimates.fromExecPlan(plan);
    }
}
