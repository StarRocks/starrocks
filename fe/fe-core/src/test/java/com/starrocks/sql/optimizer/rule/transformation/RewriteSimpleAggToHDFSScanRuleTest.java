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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RewriteSimpleAggToHDFSScanRuleTest {
    private final ColumnRefOperator idColumn = new ColumnRefOperator(1, IntegerType.INT, "id", true);
    private final ColumnRefOperator markerNamedColumn = new ColumnRefOperator(2, IntegerType.INT, "___count___", true);
    private final ColumnRefOperator placeholderNamedColumn =
            new ColumnRefOperator(3, IntegerType.INT, "___COUNT___id", true);

    @Test
    public void testCountOfDataColumnQualifies(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        enableRewrite(context);
        Assertions.assertTrue(RewriteSimpleAggToHDFSScanRule.SCAN_NO_PROJECT.check(countOver(table, idColumn), context));
    }

    @Test
    public void testCountOfColumnNamedLikePlaceholderDoesNotQualify(@Mocked IcebergTable table,
                                                                    @Mocked OptimizerContext context) {
        enableRewrite(context);
        Assertions.assertFalse(
                RewriteSimpleAggToHDFSScanRule.SCAN_NO_PROJECT.check(countOver(table, markerNamedColumn), context));
        Assertions.assertFalse(
                RewriteSimpleAggToHDFSScanRule.SCAN_NO_PROJECT.check(countOver(table, placeholderNamedColumn), context));
    }

    private static void enableRewrite(OptimizerContext context) {
        new Expectations() {
            {
                context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan();
                result = true;
                minTimes = 0;
            }
        };
    }

    private OptExpression countOver(IcebergTable table, ColumnRefOperator counted) {
        Map<ColumnRefOperator, Column> scanColumns = Map.of(
                idColumn, new Column("id", IntegerType.INT),
                markerNamedColumn, new Column("___count___", IntegerType.INT),
                placeholderNamedColumn, new Column("___COUNT___id", IntegerType.INT));
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator(table, scanColumns, Maps.newHashMap(), -1,
                null, TvrTableSnapshot.empty());

        AggregateFunction countFunction = AggregateFunction.createBuiltin(FunctionSet.COUNT,
                Lists.<Type>newArrayList(IntegerType.INT), IntegerType.BIGINT, IntegerType.BIGINT, false, true, false);
        CallOperator count = new CallOperator(FunctionSet.COUNT, IntegerType.BIGINT, List.of(counted), countFunction);
        LogicalAggregationOperator aggregation = new LogicalAggregationOperator(AggType.GLOBAL, List.of(),
                Map.of(new ColumnRefOperator(10, IntegerType.BIGINT, "count", false), count));
        return OptExpression.create(aggregation, OptExpression.create(scan));
    }
}
