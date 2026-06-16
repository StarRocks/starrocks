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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergTopNScanPruneRuleTest {
    private final IcebergTopNScanPruneRule rule = new IcebergTopNScanPruneRule();

    // Build TopN(ORDER BY key [asc] [nullsFirst] LIMIT limit) over an Iceberg scan of {key}.
    private OptExpression topnOverScan(IcebergTable table, ColumnRefOperator key, boolean asc, boolean nullsFirst,
                                       long limit, boolean pushDownAgg) {
        Map<ColumnRefOperator, Column> colMap = new HashMap<>();
        colMap.put(key, new Column(key.getName(), key.getType()));
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator(table, colMap, Maps.newHashMap(), -1, null,
                TvrTableSnapshot.empty());
        List<Ordering> orderings = Lists.newArrayList(new Ordering(key, asc, nullsFirst));
        LogicalTopNOperator topn = new LogicalTopNOperator(orderings, limit, 0);
        if (pushDownAgg) {
            topn.setTopNPushDownAgg();
        }
        return OptExpression.create(topn, OptExpression.create(scan));
    }

    private void mockSession(OptimizerContext context, boolean pruningOn, boolean rfOn) {
        new Expectations() {
            {
                context.getSessionVariable().isEnableIcebergTopnScanPruning();
                result = pruningOn;
                minTimes = 0;
                context.getSessionVariable().getEnableTopNRuntimeFilter();
                result = rfOn;
                minTimes = 0;
            }
        };
    }

    private static ColumnRefOperator col(Type type) {
        return new ColumnRefOperator(1, type, "c", true);
    }

    private static LogicalScanOperator scanOf(OptExpression topn) {
        return (LogicalScanOperator) topn.getInputs().get(0).getOp();
    }

    @Test
    public void firesForIntAscLimit(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        mockSession(context, true, true);
        ColumnRefOperator key = col(IntegerType.INT);
        OptExpression expr = topnOverScan(table, key, true, false, 10, false);

        Assertions.assertTrue(rule.check(expr, context));
        List<OptExpression> out = rule.transform(expr, context);
        Assertions.assertTrue(out.isEmpty());

        LogicalScanOperator scan = scanOf(expr);
        Assertions.assertEquals(key, scan.getScanOptimizeOption().getTopnReorderKey());
        Assertions.assertFalse(scan.getScanOptimizeOption().isTopnReorderDesc());
        Assertions.assertFalse(scan.getScanOptimizeOption().isTopnReorderNullsFirst());
    }

    @Test
    public void firesForDateDescNullsFirst(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        mockSession(context, true, true);
        ColumnRefOperator key = col(DateType.DATE);
        OptExpression expr = topnOverScan(table, key, false, true, 5, false);

        Assertions.assertTrue(rule.check(expr, context));
        rule.transform(expr, context);

        LogicalScanOperator scan = scanOf(expr);
        Assertions.assertEquals(key, scan.getScanOptimizeOption().getTopnReorderKey());
        Assertions.assertTrue(scan.getScanOptimizeOption().isTopnReorderDesc());
        Assertions.assertTrue(scan.getScanOptimizeOption().isTopnReorderNullsFirst());
    }

    @Test
    public void notFiresWhenSessionDisabled(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        mockSession(context, false, true);
        OptExpression expr = topnOverScan(table, col(IntegerType.INT), true, false, 10, false);
        Assertions.assertFalse(rule.check(expr, context));
    }

    @Test
    public void notFiresWithoutLimit(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        mockSession(context, true, true);
        // limit == DEFAULT_LIMIT (-1) => hasLimit() is false.
        OptExpression expr = topnOverScan(table, col(IntegerType.INT), true, false, -1, false);
        Assertions.assertFalse(rule.check(expr, context));
    }

    @Test
    public void notFiresWhenTopNRuntimeFilterDisabled(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        mockSession(context, true, false);
        OptExpression expr = topnOverScan(table, col(IntegerType.INT), true, false, 10, false);
        Assertions.assertFalse(rule.check(expr, context));
    }

    @Test
    public void notFiresForStringKey(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        mockSession(context, true, true);
        OptExpression expr = topnOverScan(table, col(StringType.STRING), true, false, 10, false);
        Assertions.assertFalse(rule.check(expr, context));
    }

    @Test
    public void notFiresForTopNPushDownAgg(@Mocked IcebergTable table, @Mocked OptimizerContext context) {
        mockSession(context, true, true);
        OptExpression expr = topnOverScan(table, col(IntegerType.INT), true, false, 10, true);
        Assertions.assertFalse(rule.check(expr, context));
    }
}
