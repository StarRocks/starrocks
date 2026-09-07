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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RewriteGroupingSetsByCTERuleTest {

    /**
     * The union node built by the rewrite aligns each child's output columns with the union's
     * output columns by position, and the union's output columns follow the origin aggregations'
     * order. Each child used to emit its agg columns in HashMap iteration order, which depends on
     * the column ref ids: when a child's first new agg column id is a multiple of 16, the two agg
     * columns' buckets wrap around and the child emits them reversed, silently swapping the values
     * of e.g. count(*) and count(distinct c) for that grouping set.
     * <p>
     * Sweep the column id offset so the new agg column refs of some child land on every position
     * of a HashMap bucket ring, and assert the positional mapping stays correct for all of them.
     */
    @Test
    public void testChildOutputColumnsAlignWithUnionOutputUnderAnyColumnIdOffset() {
        for (int pad = 0; pad < 32; pad++) {
            ColumnRefFactory factory = new ColumnRefFactory();
            for (int p = 0; p < pad; p++) {
                factory.create("pad", IntegerType.BIGINT, false);
            }
            ColumnRefOperator v1 = factory.create("v1", IntegerType.BIGINT, true);
            ColumnRefOperator v2 = factory.create("v2", IntegerType.BIGINT, true);
            ColumnRefOperator v3 = factory.create("v3", IntegerType.BIGINT, true);
            ColumnRefOperator groupingV1 = factory.create("grouping_v1", IntegerType.BIGINT, false);
            ColumnRefOperator groupingV2 = factory.create("grouping_v2", IntegerType.BIGINT, false);
            ColumnRefOperator cnt = factory.create("count", IntegerType.BIGINT, false);
            ColumnRefOperator uv = factory.create("count_distinct", IntegerType.BIGINT, false);

            // select v1, v2, count(*), count(distinct v3), grouping(v1), grouping(v2)
            // from t group by grouping sets ((v1, v2), (v1), (v2), ())
            LogicalValuesOperator values = new LogicalValuesOperator(ImmutableList.of(v1, v2, v3));
            List<List<ColumnRefOperator>> repeatColumnRefList = ImmutableList.of(
                    ImmutableList.of(v1, v2), ImmutableList.of(v1), ImmutableList.of(v2), ImmutableList.of());
            List<List<Long>> groupingIds = ImmutableList.of(
                    ImmutableList.of(0L, 0L, 1L, 1L),
                    ImmutableList.of(0L, 1L, 0L, 1L));
            LogicalRepeatOperator repeat = new LogicalRepeatOperator(
                    ImmutableList.of(groupingV1, groupingV2), repeatColumnRefList, groupingIds, Maps.newHashMap());

            Map<ColumnRefOperator, CallOperator> aggregations = new LinkedHashMap<>();
            aggregations.put(cnt, new CallOperator("count", IntegerType.BIGINT, ImmutableList.of()));
            aggregations.put(uv, new CallOperator("count", IntegerType.BIGINT,
                    ImmutableList.of((ScalarOperator) v3), null, true));
            LogicalAggregationOperator aggregate = new LogicalAggregationOperator(AggType.GLOBAL,
                    new ArrayList<>(ImmutableList.of(v1, v2, groupingV1, groupingV2)), aggregations);

            OptExpression input = OptExpression.create(aggregate,
                    OptExpression.create(repeat, OptExpression.create(values)));

            OptimizerContext context = OptimizerFactory.mockContext(new ConnectContext(), factory);
            RewriteGroupingSetsByCTERule rule = new RewriteGroupingSetsByCTERule();
            Assertions.assertTrue(rule.check(input, context));
            List<OptExpression> result = rule.transform(input, context);
            Assertions.assertEquals(1, result.size());

            // cte anchor -> (produce, union all)
            OptExpression union = result.get(0).inputAt(1);
            LogicalUnionOperator unionOperator = (LogicalUnionOperator) union.getOp();
            List<ColumnRefOperator> outputColumns = unionOperator.getOutputColumnRefOp();
            List<List<ColumnRefOperator>> childOutputColumns = unionOperator.getChildOutputColumns();
            int cntPos = outputColumns.indexOf(cnt);
            int uvPos = outputColumns.indexOf(uv);
            Assertions.assertTrue(cntPos >= 0 && uvPos >= 0);

            for (int i = 0; i < childOutputColumns.size(); i++) {
                // child: project -> aggregate -> cte consume
                LogicalAggregationOperator childAgg =
                        (LogicalAggregationOperator) union.inputAt(i).inputAt(0).getOp();
                CallOperator cntCall = childAgg.getAggregations().get(childOutputColumns.get(i).get(cntPos));
                CallOperator uvCall = childAgg.getAggregations().get(childOutputColumns.get(i).get(uvPos));
                Assertions.assertNotNull(cntCall, "pad=" + pad + " child=" + i);
                Assertions.assertNotNull(uvCall, "pad=" + pad + " child=" + i);
                Assertions.assertFalse(cntCall.isDistinct(),
                        "pad=" + pad + " child=" + i + ": count(*) slot mapped to a distinct agg");
                Assertions.assertTrue(uvCall.isDistinct(),
                        "pad=" + pad + " child=" + i + ": count(distinct) slot mapped to a non-distinct agg");
            }
        }
    }
}
