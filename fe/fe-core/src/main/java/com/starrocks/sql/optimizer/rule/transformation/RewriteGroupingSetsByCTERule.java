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
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * `RewriteGroupingSetsByCTERule` will rewrite grouping sets to union all. eg:
 *      select a, b, count(1) from t group by rollup(a, b);
 * rewrite to:
 *    with cte1 as (select a, b from t)
 *      select a, b, count(1) from cte1 group by a, b
 *      union all
 *      select a, null, count(1) from cte1 group by a
 *      union all
 *      select null, b, count(1) from cte1 group by b
 *      union all
 *      select null, null, count(1) from cte1
 */
public class RewriteGroupingSetsByCTERule extends TransformationRule {

    public RewriteGroupingSetsByCTERule() {
        super(RuleType.TF_REWRITE_GROUPING_SET,
                Pattern.create(OperatorType.LOGICAL_AGGR)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_REPEAT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();
        LogicalRepeatOperator repeatOperator = (LogicalRepeatOperator) input.inputAt(0).getOp();
        return aggregate.getType() == AggType.GLOBAL && repeatOperator.getRepeatColumnRef().size() > 1;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();
        OptExpression repeatOpt = input.inputAt(0);

        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        int cteId = context.getCteContext().getNextCteId();
        OptExpression cteProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId), repeatOpt.getInputs());

        List<OptExpression> children = new ArrayList<>();
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        LogicalRepeatOperator repeatOperator = (LogicalRepeatOperator) repeatOpt.getOp();
        for (int i = 0; i < repeatOperator.getRepeatColumnRef().size(); i++) {
            // create cte consume, cte output columns
            // output column -> old input column.
            List<ColumnRefOperator> groupingSetKeys = repeatOperator.getRepeatColumnRef().get(i);

            ColumnRefSet allCteConsumeRequiredColumns = new ColumnRefSet();
            aggregate.getAggregations().keySet().stream().map(k -> aggregate.getAggregations().get(k).getUsedColumns())
                    .forEach(allCteConsumeRequiredColumns::union);
            allCteConsumeRequiredColumns.union(groupingSetKeys);
            LogicalCTEConsumeOperator cteConsume = buildCteConsume(cteProduce, allCteConsumeRequiredColumns, columnRefFactory);

            // rewrite aggregate
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
            cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

            // rewrite agg functions by cte consume output
            Map<ColumnRefOperator, CallOperator> newAggregations = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregate.getAggregations().entrySet()) {
                ColumnRefOperator aggColumnRef =
                        columnRefFactory.create(kv.getKey(), kv.getKey().getType(), kv.getKey().isNullable());
                newAggregations.put(aggColumnRef, (CallOperator) rewriter.rewrite(kv.getValue()));
            }

            List<ColumnRefOperator> rewriteGroupingKeys = groupingSetKeys.stream().
                    map(column -> (ColumnRefOperator) rewriter.rewrite(column)).collect(
                            Collectors.toList());
            LogicalAggregationOperator newChildAgg = new LogicalAggregationOperator(AggType.GLOBAL, rewriteGroupingKeys,
                    newAggregations);

            //  add project node above agg
            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
            List<ColumnRefOperator> projectionOutputColumns = Lists.newArrayList();
            for (ColumnRefOperator groupKey : aggregate.getGroupingKeys()) {
                // output grouping keys if exists
                if (groupingSetKeys.contains(groupKey)) {
                    ScalarOperator rewriteGroupKey = rewriter.rewrite(groupKey);
                    newProjectMap.put((ColumnRefOperator) rewriteGroupKey, rewriteGroupKey);
                    projectionOutputColumns.add((ColumnRefOperator) rewriteGroupKey);
                } else {
                    // output null if not in the grouping keys.
                    ColumnRefOperator projectOutputColumn =
                            columnRefFactory.create(groupKey, groupKey.getType(), groupKey.isNullable());
                    newProjectMap.put(projectOutputColumn, ConstantOperator.createNull(groupKey.getType()));
                    projectionOutputColumns.add(projectOutputColumn);
                }
            }
            // output agg functions after grouping keys.
            for (Map.Entry<ColumnRefOperator, CallOperator> kv : newAggregations.entrySet()) {
                newProjectMap.put(kv.getKey(), kv.getKey());
                projectionOutputColumns.add(kv.getKey());
            }
            LogicalProjectOperator projectOperator = new LogicalProjectOperator(newProjectMap);

            // project->aggregate->cte consumer
            children.add(OptExpression.create(projectOperator,
                    OptExpression.create(newChildAgg, OptExpression.create(cteConsume))));

            // collect outputs for union all
            childOutputColumns.add(projectionOutputColumns);
        }

        List<ColumnRefOperator> outputColumns = Lists.newArrayList();
        outputColumns.addAll(aggregate.getGroupingKeys());
        outputColumns.addAll(aggregate.getAggregations().keySet());
        LogicalUnionOperator.Builder unionAllBuilder = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(outputColumns)
                .setChildOutputColumns(childOutputColumns)
                .isUnionAll(true);
        OptExpression rightTree = OptExpression.create(unionAllBuilder.build(), children);

        context.getCteContext().addForceCTE(cteId);

        LogicalCTEAnchorOperator cteAnchor = new LogicalCTEAnchorOperator(cteId);
        return Lists.newArrayList(OptExpression.create(cteAnchor, cteProduce, rightTree));
    }

    private LogicalCTEConsumeOperator buildCteConsume(OptExpression cteProduce, ColumnRefSet requiredColumns,
                                                      ColumnRefFactory factory) {
        LogicalCTEProduceOperator produceOperator = (LogicalCTEProduceOperator) cteProduce.getOp();
        int cteId = produceOperator.getCteId();

        // create cte consume, cte output columns
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputMap = Maps.newHashMap();
        for (int columnId : requiredColumns.getColumnIds()) {
            ColumnRefOperator produceOutput = factory.getColumnRef(columnId);
            ColumnRefOperator consumeOutput =
                    factory.create(produceOutput, produceOutput.getType(), produceOutput.isNullable());
            consumeOutputMap.put(consumeOutput, produceOutput);
        }
        // If there is no requiredColumns, we need to add least one column which is smallest
        if (consumeOutputMap.isEmpty()) {
            List<ColumnRefOperator> outputColumns =
                    produceOperator.getOutputColumns(new ExpressionContext(cteProduce)).getStream().
                            map(factory::getColumnRef).collect(Collectors.toList());
            ColumnRefOperator smallestColumn = Utils.findSmallestColumnRef(outputColumns);
            ColumnRefOperator consumeOutput =
                    factory.create(smallestColumn, smallestColumn.getType(), smallestColumn.isNullable());
            consumeOutputMap.put(consumeOutput, smallestColumn);
        }

        return new LogicalCTEConsumeOperator(cteId, consumeOutputMap);
    }
}
