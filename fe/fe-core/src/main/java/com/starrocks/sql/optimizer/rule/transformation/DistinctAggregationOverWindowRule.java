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

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/*
Rewrite count(distinct) over window without sliding frames into null-safe-equal join + aggregation
for an example:

select v1,v2,v3,count(distinct v3) over(partition by v1,v2) from t;
can be rewritten into

with cte as(
  select v1, v2, count(distinct v3) cd
  from t0
  group by v1,v2
)
select t0.v1, t0.v2, t0.v3, cte.cd
from t0 inner join cte on t0.v1 <=> cte.v1 and t0.v2 <=> cte.v2;

*/

public class DistinctAggregationOverWindowRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        return INSTANCE.process(root, taskContext).orElse(root);
    }

    private static final Visitor INSTANCE = new Visitor();

    private static class Visitor extends OptExpressionVisitor<Optional<OptExpression>, TaskContext> {
        @Override
        public Optional<OptExpression> visit(OptExpression optExpression, TaskContext context) {
            return Optional.empty();
        }

        @Override
        public Optional<OptExpression> visitLogicalWindow(OptExpression optExpression, TaskContext context) {
            LogicalWindowOperator windowOp = optExpression.getOp().cast();

            boolean isWindowWithSlidingFrame = windowOp.getAnalyticWindow() != null &&
                    !(windowOp.getAnalyticWindow().getRightBoundary().getBoundaryType().isAbsolutePos() &&
                            windowOp.getAnalyticWindow().getLeftBoundary().getBoundaryType().isAbsolutePos());

            if (isWindowWithSlidingFrame || !windowOp.getOrderByElements().isEmpty()) {
                return Optional.empty();
            }
            Set<String> funcNames = ImmutableSet.of(FunctionSet.SUM, FunctionSet.COUNT, FunctionSet.AVG);
            java.util.function.Function<Map.Entry<ColumnRefOperator, CallOperator>, Integer> classify = e -> {
                if (FunctionSet.onlyAnalyticUsedFunctions.contains(e.getValue().getFnName())) {
                    return 0;
                } else if (e.getValue().isDistinct() && funcNames.contains(e.getValue().getFnName())) {
                    return 1;
                } else {
                    return 2;
                }
            };
            Map<Integer, Map<ColumnRefOperator, CallOperator>> windowCallGroups =
                    windowOp.getWindowCall().entrySet().stream().collect(
                            Collectors.groupingBy(classify, Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            Map<ColumnRefOperator, CallOperator> analyticCalls = windowCallGroups.getOrDefault(0, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> distinctAggCalls = windowCallGroups.getOrDefault(1, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> aggCalls = windowCallGroups.getOrDefault(2, Maps.newHashMap());
            if (distinctAggCalls.isEmpty()) {
                return Optional.empty();
            }
            aggCalls.putAll(distinctAggCalls);
            List<ColumnRefOperator> groupBy = Optional.ofNullable(windowOp.getPartitionExpressions())
                    .map(exprs -> exprs.stream().map(e -> (ColumnRefOperator) e).collect(Collectors.toList()))
                    .orElse(List.of());

            OptExpression child = optExpression.inputAt(0);
            int cteId = context.getOptimizerContext().getCteContext().getNextCteId();
            List<ColumnRefOperator> inputColumnRefs = child.getRowOutputInfo().getOutputColRefs();
            java.util.function.Function<ColumnRefOperator, ColumnRefOperator> forkColumnRef =
                    colRef -> context.getOptimizerContext().getColumnRefFactory()
                            .create(colRef.getName(), colRef.getType(), colRef.isNullable());

            Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap =
                    inputColumnRefs.stream().collect(Collectors.toMap(forkColumnRef, Functions.identity()));
            Map<ColumnRefOperator, ColumnRefOperator> joinLhsCteOutputColumnRefMap =
                    inputColumnRefs.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));

            LogicalCTEAnchorOperator cteAnchorOp = new LogicalCTEAnchorOperator(cteId);
            LogicalCTEProduceOperator cteProduceOp = new LogicalCTEProduceOperator(cteId);
            LogicalCTEConsumeOperator cteConsumeOp = new LogicalCTEConsumeOperator(cteId, cteOutputColumnRefMap);

            LogicalCTEConsumeOperator joinLhsCteConsumeOp =
                    new LogicalCTEConsumeOperator(cteId, joinLhsCteOutputColumnRefMap);

            Map<ColumnRefOperator, ScalarOperator> replaceMap = cteOutputColumnRefMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

            ReplaceColumnRefRewriter replaceRewriter = new ReplaceColumnRefRewriter(replaceMap, false);
            groupBy = groupBy.stream().map(expr -> (ColumnRefOperator) replaceRewriter.rewrite(expr))
                    .collect(Collectors.toList());
            aggCalls = aggCalls.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey, e -> (CallOperator) replaceRewriter.rewrite(e.getValue())));

            LogicalAggregationOperator aggOp = new LogicalAggregationOperator(AggType.GLOBAL, groupBy, aggCalls);
            Map<ColumnRefOperator, ScalarOperator> aggColRefMap =
                    Stream.concat(groupBy.stream(), aggCalls.keySet().stream())
                            .collect(Collectors.toMap(Function.identity(), Function.identity()));
            aggOp.setProjection(new Projection(aggColRefMap));

            LogicalJoinOperator joinOp;
            if (groupBy.isEmpty()) {
                joinOp = new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null);
            } else {
                List<ScalarOperator> eqConjuncts = groupBy.stream()
                        .map(expr -> BinaryPredicateOperator.null_safe_eq(expr, cteOutputColumnRefMap.get(expr)))
                        .collect(Collectors.toList());

                joinOp = new LogicalJoinOperator(JoinOperator.INNER_JOIN, Utils.compoundAnd(eqConjuncts));
            }

            Map<ColumnRefOperator, ScalarOperator> joinColRefMap =
                    Stream.concat(joinLhsCteOutputColumnRefMap.keySet().stream(), aggCalls.keySet().stream())
                            .collect(Collectors.toMap(Function.identity(), Function.identity()));

            if (analyticCalls.isEmpty() && windowOp.getProjection() != null) {
                joinOp.setProjection(windowOp.getProjection());
            } else {
                joinOp.setProjection(new Projection(joinColRefMap));
            }
            OptExpression cteProduceOptExpr = OptExpression.create(cteProduceOp, child);
            OptExpression aggOptExpr = OptExpression.create(aggOp, OptExpression.create(cteConsumeOp));
            OptExpression joinLhsOptExpr = OptExpression.create(joinLhsCteConsumeOp);
            OptExpression joinOptExpr = OptExpression.create(joinOp, joinLhsOptExpr, aggOptExpr);
            OptExpression cteAnchorOptExpr = OptExpression.create(cteAnchorOp, cteProduceOptExpr, joinOptExpr);

            if (analyticCalls.isEmpty()) {
                return Optional.of(cteAnchorOptExpr);
            }

            LogicalWindowOperator newWindowOp =
                    LogicalWindowOperator.builder().withOperator(windowOp).setWindowCall(analyticCalls).build();
            OptExpression windowOptExpr = OptExpression.create(newWindowOp, cteAnchorOptExpr);
            return Optional.of(windowOptExpr);
        }

        private Optional<OptExpression> process(OptExpression opt, TaskContext context) {
            for (int i = 0; i < opt.getInputs().size(); ++i) {
                OptExpression input = opt.inputAt(i);
                opt.setChild(i, process(input, context).orElse(input));
            }
            return opt.getOp().accept(this, opt, context);
        }
    }
}
