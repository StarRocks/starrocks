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

import com.google.api.client.util.Lists;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.math3.util.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/*
This rule is used to support count/sum/avg(distinct) over range-frame window.
range-frame windows can be categorized into two types:
1. type-1. overall window:  for examples:
```sql
    -- Q1
    select a, b, count(distinct a) over() from t;
    -- Q2
    select a, b, count(distinct a) over(partition by b) from t;
```
2. type-2. framed window: for examples:
```sql
    -- Q3
    select a, b, count(distinct a) over(order by b) from t;
    -- Q4
    select a, b, c, count(distinct a) over(partition by b order by c) from t;
```

both types of distinct aggregations over range-frame windows are supported now.
For Q1, the query is rewritten into agg + cross-join, as following:
```sql
with cte as(
select count(distinct a) cda from t
)
select t.a, t.b, cte.cda from t CROSS JOIN cte;
```
For Q2, the query is rewritten into agg + null-safe-equal-join, as following:

```sql
with cte as(
  select b, count(distinct a) cda
  from t
  group by b
)
select t.a, t.b, cte.cda
from t inner join cte on t.b <=> t.b;
```

for Q3 and Q4, we introduce fused_multi_distinct aggregation function for process count/sum/avg(distinct), when
the distinct aggregations are applied to same window and same column, then its agg state are fused into one state.

fused_multi_distinct prototype as follows
```
fused_multi_distinct_count[_sum][_avg]: Type -> StructType("count": BIGINT[, "sum": sumType][, "avg": avgType);
```
for an example:
```sql
select a, b, c,
    count(distinct c) over(partition by a order by b),
    sum(distinct c) over(partition by a order by b),
    avg(distinct c) over(partition by a order by b),
from t;
```
above query is rewritten into the plan just as follow query shows
```
with cte as(
select a,b,c,
fused_multi_distinct_count_sum_avg(c) over(partition by a order by b) fmdc
from t
)
select a,b,c, fmdc.count, fmdc.sum, fmdc.avg
from cte;
```
So for Q3, it is transformed into

```sql
with cte as (
select a, b, fused_multi_distinct_count(distinct a) over(order by b) fmdc from t
)
select a, b, fmdc.count from cte
```

For Q4, it is transformed into
```
with cte as (
select a, b, c, fused_multi_distinct_count(distinct a) over(partition by b order by c) fmdc from t;
)
select a, b, c, fmdc.count from cte;
```

For framed windows(Q3,Q4), we try to optimize the queries to reduce input rows of window operators when
1. when multi column cardinality of partition-by columns, order-by columns and distinct columns are low;
2. when session variable optimize_distinct_agg_over_framed_window = 1.

when optimization is enabled, Q3 and Q4 would be transformed into

```sql
-- optimized Q3
with cte0 as (
select distinct a,b from t
),
cte1 as (
select b, fused_multi_distinct_count(distinct a) over(order by b) fmdc from cte0
),
cte2 as (
select distinct b, fmdc.count
)

select t.a, t.b, cte2.fmdc.count from t inner join cte2 on t.b <=> cte2.b

-- optimized Q4
with cte0 as (
select distinct a, b, c from t
),
cte1 as (
select b, c, fused_multi_distinct_count(distinct a) over(partition by b order by c) fmdc from cte0
),
cte2 as (
select distinct b, c, fmdc.count
)
select t.a, t.b, t.c, cte2.fmdc.count from t inner join cte2 on t.b <=> cte2.b and t.c <=> cte2.c
```
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
            if (isOverallWindow(windowOp)) {
                return handleDistinctAggOverOverallWindow(optExpression, context);
            } else if (isFramedWindow(windowOp)) {
                return handleDistinctAggOverFramedWindow(optExpression, context);
            } else {
                return Optional.empty();
            }
        }

        int classifyWindowCall(Map.Entry<ColumnRefOperator, CallOperator> call) {
            Set<String> funcNames = ImmutableSet.of(FunctionSet.SUM, FunctionSet.COUNT, FunctionSet.AVG);
            if (FunctionSet.onlyAnalyticUsedFunctions.contains(call.getValue().getFnName())) {
                return 0;
            } else if (call.getValue().isDistinct() && funcNames.contains(call.getValue().getFnName())) {
                return 1;
            } else {
                return 2;
            }
        }

        public Optional<OptExpression> handleDistinctAggOverOverallWindow(OptExpression optExpression,
                                                                          TaskContext context) {
            LogicalWindowOperator windowOp = optExpression.getOp().cast();

            boolean isWindowWithSlidingFrame = windowOp.getAnalyticWindow() != null &&
                    !(windowOp.getAnalyticWindow().getRightBoundary().getBoundaryType().isAbsolutePos() &&
                            windowOp.getAnalyticWindow().getLeftBoundary().getBoundaryType().isAbsolutePos());

            if (isWindowWithSlidingFrame || !windowOp.getOrderByElements().isEmpty()) {
                return Optional.empty();
            }

            Map<Integer, Map<ColumnRefOperator, CallOperator>> windowCallGroups =
                    windowOp.getWindowCall().entrySet().stream().collect(Collectors.groupingBy(this::classifyWindowCall,
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
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

        // non-sliding window is a window contains no order-by clause and both sides of the frame is
        // unbounded.
        public boolean isOverallWindow(LogicalWindowOperator windowOp) {
            AnalyticWindow windowDef = windowOp.getAnalyticWindow();
            return windowOp.getOrderByElements().isEmpty() && (windowDef == null ||
                    (windowDef.getLeftBoundary().getBoundaryType().isAbsolutePos() &&
                            windowDef.getRightBoundary().getBoundaryType().isAbsolutePos()));
        }

        //  sliding-range-frame window is a window contains order-by clause and at least one of sides of frame
        //  is bounded and
        public boolean isFramedWindow(LogicalWindowOperator windowOp) {
            AnalyticWindow windowDef = windowOp.getAnalyticWindow();
            return !windowOp.getOrderByElements().isEmpty() && windowDef != null &&
                    windowDef.getType().equals(AnalyticWindow.Type.RANGE) &&
                    !(windowDef.getLeftBoundary().getBoundaryType().isAbsolutePos() &&
                            windowDef.getRightBoundary().getBoundaryType().isAbsolutePos());
        }

        private static class DistinctAggRewriteResult {
            public final ColumnRefOperator distinctColRef;
            public final Map<ColumnRefOperator, CallOperator> windowCalls;
            public final Map<ColumnRefOperator, ScalarOperator> outputExprs;

            public DistinctAggRewriteResult(ColumnRefOperator inputColumnRef,
                                            Map<ColumnRefOperator, CallOperator> windowCalls,
                                            Map<ColumnRefOperator, ScalarOperator> outputExprs) {
                this.distinctColRef = inputColumnRef;
                this.windowCalls = windowCalls;
                this.outputExprs = outputExprs;
            }
        }

        private List<DistinctAggRewriteResult> rewriteDistinctAgg(Map<ColumnRefOperator, CallOperator> distinctAgg,
                                                                  ColumnRefFactory columnRefFactory) {
            Map<List<ScalarOperator>, List<Pair<ColumnRefOperator, CallOperator>>> sameArgsDistinctAggGroups =
                    distinctAgg.entrySet().stream().map(e -> Pair.create(e.getKey(), e.getValue()))
                            .collect(Collectors.groupingBy(p -> p.getSecond().getArguments()));

            List<DistinctAggRewriteResult> results = Lists.newArrayList();
            for (Map.Entry<List<ScalarOperator>, List<Pair<ColumnRefOperator, CallOperator>>> e :
                    sameArgsDistinctAggGroups.entrySet()) {
                Preconditions.checkArgument(e.getKey().size() == 1 && e.getKey().get(0) instanceof ColumnRefOperator);
                ColumnRefOperator arg = (ColumnRefOperator) e.getKey().get(0);
                List<CallOperator> calls = e.getValue().stream().map(Pair::getSecond).collect(Collectors.toList());
                CallOperator fusedMultiDistinct = ScalarOperatorUtil.buildFusedMultiDistinct(calls, arg);
                ColumnRefOperator fusedMultiDistinctColRef =
                        columnRefFactory.create(fusedMultiDistinct, fusedMultiDistinct.getType(),
                                fusedMultiDistinct.isNullable());

                Map<ColumnRefOperator, ScalarOperator> outputExprs = e.getValue().stream().collect(
                        Collectors.toMap(Pair::getFirst,
                                p -> new SubfieldOperator(fusedMultiDistinctColRef, p.getSecond().getType(),
                                        List.of(p.getValue().getFnName()))));
                Map<ColumnRefOperator, CallOperator> windowCalls = Maps.newHashMap();
                windowCalls.put(fusedMultiDistinctColRef, fusedMultiDistinct);
                results.add(new DistinctAggRewriteResult(arg, windowCalls, outputExprs));
            }
            return results;
        }

        public OptExpression convertSingleDistinctAgg(OptExpression joinLhsOpt, List<ColumnRefOperator> groupBy,
                                                      LogicalWindowOperator windowOp, int cteId,
                                                      List<ColumnRefOperator> cteProduceOutputColumnRef,
                                                      DistinctAggRewriteResult result, TaskContext context) {

            java.util.function.Function<ColumnRefOperator, ColumnRefOperator> forkColumnRef =
                    colRef -> context.getOptimizerContext().getColumnRefFactory()
                            .create(colRef.getName(), colRef.getType(), colRef.isNullable());

            Map<ColumnRefOperator, ColumnRefOperator> cteConsumeOutputColumnRefMap =
                    cteProduceOutputColumnRef.stream().collect(Collectors.toMap(forkColumnRef, Functions.identity()));

            Map<ColumnRefOperator, ScalarOperator> replaceMap = cteConsumeOutputColumnRefMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

            ReplaceColumnRefRewriter replaceRewriter = new ReplaceColumnRefRewriter(replaceMap, false);

            List<ColumnRefOperator> groupByColumnRefs =
                    groupBy.stream().map(replaceRewriter::rewrite).map(expr -> (ColumnRefOperator) expr)
                            .collect(Collectors.toList());

            groupByColumnRefs.add((ColumnRefOperator) replaceRewriter.rewrite(result.distinctColRef));

            LogicalCTEConsumeOperator cteConsumeOp = new LogicalCTEConsumeOperator(cteId, cteConsumeOutputColumnRefMap);
            LogicalAggregationOperator distinctOp1 =
                    new LogicalAggregationOperator(AggType.GLOBAL, groupByColumnRefs, Maps.newHashMap());
            Map<ColumnRefOperator, ScalarOperator> distinctOp1ColRefMap =
                    groupByColumnRefs.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
            distinctOp1.setProjection(new Projection(distinctOp1ColRefMap));

            List<ScalarOperator> newPartitionExprs =
                    windowOp.getPartitionExpressions().stream().map(replaceRewriter::rewrite)
                            .collect(Collectors.toList());

            List<Ordering> newOrderByElements = windowOp.getOrderByElements().stream()
                    .map(ord -> new Ordering((ColumnRefOperator) replaceRewriter.rewrite(ord.getColumnRef()),
                            ord.isAscending(), ord.isNullsFirst())).collect(Collectors.toList());

            List<Ordering> newEnforceSortColumns = windowOp.getEnforceSortColumns().stream()
                    .map(ord -> new Ordering((ColumnRefOperator) replaceRewriter.rewrite(ord.getColumnRef()),
                            ord.isAscending(), ord.isNullsFirst())).collect(Collectors.toList());

            Map<ColumnRefOperator, CallOperator> newWindowCalls = result.windowCalls.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey, e -> (CallOperator) replaceRewriter.rewrite(e.getValue())));

            Map<ColumnRefOperator, ScalarOperator> windowOpColumnRefMap = Maps.newHashMap();
            newPartitionExprs.forEach(e -> windowOpColumnRefMap.put((ColumnRefOperator) e, e));
            newOrderByElements.forEach(e -> windowOpColumnRefMap.put(e.getColumnRef(), e.getColumnRef()));
            windowOpColumnRefMap.putAll(result.outputExprs);

            LogicalWindowOperator newWindowOp =
                    LogicalWindowOperator.builder().setAnalyticWindow(windowOp.getAnalyticWindow().clone())
                            .setPartitionExpressions(newPartitionExprs).setOrderByElements(newOrderByElements)
                            .setEnforceSortColumns(newEnforceSortColumns).setWindowCall(newWindowCalls)
                            .setProjection(new Projection(windowOpColumnRefMap)).build();

            List<ColumnRefOperator> distinctGroupBy =
                    windowOpColumnRefMap.keySet().stream().sorted(Comparator.comparingInt(ColumnRefOperator::getId))
                            .collect(Collectors.toList());

            LogicalAggregationOperator distinctOp2 =
                    new LogicalAggregationOperator(AggType.GLOBAL, distinctGroupBy, Maps.newHashMap());
            Map<ColumnRefOperator, ScalarOperator> distinctOp2ColumnRefMap =
                    distinctGroupBy.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
            distinctOp2.setProjection(new Projection(distinctOp2ColumnRefMap));

            List<ColumnRefOperator> joinColumnRefs = Lists.newArrayList();
            newPartitionExprs.forEach(e -> joinColumnRefs.add((ColumnRefOperator) e));
            newOrderByElements.forEach(e -> joinColumnRefs.add(e.getColumnRef()));

            List<ScalarOperator> eqConjuncts = joinColumnRefs.stream()
                    .map(expr -> BinaryPredicateOperator.null_safe_eq(expr, cteConsumeOutputColumnRefMap.get(expr)))
                    .collect(Collectors.toList());

            LogicalJoinOperator joinOp =
                    new LogicalJoinOperator(JoinOperator.INNER_JOIN, Utils.compoundAnd(eqConjuncts));
            Map<ColumnRefOperator, ScalarOperator> joinOpColumnRefMap =
                    Stream.concat(cteProduceOutputColumnRef.stream(), result.outputExprs.keySet().stream())
                            .collect(Collectors.toMap(Function.identity(), Function.identity()));
            joinOp.setProjection(new Projection(joinOpColumnRefMap));

            OptExpression joinRhsOpt = OptExpression.create(cteConsumeOp);
            joinRhsOpt = OptExpression.create(distinctOp1, joinRhsOpt);
            joinRhsOpt = OptExpression.create(newWindowOp, joinRhsOpt);
            joinRhsOpt = OptExpression.create(distinctOp2, joinRhsOpt);
            return OptExpression.create(joinOp, joinLhsOpt, joinRhsOpt);
        }

        private boolean shouldOptimizeFramedWindow(Statistics inputStatistics, List<ColumnRefOperator> groupBy,
                                                   ColumnRefOperator distinctColumnRef, int optimizeOption) {

            if (optimizeOption >= 1) {
                return true;
            } else if (optimizeOption <= -1) {
                return false;
            } else {
                List<ColumnRefOperator> distinctColumns = Lists.newArrayList(groupBy);
                distinctColumns.add(distinctColumnRef);
                Map<ColumnRefOperator, ColumnStatistic> distinctColumnStatistics = Maps.newHashMap();
                double rowCount = StatisticsCalculator.computeGroupByStatistics(distinctColumns, inputStatistics,
                        distinctColumnStatistics);

                double inputRowCount = inputStatistics.getOutputRowCount();
                inputRowCount = inputRowCount == 0.0 ? 1.0 : inputRowCount;
                return 0 <= rowCount && rowCount <= inputRowCount && (rowCount / inputRowCount <= 0.1);
            }
        }

        public Optional<OptExpression> handleDistinctAggOverFramedWindow(OptExpression optExpression,
                                                                         TaskContext context) {
            LogicalWindowOperator windowOp = optExpression.getOp().cast();

            if (!isFramedWindow(windowOp)) {
                return Optional.empty();
            }

            Map<Integer, Map<ColumnRefOperator, CallOperator>> windowCallGroups =
                    windowOp.getWindowCall().entrySet().stream().collect(Collectors.groupingBy(this::classifyWindowCall,
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            Map<ColumnRefOperator, CallOperator> analyticCalls = windowCallGroups.getOrDefault(0, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> distinctAggCalls = windowCallGroups.getOrDefault(1, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> aggCalls = windowCallGroups.getOrDefault(2, Maps.newHashMap());

            if (distinctAggCalls.isEmpty()) {
                return Optional.empty();
            }

            List<ColumnRefOperator> groupBy = Lists.newArrayList();
            Optional.ofNullable(windowOp.getPartitionExpressions())
                    .ifPresent(exprs -> exprs.stream().map(e -> (ColumnRefOperator) e).forEach(groupBy::add));

            Optional.ofNullable(windowOp.getOrderByElements())
                    .ifPresent(orderings -> orderings.stream().map(Ordering::getColumnRef).forEach(groupBy::add));

            OptExpression child = optExpression.inputAt(0);
            int cteId = context.getOptimizerContext().getCteContext().getNextCteId();

            // when distinct arguments are expr(not ColumnRefOperator), we must push them down to input OptExpression,
            // since distinct arguments may be group-by columns in optimized plan.
            Map<ScalarOperator, ColumnRefOperator> pushDownDistinctArgs = Maps.newHashMap();
            for (CallOperator call : distinctAggCalls.values()) {
                Preconditions.checkArgument(call.getChildren().size() == 1);
                ScalarOperator arg = call.getChild(0);
                if (arg.isColumnRef()) {
                    continue;
                }
                Optional<ColumnRefOperator> optColRef = Optional.ofNullable(pushDownDistinctArgs.get(arg));
                ColumnRefOperator colRef = optColRef.orElseGet(() -> context.getOptimizerContext().getColumnRefFactory()
                        .create(arg, arg.getType(), arg.isNullable()));
                call.setChild(0, colRef);
                if (optColRef.isEmpty()) {
                    pushDownDistinctArgs.put(arg, colRef);
                }
            }

            if (!pushDownDistinctArgs.isEmpty()) {
                Map<ColumnRefOperator, ScalarOperator> childColRefMap =
                        Optional.ofNullable(child.getOp().getProjection())
                                .map(Projection::getColumnRefMap)
                                .orElseGet(() -> child.getRowOutputInfo().getColumnRefMap().entrySet()
                                        .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getKey)));

                pushDownDistinctArgs.forEach((k, v) -> childColRefMap.put(v, k));
                child.getOp().setProjection(new Projection(childColRefMap));
            }

            List<DistinctAggRewriteResult> results =
                    rewriteDistinctAgg(distinctAggCalls, context.getOptimizerContext().getColumnRefFactory());

            List<ColumnRefOperator> inputColumnRefs = child.getRowOutputInfo().getOutputColRefs();
            Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap =
                    inputColumnRefs.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));

            LogicalCTEAnchorOperator cteAnchorOp = new LogicalCTEAnchorOperator(cteId);
            LogicalCTEProduceOperator cteProduceOp = new LogicalCTEProduceOperator(cteId);
            LogicalCTEConsumeOperator joinLhsCteConsumeOp = new LogicalCTEConsumeOperator(cteId, cteOutputColumnRefMap);

            OptExpression newOpt = OptExpression.create(joinLhsCteConsumeOp);
            Map<ColumnRefOperator, ScalarOperator> fusedResultSubfieldMap = Maps.newHashMap();
            int optOption = context.getOptimizerContext().getSessionVariable().getOptimizeDistinctAggOverFramedWindow();
            Utils.calculateStatistics(child, context.getOptimizerContext());
            List<DistinctAggRewriteResult> needOptimizeResults = Lists.newArrayList();
            for (DistinctAggRewriteResult result : results) {
                if (!shouldOptimizeFramedWindow(child.getStatistics(), groupBy, result.distinctColRef, optOption)) {
                    analyticCalls.putAll(result.windowCalls);
                    fusedResultSubfieldMap.putAll(result.outputExprs);
                } else {
                    needOptimizeResults.add(result);
                }
            }
            analyticCalls.putAll(aggCalls);
            Optional<LogicalWindowOperator> optNewWindowOp = Optional.empty();

            if (!analyticCalls.isEmpty()) {
                Map<ColumnRefOperator, ScalarOperator> outputColRefMap = Optional.ofNullable(windowOp.getProjection())
                        .map(Projection::getColumnRefMap)
                        .orElseGet(() -> optExpression.getRowOutputInfo().getColumnRefMap().entrySet()
                                .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getKey)));

                ReplaceColumnRefRewriter colRefRewriter = new ReplaceColumnRefRewriter(fusedResultSubfieldMap);
                Map<ColumnRefOperator, ScalarOperator> newColRefMap = outputColRefMap.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> Optional.ofNullable(fusedResultSubfieldMap.get(e.getKey()))
                                        .orElseGet(() -> colRefRewriter.rewrite(e.getValue()))));

                LogicalWindowOperator newWindowOp = LogicalWindowOperator.builder().withOperator(windowOp)
                        .setProjection(new Projection(newColRefMap)).setWindowCall(analyticCalls).build();
                optNewWindowOp = Optional.of(newWindowOp);
            }

            if (needOptimizeResults.isEmpty()) {
                Preconditions.checkArgument(optNewWindowOp.isPresent());
                return Optional.of(OptExpression.create(optNewWindowOp.get(), child));
            }

            for (DistinctAggRewriteResult result : needOptimizeResults) {
                newOpt = convertSingleDistinctAgg(newOpt, groupBy, windowOp, cteId, inputColumnRefs, result, context);
            }

            if (optNewWindowOp.isEmpty() && windowOp.getProjection() != null) {
                newOpt.getOp().setProjection(windowOp.getProjection());
            }

            OptExpression cteProduceOptExpr = OptExpression.create(cteProduceOp, child);
            OptExpression cteAnchorOptExpr = OptExpression.create(cteAnchorOp, cteProduceOptExpr, newOpt);

            if (optNewWindowOp.isEmpty()) {
                return Optional.of(cteAnchorOptExpr);
            }
            newOpt = optNewWindowOp.map(newWindowOp -> OptExpression.create(newWindowOp, cteAnchorOptExpr))
                    .orElse(cteAnchorOptExpr);
            return Optional.of(newOpt);
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
