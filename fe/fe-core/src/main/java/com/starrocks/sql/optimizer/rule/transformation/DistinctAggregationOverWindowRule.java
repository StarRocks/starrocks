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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.math3.util.Pair;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
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

        // Two windows are commutative means the their orders can be permuted, so there exists a common base
        // OptExpression that all the commutative windows only use column refs generated by this base OptExpression.
        public List<OptExpression> getCommutativeWindows(OptExpression optExpression) {
            List<OptExpression> commutativeWindows = Lists.newArrayList();
            for (OptExpression opt = optExpression; opt.getInputs().size() == 1; opt = opt.inputAt(0)) {
                if (opt.getOp() instanceof LogicalWindowOperator &&
                        isOverallWindow((LogicalWindowOperator) opt.getOp())) {
                    commutativeWindows.add(opt);
                } else {
                    break;
                }
            }

            if (commutativeWindows.isEmpty()) {
                return commutativeWindows;
            }
            
            OptExpression lastWindowOpt = commutativeWindows.get(commutativeWindows.size() - 1);
            OptExpression baseOp = lastWindowOpt.inputAt(0);
            ColumnRefSet baseColRefSet = baseOp.getRowOutputInfo().getOutputColumnRefSet();
            int i = 0;
            for (; i < commutativeWindows.size(); ++i) {
                OptExpression opt = commutativeWindows.get(i);
                LogicalWindowOperator windowOp = opt.getOp().cast();
                boolean onlyUsedBaseColRefs = windowOp.getPartitionExpressions().stream()
                        .allMatch(expr -> baseColRefSet.containsAll(expr.getUsedColumns()));

                onlyUsedBaseColRefs &= windowOp.getOrderByElements().stream()
                        .allMatch(ordering -> baseColRefSet.contains(ordering.getColumnRef()));

                onlyUsedBaseColRefs &= windowOp.getWindowCall().values().stream()
                        .allMatch(call -> baseColRefSet.containsAll(call.getUsedColumns()));

                ColumnRefSet colRefSet = new ColumnRefSet(windowOp.getWindowCall().keySet());
                colRefSet.union(baseColRefSet);

                onlyUsedBaseColRefs &= Optional.ofNullable(windowOp.getProjection())
                        .map(Projection::getColumnRefMap)
                        .map(Map::values)
                        .map(exprs ->
                                exprs.stream().allMatch(e -> colRefSet.containsAll(e.getUsedColumns())))
                        .orElse(true);
                onlyUsedBaseColRefs &= Optional.ofNullable(windowOp.getPredicate())
                        .map(expr -> colRefSet.containsAll(expr.getUsedColumns()))
                        .orElse(true);
                if (!onlyUsedBaseColRefs) {
                    break;
                }

                Map<Integer, Map<ColumnRefOperator, CallOperator>> windowCallGroups =
                        windowOp.getWindowCall().entrySet().stream()
                                .collect(Collectors.groupingBy(this::classifyWindowCall,
                                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                Map<ColumnRefOperator, CallOperator> analyticCalls =
                        windowCallGroups.getOrDefault(0, Maps.newHashMap());
                Map<ColumnRefOperator, CallOperator> distinctAggCalls =
                        windowCallGroups.getOrDefault(1, Maps.newHashMap());
                if (!analyticCalls.isEmpty() || distinctAggCalls.isEmpty()) {
                    break;
                }
            }
            return commutativeWindows.subList(0, i);
        }

        private OptExpression createNullSafeEqualJoin(OptExpression optExpression,
                                                      OptExpression childOpt,
                                                      OptExpression baseOpt,
                                                      Map<ColumnRefOperator, ColumnRefOperator> producerColMap,
                                                      int cteId,
                                                      TaskContext context) {
            LogicalWindowOperator windowOp = optExpression.getOp().cast();

            Map<Integer, Map<ColumnRefOperator, CallOperator>> windowCallGroups =
                    windowOp.getWindowCall().entrySet().stream().collect(Collectors.groupingBy(this::classifyWindowCall,
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            Map<ColumnRefOperator, CallOperator> analyticCalls = windowCallGroups.getOrDefault(0, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> distinctAggCalls = windowCallGroups.getOrDefault(1, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> aggCalls = windowCallGroups.getOrDefault(2, Maps.newHashMap());
            if (!analyticCalls.isEmpty()) {
                return OptExpression.builder().with(optExpression).setInputs(Lists.newArrayList(childOpt)).build();
            }

            aggCalls.putAll(distinctAggCalls);
            List<ColumnRefOperator> groupBy = Optional.ofNullable(windowOp.getPartitionExpressions())
                    .map(exprs -> exprs.stream().map(e -> (ColumnRefOperator) e).collect(Collectors.toList()))
                    .orElse(List.of());
            OptimizerContext optimizerContext = context.getOptimizerContext();
            OptExpressionDuplicator rhsDuplicator =
                    new OptExpressionDuplicator(optimizerContext.getColumnRefFactory(), optimizerContext);
            OptExpression rhsOpt = rhsDuplicator.duplicate(baseOpt);

            Map<ColumnRefOperator, ColumnRefOperator> rhsCteOutputColumnRefMap =
                    baseOpt.getRowOutputInfo().getColumnOutputInfo().stream()
                            .map(ColumnOutputInfo::getColumnRef)
                            .collect(Collectors.toMap(colRef -> rhsDuplicator.getColumnMapping().get(colRef),
                                    producerColMap::get));
            LogicalCTEConsumeOperator cteConsumeOp = new LogicalCTEConsumeOperator(cteId, rhsCteOutputColumnRefMap);

            Map<ColumnRefOperator, ColumnRefOperator> rhsToLhsMap =
                    baseOpt.getRowOutputInfo().getColumnOutputInfo().stream()
                            .map(ColumnOutputInfo::getColumnRef)
                            .collect(Collectors.toMap(colRef -> rhsDuplicator.getColumnMapping().get(colRef),
                                    Function.identity()));

            Map<ColumnRefOperator, ScalarOperator> replaceMap = rhsToLhsMap.entrySet().stream()
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

            Optional<ScalarOperator> windowPredicate = Optional.ofNullable(windowOp.getPredicate());

            if (groupBy.isEmpty()) {
                joinOp = new LogicalJoinOperator(JoinOperator.CROSS_JOIN, windowPredicate.orElse(null));
            } else {
                List<ScalarOperator> conjuncts = groupBy.stream()
                        .map(expr -> BinaryPredicateOperator.null_safe_eq(expr, rhsToLhsMap.get(expr)))
                        .collect(Collectors.toList());
                windowPredicate.ifPresent(conjuncts::add);
                joinOp = new LogicalJoinOperator(JoinOperator.INNER_JOIN, Utils.compoundAnd(conjuncts));
            }

            Map<ColumnRefOperator, ScalarOperator> joinColRefMap =
                    Stream.concat(childOpt.getRowOutputInfo().getOutputColRefs().stream(), aggCalls.keySet().stream())
                            .collect(Collectors.toMap(Function.identity(), Function.identity()));
            if (windowOp.getProjection() != null) {
                joinOp.setProjection(windowOp.getProjection());
            } else {
                joinOp.setProjection(new Projection(joinColRefMap));
            }

            // When Both sides of Null-safe-equal join reuse CTE via MulticastSink and the CTE is Scan Operator,
            // the performance would regress, since the common ScanNode will retrieve all the columns required by
            // both sides and sending extra columns to both sides, the build sides of HashJoin would pend more
            // time.
            List<OptExpression> cteChildren = Lists.newArrayList();
            if (baseOpt.getOp() instanceof LogicalScanOperator) {
                cteChildren.add(rhsOpt);
            }

            OptExpression joinRhsOpt = OptExpression.create(aggOp, OptExpression.create(cteConsumeOp, cteChildren));
            OptExpression joinOpt = OptExpression.create(joinOp, childOpt, joinRhsOpt);
            return joinOpt;
        }

        private Optional<LogicalWindowOperator> rewriteDistinctAggIfExists(
                OptExpression optExpression, TaskContext context,
                BiFunction<OptExpression, DistinctAggRewriteResult, Boolean> shouldOptimize,
                List<DistinctAggRewriteResult> shouldOptimizedResults) {

            LogicalWindowOperator windowOp = optExpression.getOp().cast();
            Map<Integer, Map<ColumnRefOperator, CallOperator>> windowCallGroups =
                    windowOp.getWindowCall().entrySet().stream().collect(Collectors.groupingBy(this::classifyWindowCall,
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            Map<ColumnRefOperator, CallOperator> analyticCalls = windowCallGroups.getOrDefault(0, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> distinctAggCalls = windowCallGroups.getOrDefault(1, Maps.newHashMap());
            Map<ColumnRefOperator, CallOperator> aggCalls = windowCallGroups.getOrDefault(2, Maps.newHashMap());
            if (distinctAggCalls.isEmpty()) {
                return Optional.empty();
            }
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

            OptExpression child = optExpression.inputAt(0);
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

            Map<ColumnRefOperator, ScalarOperator> fusedResultSubfieldMap = Maps.newHashMap();
            Preconditions.checkState(shouldOptimizedResults != null);
            for (DistinctAggRewriteResult result : results) {
                if (!shouldOptimize.apply(child, result)) {
                    analyticCalls.putAll(result.windowCalls);
                    fusedResultSubfieldMap.putAll(result.outputExprs);
                } else {
                    shouldOptimizedResults.add(result);
                }
            }
            analyticCalls.putAll(aggCalls);
            Optional<LogicalWindowOperator> optNewWindowOp = Optional.empty();

            if (analyticCalls.isEmpty()) {
                return Optional.empty();
            }
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
            return Optional.of(newWindowOp);
        }

        public Optional<OptExpression> handleDistinctAggOverOverallWindow(OptExpression optExpression,
                                                                          TaskContext context) {
            List<OptExpression> windowOpts = getCommutativeWindows(optExpression);
            // if there are no commutative windows, we try to use fused_multi_distinct to rewrite
            // count/avg/sum(distinct) over window.
            if (windowOpts.isEmpty()) {
                return rewriteDistinctAggIfExists(optExpression, context, (childOpt, result) -> false,
                        Lists.newArrayList())
                        .map(newWindowOp -> OptExpression.create(newWindowOp, optExpression.getInputs()));
            }

            OptExpression baseOpt = windowOpts.get(windowOpts.size() - 1).inputAt(0);
            OptimizerContext optimizerContext = context.getOptimizerContext();
            OptExpressionDuplicator produceDuplicator =
                    new OptExpressionDuplicator(optimizerContext.getColumnRefFactory(), optimizerContext);
            OptExpression produceOpt = produceDuplicator.duplicate(baseOpt);
            List<ColumnOutputInfo> outputInfo = baseOpt.getRowOutputInfo().getColumnOutputInfo();

            int cteId = context.getOptimizerContext().getCteContext().getNextCteId();
            Map<ColumnRefOperator, ColumnRefOperator> outputColumnRefMap = outputInfo.stream()
                    .map(ColumnOutputInfo::getColumnRef)
                    .collect(Collectors.toMap(Function.identity(),
                            colRef -> produceDuplicator.getColumnMapping().get(colRef)));

            LogicalCTEConsumeOperator consumeOp = new LogicalCTEConsumeOperator(cteId, outputColumnRefMap);
            List<OptExpression> consumeChildren = Lists.newArrayList();
            if (baseOpt.getOp() instanceof LogicalScanOperator) {
                consumeChildren.add(baseOpt);
            }
            OptExpression childOpt = OptExpression.create(consumeOp, consumeChildren);
            Collections.reverse(windowOpts);
            for (OptExpression windowOpt : windowOpts) {
                childOpt = createNullSafeEqualJoin(windowOpt, childOpt, baseOpt, produceDuplicator.getColumnMapping(),
                        cteId, context);
            }

            LogicalCTEAnchorOperator cteAnchorOp = new LogicalCTEAnchorOperator(cteId);
            LogicalCTEProduceOperator cteProduceOp = new LogicalCTEProduceOperator(cteId);
            OptExpression cteProduceOptExpr = OptExpression.create(cteProduceOp, produceOpt);
            OptExpression cteAnchorOptExpr = OptExpression.create(cteAnchorOp, cteProduceOptExpr, childOpt);
            return Optional.of(cteAnchorOptExpr);
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

        private boolean shouldOptimizeFramedWindow(OptExpression childOpt, List<ColumnRefOperator> groupBy,
                                                   ColumnRefOperator distinctColumnRef, int optimizeOption,
                                                   OptimizerContext optimizerContext) {

            if (optimizeOption >= 1) {
                return true;
            } else if (optimizeOption <= -1) {
                return false;
            } else {
                Utils.calculateStatistics(childOpt, optimizerContext);
                Statistics inputStatistics = childOpt.getStatistics();
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
            int optOption = context.getOptimizerContext().getSessionVariable().getOptimizeDistinctAggOverFramedWindow();
            BiFunction<OptExpression, DistinctAggRewriteResult, Boolean> shouldOptimize = (childOpt, rewriteResult) ->
                    shouldOptimizeFramedWindow(childOpt, groupBy, rewriteResult.distinctColRef, optOption,
                            context.getOptimizerContext());
            List<DistinctAggRewriteResult> needOptimizeResults = Lists.newArrayList();
            Optional<LogicalWindowOperator> optNewWindowOp =
                    rewriteDistinctAggIfExists(optExpression, context, shouldOptimize, needOptimizeResults);

            if (needOptimizeResults.isEmpty()) {
                Preconditions.checkArgument(optNewWindowOp.isPresent());
                OptExpression newOpt = OptExpression.create(optNewWindowOp.get(), child);
                return Optional.of(newOpt);
            }

            List<ColumnRefOperator> inputColumnRefs = child.getRowOutputInfo().getOutputColRefs();
            Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap =
                    inputColumnRefs.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));

            LogicalCTEAnchorOperator cteAnchorOp = new LogicalCTEAnchorOperator(cteId);
            LogicalCTEProduceOperator cteProduceOp = new LogicalCTEProduceOperator(cteId);
            LogicalCTEConsumeOperator joinLhsCteConsumeOp = new LogicalCTEConsumeOperator(cteId, cteOutputColumnRefMap);

            OptExpression newOpt = OptExpression.create(joinLhsCteConsumeOp);

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
