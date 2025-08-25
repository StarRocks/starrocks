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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.OperatorFunctionChecker;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import com.starrocks.statistic.StatisticUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Rewrite MIN(f(col)) -> f(MIN(col)) and MAX(f(col)) -> f(MAX(col)) when f is monotonic and safe.
 * Correctness:
 * 1. Aggregation must be MIN or MAX.
 * 2. Function must be deterministic and monotonic.
 * 3. NULLs are handled consistently: both forms ignore NULL values.
 * 4. Domain must be valid: inputs must not produce overflow/invalid casts.
 *
 * Initial scope: only supports
 *  - to_datetime(BIGINT)
 *  - from_unixtime(BIGINT)
 */
public class RewriteMinMaxByMonotonicFunctionRule extends TransformationRule {

    private static final ImmutableSet<String> SUPPORTED_FUNCTION_SET = ImmutableSet.of(
            FunctionSet.TO_DATETIME,
            FunctionSet.FROM_UNIXTIME
    );

    public RewriteMinMaxByMonotonicFunctionRule() {
        super(RuleType.TF_REWRITE_MINMAX_BY_MONOTONIC_FUNCTION,
                Pattern.create(OperatorType.LOGICAL_AGGR)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = input.getOp().cast();
        LogicalScanOperator scanOperator = input.inputAt(0).getOp().cast();
        if (!context.getSessionVariable().isCboRewriteMonotonicMinMaxAggregation()) {
            return false;
        }
        // 1. No aggregation
        // 2. No projection
        // 3. HAVING
        if (agg.getAggregations().isEmpty() || agg.getPredicate() != null || scanOperator.getProjection() == null) {
            return false;
        }
        // Fast check: at least one MIN/MAX over a projected monotonic function of a single column
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            CallOperator call = entry.getValue();
            // 1. Min/Max aggregation
            if (!isMinOrMax(call)) {
                continue;
            }
            if (call.getArguments().size() != 1 || !call.getArguments().get(0).isColumnRef()) {
                continue;
            }
            ColumnRefOperator argRef = (ColumnRefOperator) call.getArguments().get(0);
            ScalarOperator projected = scanOperator.getProjection().getColumnRefMap().get(argRef);
            if (projected == null) {
                continue;
            }
            // 2. Monotonic function
            if (!isAllowedMonotonicFunction(projected)) {
                continue;
            }

            // 3. The MinMaxStats exists
            List<ColumnRefOperator> columnRefList = Utils.extractColumnRef(projected);
            if (columnRefList.size() != 1) {
                continue;
            }
            ColumnRefOperator ref = columnRefList.get(0);
            OlapTable table = (OlapTable) scanOperator.getTable();
            Column column = scanOperator.getColRefToColumnMetaMap().get(ref);
            if (column == null) {
                continue;
            }
            final Long lastUpdateTime = StatisticUtils.getTableLastUpdateTimestamp(table);
            Optional<IMinMaxStatsMgr.ColumnMinMax> minMax = IMinMaxStatsMgr.internalInstance()
                    .getStats(new ColumnIdentifier(table.getId(), column.getColumnId()),
                            new StatsVersion(-1, lastUpdateTime));
            if (minMax.isPresent()) {
                return validateArgumentDomain(projected, minMax.get());
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalOlapScanOperator scan = input.inputAt(0).getOp().cast();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        Map<ColumnRefOperator, ScalarOperator> oldPreProj = scan.getProjection().getColumnRefMap();
        Map<ColumnRefOperator, ScalarOperator> newPreProj = Maps.newHashMap(oldPreProj);
        Map<ColumnRefOperator, CallOperator> newAggs = Maps.newHashMap();
        // Collect rewritten outputs and their reapplied expressions first.
        Map<ColumnRefOperator, ScalarOperator> rewrittenPostProj = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> postProj = Maps.newHashMap();

        boolean rewroteAny = false;

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            ColumnRefOperator outAggRef = entry.getKey();
            CallOperator outerAgg = entry.getValue();

            if (!isMinOrMax(outerAgg) || outerAgg.getArguments().size() != 1 || !outerAgg.getArguments().get(0).isColumnRef()) {
                // Preserve as-is
                newAggs.put(outAggRef, outerAgg);
                continue;
            }

            ColumnRefOperator projectedRef = (ColumnRefOperator) outerAgg.getArguments().get(0);
            ScalarOperator projectedExpr = oldPreProj.get(projectedRef);
            if (projectedExpr == null || !isAllowedMonotonicFunction(projectedExpr)) {
                // Not eligible, keep as-is
                newAggs.put(outAggRef, outerAgg);
                continue;
            }

            ScalarOperator innerArg;
            FunctionWithChild fnWithChild = extractFunctionAndChild(projectedExpr);
            if (fnWithChild == null) {
                newAggs.put(outAggRef, outerAgg);
                continue;
            }
            innerArg = fnWithChild.child;
            if (!(innerArg instanceof ColumnRefOperator)) {
                newAggs.put(outAggRef, outerAgg);
                continue;
            }
            ColumnRefOperator innerColRef = (ColumnRefOperator) innerArg;

            // Ensure inner column is projected pre-agg
            if (!newPreProj.containsKey(innerColRef)) {
                newPreProj.put(innerColRef, innerColRef);
            }

            // Build MIN/MAX over innerColRef
            String aggName = outerAgg.getFnName();
            Type innerType = innerColRef.getType();
            AggregateFunction aggFn = AggregateFunction.createBuiltin(aggName,
                    Lists.newArrayList(innerType), innerType, innerType, false, true, false);
            Function resolvedAggFn = GlobalStateMgr.getCurrentState().getFunction(aggFn, Function.CompareMode.IS_IDENTICAL);
            Preconditions.checkState(resolvedAggFn != null, "cannot find aggregate function %s(%s)", aggName, innerType);

            CallOperator newInnerAggCall = new CallOperator(aggName, innerType, List.of(innerColRef), resolvedAggFn);
            ColumnRefOperator newInnerAggRef = columnRefFactory.create(newInnerAggCall, newInnerAggCall.getType(), true);
            newAggs.put(newInnerAggRef, newInnerAggCall);

            // Post-agg: outAggRef := f(newInnerAggRef)
            ScalarOperator reapplied;
            if (fnWithChild.call != null) {
                CallOperator origCall = fnWithChild.call;
                Function fn = origCall.getFunction();
                // rebuild with new child
                reapplied = new CallOperator(origCall.getFnName(), origCall.getType(), List.of(newInnerAggRef), fn);
            } else {
                // should not happen
                newAggs.put(outAggRef, outerAgg);
                continue;
            }
            rewrittenPostProj.put(outAggRef, reapplied);
            rewroteAny = true;
        }

        if (!rewroteAny) {
            return Lists.newArrayList();
        }

        // Preserve existing projection entries for outputs that were not rewritten
        if (agg.getProjection() != null && agg.getProjection().getColumnRefMap() != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : agg.getProjection().getColumnRefMap().entrySet()) {
                if (!rewrittenPostProj.containsKey(e.getKey())) {
                    postProj.put(e.getKey(), e.getValue());
                }
            }
        }
        // Add rewritten outputs
        postProj.putAll(rewrittenPostProj);

        // Build the new expression
        LogicalOlapScanOperator newScan =
                new LogicalOlapScanOperator.Builder()
                        .withOperator(scan).setProjection(new Projection(newPreProj))
                        .build();
        LogicalAggregationOperator newAgg =
                new LogicalAggregationOperator.Builder()
                        .withOperator(agg)
                        .setAggregations(newAggs)
                        .setProjection(new Projection(postProj)).build();

        OptExpression newScanExpr =
                OptExpression.builder().with(input.inputAt(0)).setOp(newScan).build();
        OptExpression newAggOpt =
                OptExpression.builder().with(input).setOp(newAgg).setInputs(List.of(newScanExpr)).build();
        return Lists.newArrayList(newAggOpt);
    }

    private static boolean isMinOrMax(CallOperator call) {
        String name = call.getFnName();
        return Objects.equals(name, FunctionSet.MIN) || Objects.equals(name, FunctionSet.MAX);
    }

    private static boolean isAllowedMonotonicFunction(ScalarOperator op) {
        if (op instanceof CallOperator call) {
            if (!OperatorFunctionChecker.onlyContainMonotonicFunctions(call).first) {
                return false;
            }
            if (!SUPPORTED_FUNCTION_SET.contains(call.getFnName().toLowerCase())) {
                return false;
            }
            // require one child which is column
            return !call.getChildren().isEmpty() && call.getChild(0).isColumnRef();
        }
        return false;
    }

    private static boolean validateArgumentDomain(ScalarOperator projected, IMinMaxStatsMgr.ColumnMinMax minMax) {
        FunctionWithChild fnWithChild = extractFunctionAndChild(projected);
        String functionName = fnWithChild.call.getFunction().functionName();
        if (functionName.equalsIgnoreCase(FunctionSet.FROM_UNIXTIME) ||
                functionName.equalsIgnoreCase(FunctionSet.TO_DATETIME)) {
            long minValue = Long.parseLong(minMax.minValue());
            long maxValue = Long.parseLong(minMax.maxValue());
            ConstantOperator scale = ConstantOperator.createInt(0);
            if (fnWithChild.call.getArguments().size() > 1) {
                ScalarOperator arg0 = fnWithChild.call.getArguments().get(1);
                if (!(arg0 instanceof ConstantOperator)) {
                    return false;
                }
                scale = (ConstantOperator) arg0;
            }
            if (ScalarOperatorFunctions.toDatetime(ConstantOperator.createBigint(minValue), scale).isNull()) {
                return false;
            }
            if (ScalarOperatorFunctions.toDatetime(ConstantOperator.createBigint(maxValue), scale).isNull()) {
                return false;
            }

            return true;
        }
        return false;
    }

    private static FunctionWithChild extractFunctionAndChild(ScalarOperator op) {
        if (op instanceof CallOperator call && !call.getChildren().isEmpty()) {
            return new FunctionWithChild(call, call.getChild(0));
        }
        return null;
    }

    private record FunctionWithChild(CallOperator call, ScalarOperator child) {
    }
}