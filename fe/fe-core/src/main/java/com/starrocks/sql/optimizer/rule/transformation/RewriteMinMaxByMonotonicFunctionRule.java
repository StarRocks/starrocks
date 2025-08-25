package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.OperatorFunctionChecker;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Rewrite MIN(f(col)) -> f(MIN(col)) and MAX(f(col)) -> f(MAX(col)) when f is monotonic and safe.
 *
 * Initial scope: only supports
 *  - to_date(DATETIME)
 *  - CAST(DATETIME AS DATE)
 */
public class RewriteMinMaxByMonotonicFunctionRule extends TransformationRule {

    private static final ImmutableSet<String> SUPPORTED_FUNCTION_SET = ImmutableSet.of(
            FunctionSet.TO_DATETIME,
            FunctionSet.FROM_UNIXTIME
    );

    public RewriteMinMaxByMonotonicFunctionRule() {
        super(RuleType.TF_REWRITE_MINMAX_BY_MONOTONIC_FUNCTION,
                Pattern.create(OperatorType.LOGICAL_AGGR)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator preProject = (LogicalProjectOperator) input.inputAt(0).getOp();
        if (agg.getAggregations().isEmpty()) {
            return false;
        }
        // Fast check: at least one MIN/MAX over a projected monotonic function of a single column
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            CallOperator call = entry.getValue();
            if (!isMinOrMax(call)) {
                continue;
            }
            if (call.getArguments().size() != 1 || !call.getArguments().get(0).isColumnRef()) {
                continue;
            }
            ColumnRefOperator argRef = (ColumnRefOperator) call.getArguments().get(0);
            ScalarOperator projected = preProject.getColumnRefMap().get(argRef);
            if (projected == null) {
                continue;
            }
            if (isAllowedMonotonicFunction(projected)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator preProject = (LogicalProjectOperator) input.inputAt(0).getOp();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        Map<ColumnRefOperator, ScalarOperator> oldPreProj = preProject.getColumnRefMap();
        Map<ColumnRefOperator, ScalarOperator> newPreProj = Maps.newHashMap(oldPreProj);
        Map<ColumnRefOperator, CallOperator> newAggs = Maps.newHashMap();
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
            } else if (fnWithChild.cast != null) {
                CastOperator origCast = fnWithChild.cast;
                reapplied = new CastOperator(origCast.getType(), newInnerAggRef, origCast.isImplicit());
            } else {
                // should not happen
                newAggs.put(outAggRef, outerAgg);
                continue;
            }
            postProj.put(outAggRef, reapplied);
            rewroteAny = true;
        }

        if (!rewroteAny) {
            return Lists.newArrayList();
        }

        // Keep any original post-agg projections if existed (should be null normally in logical phase)
        Preconditions.checkState(agg.getProjection() == null,
                "projection in LogicalAggOperator shouldn't be set in logical rewrite phase");

        LogicalProjectOperator newPreProject = new LogicalProjectOperator(newPreProj);
        LogicalAggregationOperator newAgg = new LogicalAggregationOperator(
                agg.getType(), agg.getGroupingKeys(), newAggs);
        LogicalProjectOperator postProject = new LogicalProjectOperator(postProj);

        OptExpression newPreProjectOpt = OptExpression.create(newPreProject, input.getInputs().get(0).getInputs());
        OptExpression newAggOpt = OptExpression.create(newAgg, newPreProjectOpt);
        OptExpression newPostProjectOpt = OptExpression.create(postProject, newAggOpt);
        return Lists.newArrayList(newPostProjectOpt);
    }

    private static boolean isMinOrMax(CallOperator call) {
        String name = call.getFnName();
        return Objects.equals(name, FunctionSet.MIN) || Objects.equals(name, FunctionSet.MAX);
    }

    private static boolean isAllowedMonotonicFunction(ScalarOperator op) {
        // Allow monotonic to_date(datetime) and CAST(datetime AS DATE)
        if (op instanceof CallOperator) {
            CallOperator call = (CallOperator) op;
            if (!OperatorFunctionChecker.onlyContainMonotonicFunctions(call).first) {
                return false;
            }
            if (!SUPPORTED_FUNCTION_SET.contains(call.getFnName().toLowerCase())) {
                return false;
            }
            // require one child which is column
            return call.getChildren().size() == 1 && call.getChild(0).isColumnRef();
        }
        if (op instanceof CastOperator) {
            CastOperator cast = (CastOperator) op;
            if (!cast.getType().isDate()) {
                return false;
            }
            return cast.getChild(0).isColumnRef();
        }
        return false;
    }

    private static FunctionWithChild extractFunctionAndChild(ScalarOperator op) {
        if (op instanceof CallOperator) {
            CallOperator call = (CallOperator) op;
            if (call.getChildren().size() == 1) {
                return new FunctionWithChild(call, null, call.getChild(0));
            }
        } else if (op instanceof CastOperator) {
            CastOperator cast = (CastOperator) op;
            return new FunctionWithChild(null, cast, cast.getChild(0));
        }
        return null;
    }

    private static class FunctionWithChild {
        final CallOperator call;
        final CastOperator cast;
        final ScalarOperator child;
        FunctionWithChild(CallOperator call, CastOperator cast, ScalarOperator child) {
            this.call = call;
            this.cast = cast;
            this.child = child;
        }
    }
}