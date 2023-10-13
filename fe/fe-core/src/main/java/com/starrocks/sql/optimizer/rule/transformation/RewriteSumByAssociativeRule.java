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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

// For aggregate functions like `sum(expr +/- const)`, we can rewrite it to `sum(expr) +/- const * count()`
// to reduce the overhead of a project calculation.
public class RewriteSumByAssociativeRule extends TransformationRule {
    public RewriteSumByAssociativeRule() {
        super(RuleType.TF_REWRITE_SUM_BY_ASSOCIATIVE_RULE,
                Pattern.create(OperatorType.LOGICAL_AGGR)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableRewriteSumByAssociativeRule()) {
            return false;
        }

        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator preAggProjectOperator = (LogicalProjectOperator) input.getInputs().get(0).getOp();

        FastRewritableChecker fastRewritableChecker =
                new FastRewritableChecker(preAggProjectOperator.getColumnRefMap());

        boolean canRewritable = false;
        for (Map.Entry<ColumnRefOperator, CallOperator> aggregation :
                aggregationOperator.getAggregations().entrySet()) {
            canRewritable |= fastRewritableChecker.isRewritable(aggregation.getValue());
        }

        return canRewritable;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // let's use sum(id_int +1) as an example, suppose id_int is INT
        /*
         * Before
         *
         * LogicalAggregation {type=GLOBAL ,aggregations={28: sum=sum(27: expr)} ,groupKeys=[]}
         * ->  LogicalProjectOperator {projection=[add(cast(1: id_int as bigint(20)), 1)]}
         *
         * After
         *
         * LogicalProjectOperator {projection=[add(30: sum, multiply(31: count, 1))]}
         * -> LogicalAggregation {type=GLOBAL ,aggregations={30: sum=sum(29: id_int), 31: count=count()} ,groupKeys=[]}
         *   ->  LogicalProjectOperator {projection=[add(cast(1: id_int as bigint(20)), 1), 1: id_int]}
         */
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator preAggProjectOperator = (LogicalProjectOperator) input.getInputs().get(0).getOp();

        ColumnRefSet requiredColumns = context.getTaskContext().getRequiredColumns();

        Map<ColumnRefOperator, ScalarOperator> newPreAggProjections = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> newPostAggProjections = Maps.newHashMap();

        AggFunctionRewriter aggFunctionRewriter = new AggFunctionRewriter(
                aggregationOperator.getAggregations(),
                preAggProjectOperator.getColumnRefMap(),
                context.getColumnRefFactory(),
                newPreAggProjections,
                newPostAggProjections);

        for (Map.Entry<ColumnRefOperator, CallOperator> aggregation :
                aggregationOperator.getAggregations().entrySet()) {
            ColumnRefOperator columnRefOperator = aggregation.getKey();
            CallOperator aggFunction = aggregation.getValue();
            newPostAggProjections.put(columnRefOperator, aggFunctionRewriter.rewrite(columnRefOperator, aggFunction));
        }

        if (aggFunctionRewriter.newAggregations.isEmpty()) {
            return Lists.newArrayList();
        }

        requiredColumns.union(newPostAggProjections.keySet());
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        newAggregations.putAll(aggFunctionRewriter.newAggregations);
        newAggregations.putAll(aggFunctionRewriter.reservedAggregations);

        // for high-cardinality group-by agg, more agg functions may introduce additional performance overhead,
        // so we add a restriction here, for the query with group by keys,
        // if the number of agg functions can not be reduced after rewriting, just skip it.
        if (aggregationOperator.getGroupingKeys() != null && !aggregationOperator.getGroupingKeys().isEmpty()) {
            HashSet<CallOperator> uniqueNewAggregations = new HashSet<>(newAggregations.values());
            HashSet<CallOperator> uniqueOldAggregations = new HashSet<>(aggregationOperator.getAggregations().values());
            if (uniqueNewAggregations.size() >= uniqueOldAggregations.size()) {
                return Lists.newArrayList();
            }
        }

        newPreAggProjections.putAll(aggFunctionRewriter.oldPreAggProjections);

        LogicalProjectOperator newPostAggProjectOperator =
                new LogicalProjectOperator(newPostAggProjections);

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(
                aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), newAggregations);
        Preconditions.checkState(aggregationOperator.getProjection() == null,
                "projection in LogicalAggOperator shouldn't be set in logical rewrite phase");

        OptExpression newPreAggProjectOpt = OptExpression.create(new LogicalProjectOperator(newPreAggProjections),
                input.getInputs().get(0).getInputs());

        OptExpression newAggOpt = OptExpression.create(newAggOperator, newPreAggProjectOpt);

        OptExpression newPostAggProjectOpt = OptExpression.create(newPostAggProjectOperator, newAggOpt);
        return Lists.newArrayList(newPostAggProjectOpt);
    }

    // use this to quickly check if we can rewrite the aggregate function
    private static class FastRewritableChecker {
        public Map<ColumnRefOperator, ScalarOperator> preAggProjections;

        public FastRewritableChecker(Map<ColumnRefOperator, ScalarOperator> preAggProjections) {
            this.preAggProjections = preAggProjections;
        }

        public boolean isRewritable(CallOperator aggFunction) {
            if (!aggFunction.isAggregate() || aggFunction.isDistinct() ||
                    !aggFunction.getFnName().equals(FunctionSet.SUM) || aggFunction.getType().isDecimalV2()) {
                return false;
            }
            ScalarOperator aggExpr = aggFunction.getArguments().get(0);
            if (!aggExpr.isColumnRef()) {
                return false;
            }
            ColumnRefOperator aggColumnRef = (ColumnRefOperator) aggExpr;
            ScalarOperator aggColumnExpr = preAggProjections.get(aggColumnRef);
            if (aggColumnExpr.getOpType() != OperatorType.CALL) {
                return false;
            }
            String functionName = ((CallOperator) aggColumnExpr).getFnName();
            if (functionName.equals(FunctionSet.ADD) || functionName.equals(FunctionSet.SUBTRACT)) {
                List<ScalarOperator> arguments = ((CallOperator) aggColumnExpr).getArguments();
                if (arguments.get(0).isConstant() || arguments.get(1).isConstant()) {
                    return true;
                }
            }
            return false;
        }
    }


    private static class AggFunctionRewriter {
        public Map<ColumnRefOperator, CallOperator> oldAggregations;
        public Map<ColumnRefOperator, ScalarOperator> oldPreAggProjections;
        public ColumnRefFactory columnRefFactory;
        public Map<ColumnRefOperator, ScalarOperator> newPostAggProjections;
        public Map<ColumnRefOperator, CallOperator> newAggregations;
        public Map<ColumnRefOperator, ScalarOperator> newPreAggProjections;

        public Map<ColumnRefOperator, CallOperator> reservedAggregations;

        private Map<ScalarOperator, ColumnRefOperator> commonArguments;

        public AggFunctionRewriter(Map<ColumnRefOperator, CallOperator> oldAggregations,
                                   Map<ColumnRefOperator, ScalarOperator> oldPreAggProjections,
                                   ColumnRefFactory columnRefFactory,
                                   Map<ColumnRefOperator, ScalarOperator> newPreAggProjections,
                                   Map<ColumnRefOperator, ScalarOperator> newPostAggProjections) {
            this.oldAggregations = oldAggregations;
            this.oldPreAggProjections = oldPreAggProjections;
            this.columnRefFactory = columnRefFactory;
            this.newPostAggProjections = newPostAggProjections;
            this.newAggregations = Maps.newHashMap();
            this.newPreAggProjections = newPreAggProjections;
            this.reservedAggregations = Maps.newHashMap();
            this.commonArguments = Maps.newHashMap();
        }

        public ScalarOperator rewrite(ColumnRefOperator op, CallOperator aggFunction) {
            if (aggFunction.isAggregate() && !aggFunction.isDistinct()
                    && aggFunction.getFnName().equals(FunctionSet.SUM) &&
                    !aggFunction.getType().isDecimalV2()) {
                ScalarOperator aggExpr = aggFunction.getArguments().get(0);
                if (aggExpr.isColumnRef()) {
                    ColumnRefOperator aggColumnRef = (ColumnRefOperator) aggExpr;
                    ScalarOperator aggColumnExpr = oldPreAggProjections.get(aggColumnRef);
                    if (aggColumnExpr.getOpType() == OperatorType.CALL) {
                        CallOperator callOperator = (CallOperator) aggColumnExpr;
                        String functionName = callOperator.getFnName();
                        if (functionName.equals(FunctionSet.ADD) || functionName.equals(FunctionSet.SUBTRACT)) {
                            List<ScalarOperator> arguments = callOperator.getArguments();
                            Preconditions.checkState(arguments.size() == 2);
                            // there is at least one constant in arguments
                            if (arguments.get(0).isConstant() || arguments.get(1).isConstant()) {
                                // sometimes expr need to be cast before evaluating ADD/SUB operation.
                                // for example, suppose the type of expr is SMALLINT,
                                // sum(expr + 1) will translate to sum(add(cast(expr as INT), 1)),
                                // but this cast is not needed in the sum function after rewriting,
                                // we can remove it and get sum(expr) + count() * 1
                                ScalarOperator newArg0 = rewriteCastForAggExpr(arguments.get(0));
                                ScalarOperator newArg1 = rewriteCastForAggExpr(arguments.get(1));

                                ScalarOperator agg0 = createNewAggFunction(
                                        newArg0, newArg1, aggFunction.getType());
                                ScalarOperator agg1 = createNewAggFunction(
                                        newArg1, newArg0, aggFunction.getType());

                                Function newFn = GlobalStateMgr.getCurrentState().getFunction(
                                        new Function(new FunctionName(functionName),
                                                Lists.newArrayList(agg0.getType(), agg1.getType()),
                                                aggFunction.getType(), false),
                                        Function.CompareMode.IS_IDENTICAL);
                                Preconditions.checkState(newFn != null,
                                        "cannot find function " + functionName);

                                CallOperator newOperator = new CallOperator(functionName,
                                        aggFunction.getType(), Lists.newArrayList(agg0, agg1), newFn);
                                return newOperator;
                            }
                        }
                    }
                }
            }
            reservedAggregations.put(op, oldAggregations.get(op));
            return op;
        }

        private ScalarOperator rewriteCastForAggExpr(ScalarOperator expr) {
            if (expr instanceof CastOperator) {
                CastOperator castOperator = (CastOperator) expr;
                Type toType = castOperator.getType();
                ScalarOperator fromOperator = castOperator.getChild(0);
                Type fromType = fromOperator.getType();
                // if toType is fully compatible with fromType,
                // and we can find related aggregate function signature,
                // just remove the cast
                if (fromType.isFullyCompatible(toType)) {
                    AggregateFunction possibleAggregateFunction = AggregateFunction.createBuiltin(
                            FunctionSet.SUM, Lists.newArrayList(fromType), toType, toType,
                            false, true, false);
                    if (GlobalStateMgr.getCurrentState().getFunction(
                            possibleAggregateFunction, Function.CompareMode.IS_IDENTICAL) != null) {
                        return rewriteCastForAggExpr(fromOperator);
                    }
                }
            }
            return expr;
        }

        private ColumnRefOperator createColumnRefForAggArgument(ScalarOperator arg) {
            ColumnRefOperator newColumnRef;
            if (arg.isColumnRef()) {
                // if arg is a ColumnRef, we needn't create a new one, just make sure it appears in project node
                newColumnRef = (ColumnRefOperator) arg;
                if (!oldPreAggProjections.containsKey(arg)) {
                    newPreAggProjections.put((ColumnRefOperator) arg, arg);
                }
            } else if (commonArguments.containsKey(arg)) {
                // if arg has been created by the previous rewriting, we don't need to create a new one.
                newColumnRef = commonArguments.get(arg);
            } else {
                newColumnRef = columnRefFactory.create(arg, arg.getType(), arg.isNullable());
                newPreAggProjections.put(newColumnRef, arg);
                commonArguments.put(arg, newColumnRef);
            }
            return newColumnRef;
        }

        private ScalarOperator createNewAggFunction(ScalarOperator arg0, ScalarOperator arg1, Type returnType) {
            if (arg0.isConstant()) {
                // generate count(arg1) * arg0
                List<ScalarOperator> countArguments = Lists.newArrayList(createColumnRefForAggArgument(arg1));
                List<Type> argTypes = Lists.newArrayList(arg1.getType());

                AggregateFunction countFunction = AggregateFunction.createBuiltin(
                        FunctionSet.COUNT, argTypes, Type.BIGINT, Type.BIGINT,
                        false, true, true);

                CallOperator newAggFunction = new CallOperator(FunctionSet.COUNT,
                        Type.BIGINT, countArguments, countFunction);

                ColumnRefOperator newAggRef = columnRefFactory.create(
                        newAggFunction, newAggFunction.getType(), false);
                newAggregations.put(newAggRef, newAggFunction);

                // cast countOperator and constOperator to target type
                ScalarOperator countOperator = null;
                ScalarOperator constOperator = null;
                PrimitiveType primitiveType = returnType.getPrimitiveType();
                switch (primitiveType) {
                    case DECIMAL128:
                        countOperator = new CastOperator(
                                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 18, 0),
                                newAggRef, false);
                        int precision = ((ScalarType) arg0.getType()).getScalarPrecision();
                        int scale = ((ScalarType) arg0.getType()).getScalarScale();
                        Type constType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale);
                        constOperator = arg0.isConstantNull() ? ConstantOperator.createNull(constType) :
                                ConstantOperator.createDecimal(new BigDecimal(arg0.toString()), constType);
                        break;
                    case DOUBLE:
                        countOperator = new CastOperator(returnType, newAggRef, false);
                        constOperator = arg0.isConstantNull() ? ConstantOperator.createNull(Type.DOUBLE) :
                                ConstantOperator.createDouble(((ConstantOperator) arg0).getDouble());
                        break;
                    case LARGEINT:
                        countOperator = new CastOperator(returnType, newAggRef, false);
                        constOperator = arg0.isConstantNull() ? ConstantOperator.createNull(Type.LARGEINT) :
                                ConstantOperator.createLargeInt(new BigInteger(arg0.toString()));
                        break;
                    case BIGINT:
                        // count() always return BIGINT, no need to cast
                        countOperator = newAggRef;
                        constOperator = arg0.isConstantNull() ? ConstantOperator.createNull(Type.BIGINT) :
                                ConstantOperator.createBigint(Long.parseLong(arg0.toString()));
                        break;
                    default:
                        Preconditions.checkState(false, "unexpected sum function result type");
                }

                Function multiplyFn = GlobalStateMgr.getCurrentState().getFunction(
                        new Function(new FunctionName(FunctionSet.MULTIPLY),
                                Lists.newArrayList(countOperator.getType(), constOperator.getType()),
                                returnType, false),
                        Function.CompareMode.IS_IDENTICAL);
                Preconditions.checkState(multiplyFn != null,
                        "cannot find function multiply");

                CallOperator newMultiply = new CallOperator(FunctionSet.MULTIPLY, returnType,
                        Lists.newArrayList(
                                countOperator, constOperator), multiplyFn);
                return newMultiply;
            } else {
                // generate sum(arg0)

                ColumnRefOperator newColumnRef = createColumnRefForAggArgument(arg0);
                Type sumFunctionType = returnType;

                if (returnType.isDecimalV3()) {
                    // for decimal type, we should keep the result's scale same as the input's scale
                    int argScale = ((ScalarType) arg0.getType()).getScalarScale();
                    sumFunctionType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, argScale);
                }

                AggregateFunction sumFunction = AggregateFunction.createBuiltin(
                        FunctionSet.SUM, Lists.newArrayList(arg0.getType()), sumFunctionType, sumFunctionType,
                        false, true, false);

                CallOperator newAggFunction = new CallOperator(FunctionSet.SUM,
                        sumFunctionType, Lists.newArrayList(newColumnRef), sumFunction);

                ColumnRefOperator newAggRef = columnRefFactory.create(
                        newAggFunction, newAggFunction.getType(), true);
                newAggregations.put(newAggRef, newAggFunction);
                return newAggRef;
            }
        }

    }
}
