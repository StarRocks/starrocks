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
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class RewriteSumByAssociativeRule extends TransformationRule {
    public RewriteSumByAssociativeRule() {
        super(RuleType.TF_REWRITE_SUM_BY_ASSOCIATIVE_RULE,
                Pattern.create(OperatorType.LOGICAL_PROJECT)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR)
                                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT))));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        return !context.getSessionVariable().isDisableRewriteSumByAssociativeRule();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // pre-project->agg->post-project
        LogicalProjectOperator postAggProjectOperator = (LogicalProjectOperator) input.getOp();
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getInputs().get(0).getOp();
        LogicalProjectOperator preAggProjectOperator =
                (LogicalProjectOperator) input.getInputs().get(0).getInputs().get(0).getOp();

        // sum(expr + const) -> sum(expr) + const * count(expr)
        ColumnRefSet requiredColumns = context.getTaskContext().getRequiredColumns();

        Map<ColumnRefOperator, ScalarOperator> newPreAggProjections = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> newPostAggProjections = Maps.newHashMap();

        RewriteSumVisitor rewriteSumVisitor = new RewriteSumVisitor(
                aggregationOperator.getAggregations(),
                preAggProjectOperator.getColumnRefMap(),
                context.getColumnRefFactory(),
                newPreAggProjections,
                newPostAggProjections
                );

        for (Map.Entry<ColumnRefOperator, ScalarOperator> postAggProjection :
                postAggProjectOperator.getColumnRefMap().entrySet()) {
            ColumnRefOperator columnRefOperator = postAggProjection.getKey();
            ScalarOperator projectExpr = postAggProjection.getValue();
            ScalarOperator newProjectExpr = projectExpr.accept(rewriteSumVisitor, null);
            newPostAggProjections.put(columnRefOperator, newProjectExpr);
        }

        if (rewriteSumVisitor.newAggregations.isEmpty()) {
            // nothing to rewrite
            return Lists.newArrayList();
        }

        requiredColumns.except(postAggProjectOperator.getColumnRefMap().keySet());
        requiredColumns.union(newPostAggProjections.keySet());

        rewriteSumVisitor.newAggregations.putAll(rewriteSumVisitor.reservedAggregations);
        newPreAggProjections.putAll(rewriteSumVisitor.oldPreAggProjections);

        LogicalProjectOperator newPostAggProjectOperator =
                new LogicalProjectOperator(newPostAggProjections);

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(
                aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), rewriteSumVisitor.newAggregations);
        newAggOperator.setProjection(aggregationOperator.getProjection());

        OptExpression newPreAggProjectOpt = OptExpression.create(new LogicalProjectOperator(newPreAggProjections),
                input.getInputs().get(0).getInputs().get(0).getInputs());
        newPreAggProjectOpt.setLogicalProperty(new LogicalProperty(new ColumnRefSet(newPreAggProjections.keySet())));

        OptExpression newAggOpt = OptExpression.create(newAggOperator, newPreAggProjectOpt);
        newAggOpt.setLogicalProperty(new LogicalProperty(newAggOperator.getOutputColumns(null)));

        OptExpression newPostAggProjectOpt = OptExpression.create(newPostAggProjectOperator, newAggOpt);
        newPostAggProjectOpt.setLogicalProperty(new LogicalProperty(new ColumnRefSet(newPostAggProjections.keySet())));

        return Lists.newArrayList(newPostAggProjectOpt);
    }

    private static class RewriteSumVisitor extends ScalarOperatorVisitor<ScalarOperator, Void> {
        public Map<ColumnRefOperator, CallOperator> oldAggregations;
        public Map<ColumnRefOperator, ScalarOperator> oldPreAggProjections;
        public ColumnRefFactory columnRefFactory;
        public Map<ColumnRefOperator, ScalarOperator> newPostAggProjections;
        public Map<ColumnRefOperator, CallOperator> newAggregations;
        public Map<ColumnRefOperator, ScalarOperator> newPreAggProjections;

        public Map<ColumnRefOperator, CallOperator> reservedAggregations;
        public Map<ColumnRefOperator, ScalarOperator> reservedPreAggProjections;

        private Map<ScalarOperator, ColumnRefOperator> commonArguments;

        public RewriteSumVisitor(Map<ColumnRefOperator, CallOperator> oldAggregations,
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
            this.reservedPreAggProjections = Maps.newHashMap();
            this.commonArguments = Maps.newHashMap();
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            int childrenSize = scalarOperator.getChildren().size();
            for (int i = 0; i < childrenSize; i++) {
                ScalarOperator child = scalarOperator.getChild(i);
                scalarOperator.setChild(i, child.accept(this, null));
            }
            return scalarOperator;
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
                if (ScalarType.isFullyCompatible(fromType, toType)) {
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

        private ScalarOperator createNewAggFunction(ScalarOperator arg0, ScalarOperator arg1, Type returnType) {
            if (arg0.isConstant()) {
                AggregateFunction countFunction = AggregateFunction.createBuiltin(
                        FunctionSet.COUNT, Lists.newArrayList(arg1.getType()), Type.BIGINT, Type.BIGINT,
                        false, true, true);

                // if arg1 is nullable, we should use count(arg1),
                // otherwise, just use count()
                List<ScalarOperator> countArguments = Lists.newArrayList();
                if (arg1.isNullable()) {
                    countArguments.add(arg1);
                }
                CallOperator newAggFunction = new CallOperator("count",
                        Type.BIGINT, countArguments, countFunction);

                ColumnRefOperator newAggRef = columnRefFactory.create(
                        newAggFunction, newAggFunction.getType(), true);
                newAggregations.put(newAggRef, newAggFunction);

                // for decimal type, should give a correct precision and scale
                // for others, just convert?
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
                        constOperator = ConstantOperator.createDecimal(new BigDecimal(arg0.toString()),
                                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale));
                        break;
                    case DOUBLE:
                        countOperator = new CastOperator(returnType, newAggRef, false);
                        constOperator = ConstantOperator.createDouble(((ConstantOperator) arg0).getDouble());
                        break;
                    case LARGEINT:
                        countOperator = new CastOperator(returnType, newAggRef, false);
                        constOperator = ConstantOperator.createLargeInt(new BigInteger(arg0.toString()));
                        break;
                    case BIGINT:
                        countOperator = newAggRef;
                        constOperator = ConstantOperator.createBigint(Long.parseLong(arg0.toString()));
                        break;
                    default:
                        Preconditions.checkState(false, "unexpected sum function result type");
                }
                CallOperator newMultiply = new CallOperator("multiply", returnType,
                        Lists.newArrayList(
                                countOperator, constOperator));
                return newMultiply;
            } else {
                ColumnRefOperator newColumnRef;
                if (arg0.isColumnRef() && oldPreAggProjections.containsKey(arg0)) {
                    newColumnRef = (ColumnRefOperator) arg0;
                } else if (commonArguments.containsKey(arg0)) {
                    newColumnRef = commonArguments.get(arg0);
                } else {
                    newColumnRef = columnRefFactory.create(arg0, arg0.getType(), arg0.isNullable());
                    newPreAggProjections.put(newColumnRef, arg0);
                    commonArguments.put(arg0, newColumnRef);
                }

                Type sumFunctionType = returnType;
                if (returnType.isDecimalV3()) {
                    // for decimal type, we should keep result's scale same as the input's scale
                    int argScale = ((ScalarType) arg0.getType()).getScalarScale();
                    sumFunctionType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, argScale);
                }

                AggregateFunction sumFunction = AggregateFunction.createBuiltin(
                        FunctionSet.SUM, Lists.newArrayList(arg0.getType()), sumFunctionType, sumFunctionType,
                        false, true, false);

                CallOperator newAggFunction = new CallOperator("sum",
                        sumFunctionType, Lists.newArrayList(newColumnRef), sumFunction);

                ColumnRefOperator newAggRef = columnRefFactory.create(
                        newAggFunction, newAggFunction.getType(), true);
                newAggregations.put(newAggRef, newAggFunction);
                return newAggRef;
            }
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator op, Void context) {
            if (oldAggregations.containsKey(op)) {
                CallOperator aggFunction = oldAggregations.get(op);
                if (aggFunction.isAggregate() && !aggFunction.isDistinct()
                        && aggFunction.getFnName().equals("sum") &&
                        !aggFunction.getType().isDecimalV2()) {
                    ScalarOperator aggExpr = aggFunction.getArguments().get(0);
                    if (aggExpr.isColumnRef()) {
                        ColumnRefOperator aggColumnRef = (ColumnRefOperator) aggExpr;
                        ScalarOperator aggColumnExpr = oldPreAggProjections.get(aggColumnRef);
                        if (aggColumnExpr.getOpType() == OperatorType.CALL) {
                            CallOperator callOperator = (CallOperator) aggColumnExpr;
                            if (callOperator.getFnName().equals("add")) {
                                List<ScalarOperator> arguments = callOperator.getArguments();
                                Preconditions.checkState(arguments.size() == 2);
                                // there is at least one const in add expr
                                if (arguments.get(0).isConstant() || arguments.get(1).isConstant()) {
                                    ScalarOperator newArg0 = rewriteCastForAggExpr(arguments.get(0));
                                    ScalarOperator newArg1 = rewriteCastForAggExpr(arguments.get(1));

                                    ScalarOperator agg0 = createNewAggFunction(
                                            newArg0, newArg1, aggFunction.getType());
                                    ScalarOperator agg1 = createNewAggFunction(
                                            newArg1, newArg0, aggFunction.getType());

                                    CallOperator newAdd = new CallOperator("add",
                                            aggFunction.getType(), Lists.newArrayList(agg0, agg1));
                                    return newAdd;
                                }
                            }
                        }
                    }
                }
                reservedAggregations.put(op, oldAggregations.get(op));
                reservedPreAggProjections.put(op, op);
            }
            return op;
        }
    }
}
