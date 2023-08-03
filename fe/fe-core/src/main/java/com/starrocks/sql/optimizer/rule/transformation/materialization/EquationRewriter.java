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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;

import java.util.Map;
import java.util.Optional;

public class EquationRewriter {

    private Multimap<ScalarOperator, Pair<ColumnRefOperator, ScalarOperator>> equationMap;
    private Multimap<ScalarOperator, Pair<ColumnRefOperator, PredicateReplaceChecker>> predicateProbMap;
    private Map<ColumnRefOperator, ColumnRefOperator> columnMapping;
    private AggregateFunctionRewriter aggregateFunctionRewriter;
    boolean underAggFunctionRewriteContext;

    public EquationRewriter() {
        this.equationMap = ArrayListMultimap.create();
        this.predicateProbMap = ArrayListMultimap.create();
    }

    public void setOutputMapping(Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
        this.columnMapping = columnMapping;
    }

    public void setAggregateFunctionRewriter(AggregateFunctionRewriter aggregateFunctionRewriter) {
        this.aggregateFunctionRewriter = aggregateFunctionRewriter;
    }

    public boolean isUnderAggFunctionRewriteContext() {
        return underAggFunctionRewriteContext;
    }

    public void setUnderAggFunctionRewriteContext(boolean underAggFunctionRewriteContext) {
        this.underAggFunctionRewriteContext = underAggFunctionRewriteContext;
    }

    protected ScalarOperator replaceExprWithTarget(ScalarOperator expr) {

        BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                return null;
            }

            @Override
            public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                if (tmp != null) {
                    return tmp;
                }

                ScalarOperator left = predicate.getChild(0);
                ScalarOperator right = predicate.getChild(1);

                if (predicateProbMap.containsKey(left)) {
                    Pair<ColumnRefOperator, PredicateReplaceChecker> pair = predicateProbMap.get(left).iterator().next();
                    if (pair.second.canReplace(right)) {
                        ColumnRefOperator replaced = columnMapping.get(pair.first);
                        if (replaced != null) {
                            ScalarOperator clonePredicate = predicate.clone();
                            clonePredicate.setChild(0, replaced.clone());
                            return clonePredicate;
                        }
                    }
                }

                return super.visitBinaryPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitCall(CallOperator predicate, Void context) {
                // 1. rewrite query's predicate
                ScalarOperator tmp = replace(predicate);
                if (tmp != null) {
                    return tmp;
                }

                // 2. normalize predicate to better match mv
                // TODO: merge into aggregateFunctionRewriter later.
                predicate = normalizeCallOperator(predicate);
                tmp = replace(predicate);
                if (tmp != null) {
                    return tmp;
                }

                // 3. retry again by using aggregateFunctionRewriter when predicate cannot be rewritten.
                if (aggregateFunctionRewriter != null && aggregateFunctionRewriter.canRewriteAggFunction(predicate) &&
                        !isUnderAggFunctionRewriteContext()) {
                    ScalarOperator newChooseScalarOp = aggregateFunctionRewriter.rewriteAggFunction(predicate);
                    if (newChooseScalarOp != null) {
                        setUnderAggFunctionRewriteContext(true);
                        // NOTE: To avoid repeating `rewriteAggFunction` by `aggregateFunctionRewriter`, use
                        // `underAggFunctionRewriteContext` to mark it's under agg function rewriter and no need rewrite again.
                        ScalarOperator rewritten = newChooseScalarOp.accept(this, null);
                        setUnderAggFunctionRewriteContext(false);
                        return rewritten;
                    }
                }

                return super.visitCall(predicate, context);
            }

            @Override
            public ScalarOperator visitCastOperator(CastOperator cast, Void context) {
                ScalarOperator tmp = replace(cast);
                return tmp != null ? tmp : super.visitCastOperator(cast, context);
            }

            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                ScalarOperator tmp = replace(variable);
                return tmp != null ? tmp : super.visitVariableReference(variable, context);
            }

            @Override
            public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
                ScalarOperator tmp = replace(operator);
                return tmp != null ? tmp : super.visitCaseWhenOperator(operator, context);
            }

            @Override
            public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator operator, Void context) {
                ScalarOperator tmp = replace(operator);
                return tmp != null ? tmp : super.visitIsNullPredicate(operator, context);
            }

            @Override
            public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitCompoundPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitArray(ArrayOperator array, Void context) {
                ScalarOperator tmp = replace(array);
                return tmp != null ? tmp : super.visitArray(array, context);
            }

            @Override
            public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
                ScalarOperator tmp = replace(collectionElementOp);
                return tmp != null ? tmp : super.visitCollectionElement(collectionElementOp, context);
            }

            @Override
            public ScalarOperator visitArraySlice(ArraySliceOperator array, Void context) {
                ScalarOperator tmp = replace(array);
                return tmp != null ? tmp : super.visitArraySlice(array, context);
            }

            @Override
            public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitBetweenPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitExistsPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitInPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitLikePredicateOperator(predicate, context);
            }

            @Override
            public ScalarOperator visitMultiInPredicate(MultiInPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitMultiInPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitLambdaFunctionOperator(LambdaFunctionOperator operator, Void context) {
                ScalarOperator tmp = replace(operator);
                return tmp != null ? tmp : super.visitLambdaFunctionOperator(operator, context);
            }

            ScalarOperator replace(ScalarOperator scalarOperator) {
                if (equationMap.containsKey(scalarOperator)) {
                    Optional<Pair<ColumnRefOperator, ScalarOperator>> mappedColumnAndExprRef =
                            equationMap.get(scalarOperator).stream().findFirst();

                    ColumnRefOperator basedColumn = mappedColumnAndExprRef.get().first;
                    ScalarOperator extendedExpr = mappedColumnAndExprRef.get().second;

                    if (columnMapping == null) {
                        return extendedExpr == null ? basedColumn.clone() : extendedExpr.clone();
                    }

                    ColumnRefOperator replaced = columnMapping.get(basedColumn);
                    if (replaced == null) {
                        return null;
                    }

                    if (extendedExpr == null) {
                        return replaced.clone();
                    }
                    ScalarOperator newExpr = extendedExpr.clone();
                    return replaceColInExpr(newExpr, basedColumn,
                            replaced.clone()) ? newExpr : null;
                }

                return null;
            }

            private boolean replaceColInExpr(ScalarOperator expr, ColumnRefOperator oldCol, ScalarOperator newCol) {
                for (int i = 0; i < expr.getChildren().size(); i++) {
                    if (oldCol.equals(expr.getChild(i))) {
                        expr.setChild(i, newCol);
                        return true;
                    } else if (replaceColInExpr(expr.getChild(i), oldCol, newCol)) {
                        return true;
                    }
                }
                return false;
            }

        };
        return expr.accept(shuttle, null);
    }

    private static CallOperator normalizeCallOperator(CallOperator aggFunc) {
        String aggFuncName = aggFunc.getFnName();
        if (!aggFuncName.equals(FunctionSet.COUNT) || aggFunc.isDistinct()) {
            return aggFunc;
        }

        // unify count(*) or count(non-nullable column) to count(1) to be better for rewrite.
        if (aggFunc.getChildren().size() == 0 || !aggFunc.getChild(0).isNullable()) {
            return new CallOperator(FunctionSet.COUNT, aggFunc.getType(),
                    Lists.newArrayList(ConstantOperator.createTinyInt((byte) 1)));
        }
        return aggFunc;
    }

    public boolean containsKey(ScalarOperator scalarOperator) {
        return equationMap.containsKey(scalarOperator);
    }

    public void addMapping(ScalarOperator expr, ColumnRefOperator col) {
        equationMap.put(expr, Pair.create(col, null));

        // Convert a + 1 -> col_f => a => col_f - 1
        Pair<ScalarOperator, ScalarOperator> extendedEntry = new EquationTransformer(expr, col).getMapping();
        if (extendedEntry.second != col) {
            equationMap.put(extendedEntry.first, Pair.create(col, extendedEntry.second));
        }

        if (expr instanceof CallOperator) {
            CallOperator aggFunc = (CallOperator) expr;
            if (aggFunc.getFnName().equals(FunctionSet.TIME_SLICE)) {
                // mv:    SELECT time_slice(dt, INTERVAL 5 MINUTE) as t FROM table
                // query: SELECT time_slice(dt, INTERVAL 5 MINUTE) as t FROM table WHERE dt > '2023-06-01'
                // if '2023-06-01'=time_slice('2023-06-01', INTERVAL 5 MINUTE), can replace predicate dt => t
                ScalarOperator first = expr.getChild(0);
                predicateProbMap.put(first, Pair.create(col, new TimeSliceReplaceChecker(((CallOperator) expr))));
            } else if (aggFunc.getFnName().equals(FunctionSet.COUNT) && !aggFunc.isDistinct()) {
                CallOperator newAggFunc = normalizeCallOperator(aggFunc);
                if (newAggFunc != null && newAggFunc != aggFunc) {
                    equationMap.put(newAggFunc,  Pair.create(col, null));
                }
            }
        }
    }

    private static class EquationTransformer extends ScalarOperatorVisitor<Void, Void> {

        private static final Map<String, String> COMMUTATIVE_MAP = ImmutableMap.<String, String>builder()
                .put(FunctionSet.ADD, FunctionSet.SUBTRACT)
                .put(FunctionSet.SUBTRACT, FunctionSet.ADD)
                .build();

        private ScalarOperator equalExpr;
        private ScalarOperator needReducedExpr;

        public EquationTransformer(ScalarOperator needReducedExpr, ColumnRefOperator equalExpr) {
            super();
            this.equalExpr = equalExpr;
            this.needReducedExpr = needReducedExpr;
        }

        public Pair<ScalarOperator, ScalarOperator> getMapping() {
            needReducedExpr.accept(this, null);
            return Pair.create(needReducedExpr, equalExpr);
        }

        @Override
        public Void visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public Void visitCall(CallOperator call, Void context) {
            if (!COMMUTATIVE_MAP.containsKey(call.getFnName())) {
                return null;
            }

            if (!call.getChild(1).isConstant() && !call.getChild(0).isConstant()) {
                return null;
            }
            ScalarOperator constantOperator = call.getChild(0).isConstant() ? call.getChild(0) : call.getChild(1);
            ScalarOperator varOperator = call.getChild(0).isConstant() ? call.getChild(1) : call.getChild(0);

            String fnName = COMMUTATIVE_MAP.get(call.getFnName());
            equalExpr = new CallOperator(fnName, call.getType(),
                    Lists.newArrayList(equalExpr, constantOperator), findArithmeticFunction(call, fnName));
            needReducedExpr = varOperator;

            varOperator.accept(this, context);

            return null;
        }


        private Function findArithmeticFunction(CallOperator call, String fnName) {
            return Expr.getBuiltinFunction(fnName, call.getFunction().getArgs(), Function.CompareMode.IS_IDENTICAL);
        }

    }

    private interface PredicateReplaceChecker {
        boolean canReplace(ScalarOperator operator);
    }

    private static class TimeSliceReplaceChecker implements PredicateReplaceChecker {
        private final CallOperator mvTimeSlice;

        public TimeSliceReplaceChecker(CallOperator mvTimeSlice) {
            this.mvTimeSlice = mvTimeSlice;
        }

        @Override
        public boolean canReplace(ScalarOperator operator) {
            try {
                if (operator.isConstantRef() && operator.getType().getPrimitiveType() == PrimitiveType.DATETIME) {
                    ConstantOperator sliced = ScalarOperatorFunctions.timeSlice(
                            (ConstantOperator) operator,
                            ((ConstantOperator) mvTimeSlice.getChild(1)),
                            ((ConstantOperator) mvTimeSlice.getChild(2)),
                            ((ConstantOperator) mvTimeSlice.getChild(3)));
                    return sliced.equals(operator);
                }
            } catch (AnalysisException e) {
                return false;
            }
            return false;
        }
    }

}
