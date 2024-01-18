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
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.structure.Pair;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.EquivalentShuttleContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.IRewriteEquivalent;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.RewriteEquivalent;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.RewriteEquivalent.EQUIVALENTS;

public class EquationRewriter {

    private Multimap<ScalarOperator, Pair<ColumnRefOperator, ScalarOperator>> equationMap;
    private Map<IRewriteEquivalent.RewriteEquivalentType, List<RewriteEquivalent>> rewriteEquivalents;
    private Map<ColumnRefOperator, ColumnRefOperator> columnMapping;

    private AggregateFunctionRewriter aggregateFunctionRewriter;
    boolean underAggFunctionRewriteContext;

    public EquationRewriter() {
        this.equationMap = ArrayListMultimap.create();
        this.rewriteEquivalents = Maps.newHashMap();
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



    private final class EquivalentShuttle extends BaseScalarOperatorShuttle {
        private final EquivalentShuttleContext shuttleContext;

        public EquivalentShuttle(EquivalentShuttleContext eqContext) {
            this.shuttleContext = eqContext;
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public Optional<ScalarOperator> preprocess(ScalarOperator scalarOperator) {
            return replace(scalarOperator);
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            Optional<ScalarOperator> tmp = replace(predicate);
            if (tmp.isPresent()) {
                return tmp.get();
            }

            // rewrite by equivalent
            ScalarOperator rewritten = rewriteByEquivalent(predicate, IRewriteEquivalent.RewriteEquivalentType.PREDICATE);
            if (rewritten != null) {
                shuttleContext.setRewrittenByEquivalent(true);
                return rewritten;
            }

            return super.visitBinaryPredicate(predicate, context);
        }

        private ScalarOperator rewriteByEquivalent(ScalarOperator input,
                                                   IRewriteEquivalent.RewriteEquivalentType type) {
            if (!rewriteEquivalents.containsKey(type)) {
                return null;
            }
            for (RewriteEquivalent equivalent : rewriteEquivalents.get(type)) {
                ScalarOperator replaced = equivalent.rewrite(shuttleContext, columnMapping, input);
                if (replaced != null) {
                    return replaced;
                }
            }
            return null;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            // 1. rewrite query's predicate
            Optional<ScalarOperator> tmp = replace(call);
            if (tmp.isPresent()) {
                return tmp.get();
            }

            // rewrite by equivalent
            ScalarOperator rewritten = rewriteByEquivalent(call, IRewriteEquivalent.RewriteEquivalentType.AGGREGATE);
            if (rewritten != null) {
                shuttleContext.setRewrittenByEquivalent(true);
                return rewritten;
            }

            // retry again by using aggregateFunctionRewriter when predicate cannot be rewritten.
            if (aggregateFunctionRewriter != null && aggregateFunctionRewriter.canRewriteAggFunction(call) &&
                    !isUnderAggFunctionRewriteContext()) {
                ScalarOperator newChooseScalarOp = aggregateFunctionRewriter.rewriteAggFunction(call);
                if (newChooseScalarOp != null) {
                    setUnderAggFunctionRewriteContext(true);
                    // NOTE: To avoid repeating `rewriteAggFunction` by `aggregateFunctionRewriter`, use
                    // `underAggFunctionRewriteContext` to mark it's under agg function rewriter and no need rewrite again.
                    rewritten = newChooseScalarOp.accept(this, null);
                    setUnderAggFunctionRewriteContext(false);
                    return rewritten;
                }
            }

            return super.visitCall(call, context);
        }

        Optional<ScalarOperator> replace(ScalarOperator scalarOperator) {
            if (equationMap.containsKey(scalarOperator)) {
                Optional<Pair<ColumnRefOperator, ScalarOperator>> mappedColumnAndExprRef =
                        equationMap.get(scalarOperator).stream().findFirst();

                ColumnRefOperator basedColumn = mappedColumnAndExprRef.get().first;
                ScalarOperator extendedExpr = mappedColumnAndExprRef.get().second;

                if (columnMapping == null) {
                    return extendedExpr == null ? Optional.of(basedColumn.clone()) : Optional.of(extendedExpr.clone());
                }

                ColumnRefOperator replaced = columnMapping.get(basedColumn);
                if (replaced == null) {
                    return Optional.empty();
                }

                if (extendedExpr == null) {
                    return Optional.of(replaced.clone());
                }
                ScalarOperator newExpr = extendedExpr.clone();
                return replaceColInExpr(newExpr, basedColumn,
                        replaced.clone()) ? Optional.of(newExpr) : Optional.empty();
            }

            return Optional.empty();
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
    }

    private final EquivalentShuttle shuttle = new EquivalentShuttle(new EquivalentShuttleContext(false));

    protected ScalarOperator replaceExprWithTarget(ScalarOperator expr) {
        return expr.accept(shuttle, null);
    }

    protected Pair<ScalarOperator, EquivalentShuttleContext> replaceExprWithRollup(ScalarOperator expr) {
        final EquivalentShuttleContext shuttleContext = new EquivalentShuttleContext(true);
        final EquivalentShuttle shuttle = new EquivalentShuttle(shuttleContext);
        return Pair.create(expr.accept(shuttle, null), shuttleContext);
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

        for (IRewriteEquivalent equivalent : EQUIVALENTS) {
            IRewriteEquivalent.RewriteEquivalentContext eqContext = equivalent.prepare(expr);
            if (eqContext != null) {
                RewriteEquivalent eq = new RewriteEquivalent(eqContext, equivalent, col);
                rewriteEquivalents.computeIfAbsent(eq.getRewriteEquivalentType(), x -> Lists.newArrayList())
                        .add(eq);
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
}
