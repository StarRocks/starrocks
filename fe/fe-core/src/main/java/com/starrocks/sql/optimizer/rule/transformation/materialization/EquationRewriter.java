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
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.FunctionAnalyzer;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.EquivalentShuttleContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.IRewriteEquivalent;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.RewriteEquivalent;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.RewriteEquivalent.EQUIVALENTS;

public class EquationRewriter {

    private Multimap<ScalarOperator, Pair<ColumnRefOperator, ScalarOperator>> equationMap;
    private Map<IRewriteEquivalent.RewriteEquivalentType, List<RewriteEquivalent>> rewriteEquivalents;
    private Map<ColumnRefOperator, ColumnRefOperator> columnMapping;

    private AggregateFunctionRewriter aggregateFunctionRewriter;
    boolean underAggFunctionRewriteContext;

    // Replace the corresponding ColumnRef with ScalarOperator if this call operator can be pushed down.
    private Map<String, Map<ColumnRefOperator, CallOperator>> aggPushDownOperatorMap = Maps.newHashMap();

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

    public boolean isColWithOnlyGroupByKeys(ScalarOperator expr) {
        if (expr.getChildren().isEmpty()) {
            return expr.isConstant() || equationMap.containsKey(expr);
        }
        for (ScalarOperator e : expr.getChildren()) {
            if (expr.isConstant() || equationMap.containsKey(e)) {
                continue;
            }
            if (!isColWithOnlyGroupByKeys(e)) {
                return false;
            }
        }
        return true;
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
            if (!shuttleContext.isUseEquivalent() || !rewriteEquivalents.containsKey(type)) {
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

            // rewrite by pushing down aggregate
            rewritten = rewriteByPushDownAggregation(call);
            if (rewritten != null) {
                return rewritten;
            }

            // If count(1)/sum(1) cannot be rewritten by mv's defined equivalents, return null directly,
            // otherwise it may cause a wrong plan.
            // mv       : SELECT 1, count(distinct k1) from tbl1;
            // query    : SELECT count(1) from tbl1;
            // MV should not rewrite the query.
            if (call.isAggregate() && call.isConstant()) {
                return null;
            }

            return super.visitCall(call, context);
        }

        private ScalarOperator rewriteByPushDownAggregation(CallOperator call) {
            if (AggregateFunctionRollupUtils.MV_REWRITE_PUSH_DOWN_FUNCTION_MAP.containsKey(call.getFnName()) &&
                    aggPushDownOperatorMap.containsKey(call.getFnName())) {
                Map<ColumnRefOperator, CallOperator> operatorMap = aggPushDownOperatorMap.get(call.getFnName());
                ScalarOperator arg0 = call.getChild(0);
                if (call.getChildren().size() != 1) {
                    return null;
                }
                // push down aggregate now only supports one child
                // it's fine since rewrite will clone argo
                ScalarOperator pdCall = pushDownAggregationToArg0(call, arg0, operatorMap);
                if (pdCall == null || pdCall.equals(arg0)) {
                    return null;
                }
                ScalarOperator rewritten = pdCall.accept(this, null);
                // only can be used if pdCall is rewritten
                if (rewritten != null && !rewritten.equals(pdCall)) {
                    shuttleContext.setRewrittenByEquivalent(true);
                    if (FunctionSet.SUM.equalsIgnoreCase(call.getFnName())) {
                        Function newFn = ScalarOperatorUtil.findSumFn(new Type[] {rewritten.getType()});
                        CallOperator newCall = new CallOperator(call.getFnName(), call.getType(), Lists.newArrayList(rewritten),
                                newFn);
                        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
                        CallOperator result = (CallOperator) scalarRewriter.rewrite(newCall,
                                Lists.newArrayList(new ImplicitCastRule()));
                        return result;
                    } else {
                        return new CallOperator(call.getFnName(), call.getType(), Lists.newArrayList(rewritten),
                                call.getFunction());
                    }
                }
            }
            return null;
        }

        /**
         * Whether the call operator can be pushed down:
         * - Only supports to push down min/max/sum aggregate function
         * - If the argument is a column ref, and operator map contains the column ref, return true
         * - If the argument is a call operator and contains multi-column refs(Ony IF/CaseWhen is supported),
         *  ensure the aggregate column does not appear in the condition clause.
         */
        private ScalarOperator pushDownAggregationToArg0(ScalarOperator call,
                                                         ScalarOperator arg0,
                                                         Map<ColumnRefOperator, CallOperator> operatorMap) {
            if (arg0 == null || !(arg0 instanceof CallOperator)) {
                return null;
            }
            CallOperator arg0Call = (CallOperator) arg0;
            List<ColumnRefOperator> columnRefs = arg0.getColumnRefs();
            if (columnRefs.size() == 1) {
                ColumnRefOperator child0 = columnRefs.get(0);
                if (!operatorMap.containsKey(child0)) {
                    return null;
                }
                CallOperator aggFunc = operatorMap.get(child0);
                // strict mode, only supports col's type and agg(col)'s type are strict equal; otherwise we should
                // refresh the call operator's argument/result type recursively.
                if (!aggFunc.getType().equals(child0.getType())) {
                    return null;
                }
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(operatorMap);
                return rewriter.rewrite(arg0);
            } else {
                // if there are many column refs in arg0, the agg column must be the same.
                if (arg0Call.getFnName().equalsIgnoreCase(FunctionSet.IF)) {
                    if (arg0Call.getChildren().size() != 3) {
                        return null;
                    }
                    // if the first column ref is in the operatorMap, means the agg column maybe as condition which
                    // cannot be rewritten
                    if (isContainAggregateColumn(arg0Call.getChild(0), operatorMap)) {
                        return null;
                    }
                    ScalarOperator child1 = arg0Call.getChild(1);
                    ScalarOperator rewritten1 = rewriteIfOnlyContianAggregateColumn(child1, operatorMap);
                    if (rewritten1 == null) {
                        return null;
                    }
                    ScalarOperator child2 = arg0Call.getChild(2);
                    ScalarOperator rewritten2 = rewriteIfOnlyContianAggregateColumn(child2, operatorMap);
                    if (rewritten2 == null) {
                        return null;
                    }
                    ConnectContext ctx = ConnectContext.get() == null ? new ConnectContext() : ConnectContext.get();
                    List<ScalarOperator> args = Lists.newArrayList(arg0Call.getChild(0), rewritten1, rewritten2);
                    Type[] argTypes = args.stream().map(x -> x.getType()).collect(Collectors.toList()).toArray(new Type[0]);
                    Function newFn = FunctionAnalyzer.getAnalyzedBuiltInFunction(ctx, FunctionSet.IF, null, argTypes,
                            NodePosition.ZERO);
                    if (newFn == null) {
                        return null;
                    }
                    return new CallOperator(FunctionSet.IF, newFn.getReturnType(), args, newFn);
                } else if (arg0Call instanceof CaseWhenOperator) {
                    CaseWhenOperator caseClause = (CaseWhenOperator) arg0Call;
                    // if case condition contains any agg column ref, return false
                    ScalarOperator caseExpr = caseClause.hasCase() ? caseClause.getCaseClause() : null;
                    if (caseExpr != null && isContainAggregateColumn(caseExpr, operatorMap)) {
                        return null;
                    }
                    List<ScalarOperator> newCaseWhens = Lists.newArrayList();
                    for (int i = 0; i < caseClause.getWhenClauseSize(); i++) {
                        ScalarOperator when = caseClause.getWhenClause(i);
                        if (isContainAggregateColumn(when, operatorMap)) {
                            return null;
                        }
                        newCaseWhens.add(when);

                        // when clause or else clause can only contain aggregate column ref
                        ScalarOperator then = caseClause.getThenClause(i);
                        ScalarOperator newThen = rewriteIfOnlyContianAggregateColumn(then, operatorMap);
                        if (newThen == null) {
                            return null;
                        }
                        newCaseWhens.add(newThen);
                    }
                    ScalarOperator elseClause = caseClause.hasElse() ? caseClause.getElseClause() : null;
                    ScalarOperator newElseClause = elseClause;
                    if (elseClause != null) {
                        newElseClause = rewriteIfOnlyContianAggregateColumn(elseClause, operatorMap);
                        if (newElseClause == null) {
                            return null;
                        }
                    }
                    // NOTE: use call's result type as its input.
                    return new CaseWhenOperator(call.getType(), caseExpr, newElseClause, newCaseWhens);
                }
            }
            return null;
        }

        private boolean isContainAggregateColumn(ScalarOperator child,
                                                 Map<ColumnRefOperator, CallOperator> operatorMap) {
            return child.getColumnRefs().stream().anyMatch(x -> operatorMap.containsKey(x));
        }

        private ScalarOperator rewriteIfOnlyContianAggregateColumn(ScalarOperator child,
                                                                   Map<ColumnRefOperator, CallOperator> operatorMap) {
            List<ColumnRefOperator> colRefs = child.getColumnRefs();
            if (colRefs.size() > 1) {
                return null;
            }
            // constant operator
            if (colRefs.size() == 0) {
                return child;
            }
            // TODO: only supports column ref now, support common expression later.
            if (!(child instanceof ColumnRefOperator)) {
                return null;

            }
            if (!operatorMap.containsKey(colRefs.get(0))) {
                return null;
            }
            return operatorMap.get(colRefs.get(0));
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

    private final EquivalentShuttle shuttle = new EquivalentShuttle(new EquivalentShuttleContext(null, false, true));

    protected ScalarOperator replaceExprWithTarget(ScalarOperator expr) {
        return expr.accept(shuttle, null);
    }

    protected Pair<ScalarOperator, EquivalentShuttleContext> replaceExprWithRollup(RewriteContext rewriteContext,
                                                                                   ScalarOperator expr) {
        final EquivalentShuttleContext shuttleContext = new EquivalentShuttleContext(rewriteContext,
                true, true);
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

        // add into equivalents
        for (IRewriteEquivalent equivalent : EQUIVALENTS) {
            IRewriteEquivalent.RewriteEquivalentContext eqContext = equivalent.prepare(expr);
            if (eqContext != null) {
                RewriteEquivalent eq = new RewriteEquivalent(eqContext, equivalent, col);
                rewriteEquivalents.computeIfAbsent(eq.getRewriteEquivalentType(), x -> Lists.newArrayList())
                        .add(eq);
            }
        }

        // add into a push-down operator map
        if (expr instanceof CallOperator) {
            CallOperator call = (CallOperator) expr;
            String fnName = call.getFnName();
            if (AggregateFunctionRollupUtils.REWRITE_ROLLUP_FUNCTION_MAP.containsKey(fnName) && call.getChildren().size() == 1) {
                ScalarOperator arg0 = call.getChild(0);
                // NOTE: only support push down when the argument is a column ref.
                // eg:
                // mv: sum(cast(col as tinyint))
                // query: 2 * sum(col)
                // query cannot be used to push down, because the argument is not a column ref.
                if (arg0 != null && arg0.isColumnRef()) {
                    aggPushDownOperatorMap
                            .computeIfAbsent(fnName, x -> Maps.newHashMap())
                            .put((ColumnRefOperator) arg0, call);
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
}
