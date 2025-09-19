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

package com.starrocks.sql.spm;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class SPMPlanner {
    private final ConnectContext session;

    private final Map<Long, Expr> placeholderValues = Maps.newHashMap();

    private final PlaceholderBinder binder = new PlaceholderBinder();

    private final PlaceholderReplacer replacer = new PlaceholderReplacer();

    private final Stopwatch watch = Stopwatch.createUnstarted();

    private BaselinePlan baseline;

    public SPMPlanner(ConnectContext session) {
        this.session = session;
    }

    public BaselinePlan getBaseline() {
        return baseline;
    }

    public StatementBase plan(StatementBase query) {
        if (!(query instanceof QueryStatement)) {
            return query;
        }
        if (!session.getSessionVariable().isEnableSPMRewrite()) {
            return query;
        }

        if (query.isExistQueryScopeHint()) {
            return query;
        }

        watch.start();
        try (Timer ignored = Tracers.watchScope("SPMPlanner")) {
            analyze(query);
            checkTimeout();

            List<BaselinePlan> plans = Lists.newArrayList();
            try (Timer ignored2 = Tracers.watchScope("foundBaseline")) {
                SPMAst2SQLBuilder builder = new SPMAst2SQLBuilder(false, true);
                String digest = builder.build((QueryStatement) query);
                long hash = builder.buildHash();
                plans.addAll(session.getSqlPlanStorage().findBaselinePlan(digest, hash));
                plans.addAll(GlobalStateMgr.getCurrentState().getSqlPlanStorage().findBaselinePlan(digest, hash));
                checkTimeout();
                plans = plans.stream().filter(BaselinePlan::isEnable)
                        .sorted((o1, o2) -> {
                            if (o1.getQueryMs() > 0 && o2.getQueryMs() > 0) {
                                return Double.compare(o1.getQueryMs(), o2.getQueryMs());
                            } else if (o1.getQueryMs() < 0 && o2.getQueryMs() < 0) {
                                return Double.compare(o1.getCosts(), o2.getCosts());
                            } else {
                                return o1.getQueryMs() > 0 ? 1 : -1;
                            }
                        }).toList();
            }

            try (Timer ignored3 = Tracers.watchScope("bindBaseline")) {
                for (BaselinePlan base : plans) {
                    checkTimeout();
                    if (bind(base, query)) {
                        baseline = base;
                        return replacePlan(base);
                    }
                }
            }
        } catch (Exception e) {
            // fallback to original query
            baseline = null; // clean baseline
            return query;
        }
        return query;
    }

    private void checkTimeout() {
        if (session.getSessionVariable().getSpmRewriteTimeoutMs() > 0
                && watch.elapsed().toMillis() > session.getSessionVariable().getSpmRewriteTimeoutMs()) {
            throw new StarRocksPlannerException("SPM rewrite timeout", ErrorType.INTERNAL_ERROR);
        }
    }

    private void analyze(StatementBase query) {
        try (PlannerMetaLocker locker = new PlannerMetaLocker(session, query)) {
            locker.lock();
            Analyzer.analyze(query, session);
        }
    }

    private boolean bind(BaselinePlan baseline, StatementBase query) {
        List<StatementBase> binder = SqlParser.parse(baseline.getBindSql(), session.getSessionVariable());
        Preconditions.checkState(binder.size() == 1);
        // remove when support cache
        analyze(binder.get(0));
        return this.binder.bind(binder.get(0), query);
    }

    private StatementBase replacePlan(BaselinePlan baseline) {
        List<StatementBase> plan = SqlParser.parse(baseline.getPlanSql(), session.getSessionVariable());
        Preconditions.checkState(plan.size() == 1);
        if (placeholderValues.isEmpty()) {
            return plan.get(0);
        }
        return (StatementBase) replacer.visit(plan.get(0));
    }

    private class PlaceholderBinder extends SPMAstCheckVisitor {
        public boolean bind(ParseNode one, ParseNode two) {
            try {
                return one.accept(this, two);
            } catch (ClassCastException e) {
                // ignore
                return false;
            }
        }

        // Expressions using functions need to be handled separately, because spm_function may lead to the selection
        // of different types of functions, which can be ignored during binding
        //
        // this problem only occurs when binding the real plan, because the parameters at this time may be different
        // ----------------- start -----------------
        @Override
        public Boolean visitFunctionCall(FunctionCallExpr node, ParseNode node2) {
            if (SPMFunctions.isSPMFunctions(node)) {
                if (!SPMFunctions.checkParameters(node, (Expr) node2)) {
                    return false;
                }

                Preconditions.checkState(!node.getChildren().isEmpty());
                Preconditions.checkState(node.getChild(0) instanceof IntLiteral);
                long spmId = ((IntLiteral) node.getChild(0)).getValue();

                if (placeholderValues.containsKey(spmId)) {
                    // same placeholder check is same values
                    return placeholderValues.get(spmId).equals(node2);
                }
                placeholderValues.put(spmId, (Expr) node2);
                return true;
            }
            FunctionCallExpr other = cast(node2);
            Preconditions.checkNotNull(node.getFn());
            Preconditions.checkNotNull(other.getFn());
            if (!StringUtils.equals(node.getFn().functionName(), other.getFn().functionName())) {
                return false;
            }
            return check(node.getChildren(), ((Expr) node2).getChildren());
        }

        @Override
        public Boolean visitArithmeticExpr(ArithmeticExpr node, ParseNode node2) {
            ArithmeticExpr other = cast(node2);
            if (node.getOp() != other.getOp()) {
                return false;
            }
            return check(node.getChildren(), ((Expr) node2).getChildren());
        }

        @Override
        public Boolean visitTimestampArithmeticExpr(TimestampArithmeticExpr node, ParseNode node2) {
            TimestampArithmeticExpr other = cast(node2);
            Preconditions.checkNotNull(node.getFn());
            Preconditions.checkNotNull(other.getFn());
            if (!StringUtils.equals(node.getFn().functionName(), other.getFn().functionName())) {
                return false;
            }
            return check(node.getChildren(), ((Expr) node2).getChildren());
        }
        // ----------------- end -----------------

        @Override
        public Boolean visitInPredicate(InPredicate node, ParseNode context) {
            if (!SPMFunctions.isSPMFunctions(node)) {
                return super.visitExpression(node, context);
            }
            InPredicate other = cast(context);
            if (node.isNotIn() != other.isNotIn() || !check(node.getChild(0), other.getChild(0))) {
                return false;
            }
            if (!check(node.getChild(0), other.getChild(0))) {
                return false;
            }
            if (!SPMFunctions.checkParameters(node, other)) {
                return false;
            }
            Preconditions.checkState(node.getChildren().size() == 2);
            Preconditions.checkState(!node.getChild(1).getChildren().isEmpty());
            Preconditions.checkState(node.getChild(1).getChild(0) instanceof IntLiteral);
            long spmId = ((IntLiteral) node.getChild(1).getChild(0)).getValue();
            if (placeholderValues.containsKey(spmId)) {
                // same placeholder check is same values
                return placeholderValues.get(spmId).equals(other);
            }
            placeholderValues.put(spmId, other);
            return true;
        }
    }

    private class PlaceholderReplacer extends SPMUpdateExprVisitor<Void> {
        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Void context) {
            if (SPMFunctions.isSPMFunctions(node)) {
                Preconditions.checkState(!node.getChildren().isEmpty());
                Preconditions.checkState(node.getChild(0) instanceof IntLiteral);
                long id = ((IntLiteral) node.getChild(0)).getValue();
                return placeholderValues.get(id);
            }
            return super.visitExpression(node, context);
        }

        @Override
        public ParseNode visitInPredicate(InPredicate node, Void context) {
            if (SPMFunctions.isSPMFunctions(node)) {
                Preconditions.checkState(node.getChildren().size() == 2);
                Preconditions.checkState(!node.getChild(1).getChildren().isEmpty());
                Preconditions.checkState(node.getChild(1).getChild(0) instanceof IntLiteral);
                long spmId = ((IntLiteral) node.getChild(1).getChild(0)).getValue();
                InPredicate value = placeholderValues.get(spmId).cast();
                return new InPredicate(visitExpr(node.getChild(0), context), value.getListChildren(), node.isNotIn());
            }
            return super.visitExpression(node, context);
        }
    }
}
