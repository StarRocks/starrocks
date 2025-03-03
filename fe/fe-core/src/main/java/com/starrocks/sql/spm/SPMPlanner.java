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
import com.google.common.collect.Maps;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SPMPlanner {
    private final ConnectContext session;

    private final Map<Long, Expr> placeholderValues = Maps.newHashMap();

    private final PlaceholderBinder binder = new PlaceholderBinder();

    private final PlaceholderReplacer replacer = new PlaceholderReplacer();

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

        try (Timer ignored = Tracers.watchScope("SPMPlanner")) {
            analyze(query);

            SPMAst2SQLBuilder builder = new SPMAst2SQLBuilder(false, true);
            String digest = builder.build((QueryStatement) query);
            long hash = builder.buildHash();
            SQLPlanManager spm = GlobalStateMgr.getCurrentState().getSqlPlanManager();
            List<BaselinePlan> plans = spm.findBaselinePlan(digest, hash);

            Optional<BaselinePlan> base;
            try (Timer ignored2 = Tracers.watchScope("bindPlan")) {
                base = plans.stream().min(Comparator.comparingDouble(BaselinePlan::getCosts));
                if (base.isEmpty()) {
                    return query;
                }
            }
            try (Timer ignored3 = Tracers.watchScope("replacePlan")) {
                if (!bind(base.get(), query)) {
                    return query;
                }
                baseline = base.get();
                return replacePlan(base.get());
            }
        } catch (Exception e) {
            // fallback to original query
            baseline = null; // clean baseline
            return query;
        }
    }

    private void analyze(StatementBase query) {
        PlannerMetaLocker locker = new PlannerMetaLocker(session, query);
        try {
            locker.lock();
            Analyzer.analyze(query, session);
        } finally {
            locker.unlock();
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
            if (SPMFunctions.isSPMFunctions(node) && ((Expr) node2).isConstant()) {
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
            if (node.getChildren().stream().noneMatch(SPMFunctions::isSPMFunctions)) {
                return super.visitExpression(node, context);
            }
            InPredicate other = cast(context);
            if (node.isNotIn() != other.isNotIn() || !check(node.getChild(0), other.getChild(0))) {
                return false;
            }
            Preconditions.checkState(node.getChildren().size() == 2);
            Preconditions.checkState(SPMFunctions.isSPMFunctions(node.getChild(1)));
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
            if (node.getChildren().stream().anyMatch(SPMFunctions::isSPMFunctions)) {
                Preconditions.checkState(node.getChildren().size() == 2);
                Preconditions.checkState(SPMFunctions.isSPMFunctions(node.getChild(1)));
                Preconditions.checkState(!node.getChild(1).getChildren().isEmpty());
                Preconditions.checkState(node.getChild(1).getChild(0) instanceof IntLiteral);
                long spmId = ((IntLiteral) node.getChild(1).getChild(0)).getValue();
                InPredicate value = placeholderValues.get(spmId).cast();
                return new InPredicate(node.getChild(0), value.getListChildren(), node.isNotIn());
            }
            return super.visitExpression(node, context);
        }
    }
}
