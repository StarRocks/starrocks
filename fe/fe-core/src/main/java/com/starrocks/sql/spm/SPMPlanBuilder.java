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
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SetVarHint;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;

import java.util.List;
import java.util.Optional;
import java.util.Set;

// to build plan for bind sql
public class SPMPlanBuilder {
    private final ConnectContext session;
    private final CreateBaselinePlanStmt stmt;

    private String bindSqlDigest;
    private long bindSqlHash;
    private String bindSql;
    private String planStmtSQL;
    private double costs;

    protected String getBindSqlDigest() {
        return bindSqlDigest;
    }

    protected String getPlanStmtSQL() {
        return planStmtSQL;
    }

    protected String getBindSql() {
        return bindSql;
    }

    public SPMPlanBuilder(ConnectContext session, CreateBaselinePlanStmt stmt) {
        this.session = session;
        this.stmt = stmt;
    }

    public BaselinePlan execute() {
        analyze();
        parameterizedStmt();
        generatePlan();
        return new BaselinePlan(stmt.isGlobal(), bindSql, bindSqlDigest, bindSqlHash,
                planStmtSQL, costs);
    }

    // don't need lock, because we don't need to modify table stats
    public void generatePlan() {
        List<HintNode> hints = this.stmt.getAllQueryScopeHints();
        SessionVariable backupVariable = session.getSessionVariable();
        SessionVariable cloneVariable = null;
        if (hints != null && !hints.isEmpty()) {
            cloneVariable = (SessionVariable) backupVariable.clone();
            for (HintNode hint : hints) {
                if (!(hint instanceof SetVarHint)) {
                    UnsupportedException.unsupportedException(
                            "sql pLan manager only supported session variables: " + hint.toSql());
                }

                try {
                    for (var entry : hint.getValue().entrySet()) {
                        session.getGlobalStateMgr().getVariableMgr()
                                .setSystemVariable(cloneVariable, new SystemVariable(
                                        entry.getKey(), new StringLiteral(entry.getValue())), true);
                    }
                } catch (DdlException e) {
                    throw new SemanticException("set variable error", e);
                }
            }
            session.setSessionVariable(cloneVariable);
        }

        try {
            QueryRelation query = this.stmt.getPlanStmt();
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            TransformerContext transformerContext = new TransformerContext(columnRefFactory, session, null);
            LogicalPlan logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(query);

            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            optimizerContext.setOptimizerOptions(
                    new OptimizerOptions(OptimizerOptions.OptimizerStrategy.BASELINE_PLAN));
            Optimizer optimizer = OptimizerFactory.create(optimizerContext);

            OptExpression optimizedPlan = optimizer.optimize(logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));

            SPMPlan2SQLBuilder sqlBuilder = new SPMPlan2SQLBuilder();
            planStmtSQL = sqlBuilder.toSQL(hints, optimizedPlan);
            costs = optimizedPlan.getCost();
        } finally {
            if (cloneVariable != null) {
                session.setSessionVariable(backupVariable);
            }
        }
    }

    protected void parameterizedStmt() {
        QueryRelation bind;
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        if (this.stmt.getBindStmt() != null) {
            // has bind and plan
            builder.findPlaceholder(this.stmt.getBindStmt());
            bind = builder.insertPlaceholder(this.stmt.getBindStmt());
            PlanASTPlaceholderBinder placeholderBinder = new PlanASTPlaceholderBinder(builder);
            placeholderBinder.bind(this.stmt.getPlanStmt());
        } else {
            // only plan
            bind = builder.insertPlaceholder(this.stmt.getPlanStmt());
        }
        SPMAst2SQLBuilder digestBuilder = new SPMAst2SQLBuilder(false, true);
        SPMAst2SQLBuilder sqlBuilder = new SPMAst2SQLBuilder(false, false);
        bindSqlDigest = digestBuilder.build(bind);
        bindSqlHash = digestBuilder.buildHash();
        bindSql = sqlBuilder.build(bind);
    }

    protected void analyze() {
        Preconditions.checkNotNull(stmt.getPlanStmt());
        analyzeStatement(new QueryStatement(stmt.getPlanStmt()));
        if (stmt.getBindStmt() != null) {
            analyzeStatement(new QueryStatement(stmt.getBindStmt()));
        }
    }

    private void analyzeStatement(QueryStatement statement) {
        PlannerMetaLocker locker = new PlannerMetaLocker(session, stmt);
        try {
            locker.lock();
            Analyzer.analyze(statement, session);
        } finally {
            locker.unlock();
        }
    }

    private static class PlanASTPlaceholderBinder extends SPMUpdateExprVisitor<Expr> {
        private final SPMPlaceholderBuilder builder;
        private final Set<Long> userSPMIds = Sets.newHashSet();

        public PlanASTPlaceholderBinder(SPMPlaceholderBuilder builder) {
            this.builder = builder;
        }

        public void bind(QueryRelation query) {
            query.accept(this, null);
            if (!builder.getUserDefineIds().containsAll(userSPMIds) ||
                    !userSPMIds.containsAll(builder.getUserDefineIds())) {
                throw new SemanticException("placeholder conflict");
            }
            for (Long userDefineId : builder.getUserDefineIds()) {
                if (!userSPMIds.contains(userDefineId)) {
                    throw new SemanticException("can't found placeholder: " + userDefineId + " in plan stmt");
                }
            }
            for (Long userSPMId : userSPMIds) {
                if (!builder.getUserDefineIds().contains(userSPMId)) {
                    throw new SemanticException("can't found placeholder: " + userSPMId + " in bind stmt");
                }
            }
        }

        @Override
        public ParseNode visitInPredicate(InPredicate node, Expr parent) {
            if (node.getChildren().stream().anyMatch(SPMFunctions::isSPMFunctions)) {
                return super.visitInPredicate(node, parent);
            }
            Optional<Expr> spm = builder.findPlaceholderExpr(node, node);
            return spm.orElseThrow(() -> new SemanticException(
                    "can't find expression placeholder or there is placeholder conflict : " +
                            node.toMySql()));
        }

        @Override
        public ParseNode visitLiteral(LiteralExpr node, Expr parent) {
            Optional<Expr> spm = builder.findPlaceholderExpr(node, parent);
            return spm.orElseThrow(() -> new SemanticException(
                    "can't find expression placeholder or there is placeholder conflict, expression : " +
                            node.toMySql()));
        }

        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Expr parent) {
            if (SPMFunctions.isSPMFunctions(node)) {
                long spmId = ((IntLiteral) node.getChild(0)).getValue();
                userSPMIds.add(spmId);
                return node;
            }
            return super.visitFunctionCall(node, parent);
        }

        @Override
        public ParseNode visitExpression(Expr node, Expr parent) {
            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                for (int i = 0; i < node.getChildren().size(); i++) {
                    node.setChild(i, visitExpr(node.getChild(i), node));
                }
            }
            return node;
        }
    }
}
