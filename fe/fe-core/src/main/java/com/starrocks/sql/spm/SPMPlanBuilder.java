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

import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.SetVarHint;
import com.starrocks.sql.ast.expression.StringLiteral;
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

// to build plan for bind sql
public class SPMPlanBuilder {
    private final ConnectContext session;
    private final QueryRelation bindStmt;
    private final QueryRelation planStmt;
    private final List<HintNode> planHints;

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
        this.bindStmt = stmt.getBindStmt();
        this.planStmt = stmt.getPlanStmt();
        this.planHints = stmt.getAllQueryScopeHints();
    }

    public SPMPlanBuilder(ConnectContext session, QueryStatement stmt) {
        this.session = session;
        this.bindStmt = null;
        this.planStmt = stmt.getQueryRelation();
        this.planHints = stmt.getAllQueryScopeHints();
    }

    public BaselinePlan execute() {
        analyze();
        parameterizedStmt();
        generatePlan();
        return new BaselinePlan(bindSql, bindSqlDigest, bindSqlHash, planStmtSQL, costs);
    }

    // don't need lock, because we don't need to modify table stats
    public void generatePlan() {
        SessionVariable backupVariable = session.getSessionVariable();
        SessionVariable cloneVariable = null;
        if (planHints != null && !planHints.isEmpty()) {
            cloneVariable = (SessionVariable) backupVariable.clone();
            for (HintNode hint : planHints) {
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
            session.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
        }

        try {
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            TransformerContext transformerContext = new TransformerContext(columnRefFactory, session, null);
            LogicalPlan logicalPlan =
                    new RelationTransformer(transformerContext).transformWithSelectLimit(this.planStmt);

            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            optimizerContext.setOptimizerOptions(
                    new OptimizerOptions(OptimizerOptions.OptimizerStrategy.BASELINE_PLAN));
            Optimizer optimizer = OptimizerFactory.create(optimizerContext);

            OptExpression optimizedPlan = optimizer.optimize(logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));

            SPMPlan2SQLBuilder sqlBuilder = new SPMPlan2SQLBuilder();
            planStmtSQL = sqlBuilder.toSQL(planHints, optimizedPlan, logicalPlan.getOutputColumn());
            costs = optimizedPlan.getCost();
        } finally {
            if (cloneVariable != null) {
                session.setSessionVariable(backupVariable);
            }
        }
    }

    protected void parameterizedStmt() {
        QueryRelation bind;
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder(false);
        if (this.bindStmt != null) {
            // has bind and plan
            builder.findPlaceholder(this.bindStmt);
            bind = builder.insertPlaceholder(this.bindStmt);
            builder.bindPlaceholder(this.planStmt);
        } else {
            // only plan
            bind = builder.insertPlaceholder(this.planStmt);
        }
        SPMAst2SQLBuilder digestBuilder = new SPMAst2SQLBuilder(false, true);
        SPMAst2SQLBuilder sqlBuilder = new SPMAst2SQLBuilder(false, false);
        bindSqlDigest = digestBuilder.build(bind);
        bindSqlHash = digestBuilder.buildHash();
        bindSql = sqlBuilder.build(bind);
    }

    protected void analyze() {
        QueryStatement p = new QueryStatement(this.planStmt);
        try (PlannerMetaLocker locker = new PlannerMetaLocker(session, p)) {
            locker.lock();
            Analyzer.analyze(p, session);
            if (this.bindStmt != null) {
                Analyzer.analyze(new QueryStatement(this.bindStmt), session);
            }
        }
    }
}
