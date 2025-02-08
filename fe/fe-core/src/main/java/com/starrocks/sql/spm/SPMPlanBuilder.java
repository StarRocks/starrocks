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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
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

import java.time.LocalDateTime;

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
        formatStmt();
        generatePlan();

        BaselinePlan baselinePlan = new BaselinePlan();
        baselinePlan.bindSqlDigest = bindSqlDigest;
        baselinePlan.bindSqlHash = bindSqlHash;
        baselinePlan.bindSql = bindSql;
        baselinePlan.planSql = planStmtSQL;
        baselinePlan.costs = costs;
        baselinePlan.updateTime = LocalDateTime.now();

        return baselinePlan;
    }

    // don't need lock, because we don't need to modify table stats
    public void generatePlan() {
        QueryRelation query = this.stmt.getPlanStmt();

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        TransformerContext transformerContext = new TransformerContext(columnRefFactory, session, null);
        LogicalPlan logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(query);

        OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
        optimizerContext.setOptimizerOptions(new OptimizerOptions(OptimizerOptions.OptimizerStrategy.BASELINE_PLAN));
        Optimizer optimizer = OptimizerFactory.create(optimizerContext);

        OptExpression optimizedPlan = optimizer.optimize(logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));

        SPMPlan2SQLBuilder sqlBuilder = new SPMPlan2SQLBuilder();
        planStmtSQL = sqlBuilder.toSQL(columnRefFactory, optimizedPlan);
        costs = optimizedPlan.getCost();
    }

    protected void formatStmt() {
        if (this.stmt.getBindStmt() != null) {
            // pass
            // has bind and plan
        } else {
            // only plan
            SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
            QueryRelation plan = builder.build(this.stmt.getPlanStmt());
            SPMAst2SQLBuilder digestBuilder = new SPMAst2SQLBuilder(false, true);
            SPMAst2SQLBuilder sqlBuilder = new SPMAst2SQLBuilder(false, false);
            bindSqlDigest = digestBuilder.build(plan);
            bindSqlHash = digestBuilder.buildHash();
            bindSql = sqlBuilder.build(plan);
        }
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
}
