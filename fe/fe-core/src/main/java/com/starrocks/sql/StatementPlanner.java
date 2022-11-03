// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql;

import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DmlStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.ShowAuthenticationStmt;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.analysis.ShowWorkGroupStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantImpersonateStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.RevokeImpersonateStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.ShowAnalyzeStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.UseStmt;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatementPlanner {

    public ExecPlan plan(StatementBase stmt, ConnectContext session) throws AnalysisException {
        if (stmt instanceof QueryStatement) {
            OptimizerTraceUtil.logQueryStatement(session, "after parse:\n%s", (QueryStatement) stmt);
        }

        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, stmt);
        try {
            lock(dbs);
            Analyzer.analyze(stmt, session);
            PrivilegeChecker.check(stmt, session);
            if (stmt instanceof QueryStatement) {
                QueryStatement queryStmt = (QueryStatement) stmt;
                OptimizerTraceUtil.logQueryStatement(session, "after analyze:\n%s", queryStmt);

                session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));
                TResultSinkType resultSinkType =
                        queryStmt.hasOutFileClause() ? TResultSinkType.FILE : TResultSinkType.MYSQL_PROTOCAL;
                ExecPlan plan = createQueryPlan(queryStmt.getQueryRelation(), session, resultSinkType);
                setOutfileSink(queryStmt, plan);
                return plan;
            } else if (stmt instanceof InsertStmt) {
                return new InsertPlanner().plan((InsertStmt) stmt, session);
            } else if (stmt instanceof UpdateStmt) {
                return new UpdatePlanner().plan((UpdateStmt) stmt, session);
            } else if (stmt instanceof DeleteStmt) {
                return new DeletePlanner().plan((DeleteStmt) stmt, session);
            }
        } finally {
            unLock(dbs);
        }
        return null;
    }

    private ExecPlan createQueryPlan(Relation relation, ConnectContext session, TResultSinkType resultSinkType) {
        QueryRelation query = (QueryRelation) relation;
        List<String> colNames = query.getColumnOutputNames();

        //1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transformWithSelectLimit(query);

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline =
                isEnablePipeline && ResultSink.canUsePipeLine(resultSinkType) && logicalPlan.canUsePipeline();
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }

            //2. Optimize logical plan and build physical plan
            Optimizer optimizer = new Optimizer();
            OptExpression optimizedPlan = optimizer.optimize(
                    session,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            //3. Build fragment exec plan
            /*
             * SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
             * currently only used in Spark/Flink Connector
             * Because the connector sends only simple queries, it only needs to remove the output fragment
             */
            if (session.getSessionVariable().isSingleNodeExecPlan()) {
                return new PlanFragmentBuilder().createPhysicalPlanWithoutOutputFragment(
                        optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames);
            } else {
                return new PlanFragmentBuilder().createPhysicalPlan(
                        optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames);
            }
        } finally {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    // Lock all database before analyze
    private void lock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readLock();
        }
    }

    // unLock all database after analyze
    private void unLock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readUnlock();
        }
    }

    // if query stmt has OUTFILE clause, set info into ResultSink.
    // this should be done after fragments are generated.
    private void setOutfileSink(QueryStatement queryStmt, ExecPlan plan) {
        if (!queryStmt.hasOutFileClause()) {
            return;
        }
        PlanFragment topFragment = plan.getFragments().get(0);
        if (!(topFragment.getSink() instanceof ResultSink)) {
            return;
        }

        ResultSink resultSink = (ResultSink) topFragment.getSink();
        resultSink.setOutfileInfo(queryStmt.getOutFileClause());
    }

    public static boolean supportedByNewPlanner(StatementBase statement) {
        return AlterTableStmt.isSupportNewPlanner(statement)
                || AlterSystemStmt.isSupportNewPlanner(statement)
                || statement instanceof AdminSetConfigStmt
                || statement instanceof AdminSetReplicaStatusStmt
                || statement instanceof AlterViewStmt
                || statement instanceof AlterWorkGroupStmt
                || statement instanceof AnalyzeStmt
                || statement instanceof CreateAnalyzeJobStmt
                || statement instanceof CreateCatalogStmt
                || statement instanceof CreateTableStmt
                || statement instanceof CreateTableAsSelectStmt
                || statement instanceof CreateMaterializedViewStatement
                || statement instanceof CreateMaterializedViewStmt
                || statement instanceof CreateViewStmt
                || statement instanceof CreateWorkGroupStmt
                || statement instanceof DmlStmt
                || statement instanceof DropAnalyzeJobStmt
                || statement instanceof DropCatalogStmt
                || statement instanceof DropMaterializedViewStmt
                || statement instanceof DropTableStmt
                || statement instanceof DropWorkGroupStmt
                || statement instanceof ExecuteAsStmt
                || statement instanceof GrantImpersonateStmt
                || statement instanceof GrantRoleStmt
                || statement instanceof QueryStatement
                || statement instanceof RefreshTableStmt
                || statement instanceof RevokeImpersonateStmt
                || statement instanceof RevokeRoleStmt
                || statement instanceof RefreshTableStmt
                || statement instanceof ShowAnalyzeStmt
                || statement instanceof ShowAuthenticationStmt
                || statement instanceof ShowCatalogsStmt
                || statement instanceof ShowColumnStmt
                || statement instanceof ShowDbStmt
                || statement instanceof ShowMaterializedViewStmt
                || statement instanceof ShowTableStmt
                || statement instanceof ShowTableStatusStmt
                || statement instanceof ShowVariablesStmt
                || statement instanceof ShowWorkGroupStmt
                || statement instanceof SubmitTaskStmt
                || statement instanceof UseStmt;
    }
}
