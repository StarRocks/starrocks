// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql;

import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.analysis.ShowWorkGroupStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
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

    public ExecPlan plan(StatementBase stmt, ConnectContext session, TResultSinkType resultSinkType)
            throws AnalysisException {
        if (stmt instanceof QueryStatement) {
            OptimizerTraceUtil.logQueryStatement(session, "after parse:\n%s", (QueryStatement) stmt);
        }
        Analyzer.analyze(stmt, session);
        PrivilegeChecker.check(stmt, session);
        if (stmt instanceof QueryStatement) {
            OptimizerTraceUtil.logQueryStatement(session, "after analyze:\n%s", (QueryStatement) stmt);
        }

        if (stmt instanceof QueryStatement) {
            Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, stmt);
            QueryStatement queryStmt = (QueryStatement) stmt;
            try {
                lock(dbs);
                session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));
                resultSinkType = queryStmt.hasOutFileClause() ? TResultSinkType.FILE : resultSinkType;
                ExecPlan plan = createQueryPlan(queryStmt.getQueryRelation(), session, resultSinkType);
                setOutfileSink(queryStmt, plan);

                return plan;
            } finally {
                unLock(dbs);
            }
        } else if (stmt instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) stmt;
            Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, insertStmt);
            try {
                lock(dbs);
                return new InsertPlanner().plan((InsertStmt) stmt, session);
            } finally {
                unLock(dbs);
            }
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

    public static boolean supportedByNewParser(StatementBase statement) {
        return statement instanceof AlterViewStmt
                || statement instanceof CreateTableAsSelectStmt
                || statement instanceof CreateViewStmt
                || statement instanceof InsertStmt
                || statement instanceof QueryStmt
                || statement instanceof QueryStatement
                || statement instanceof ShowDbStmt
                || statement instanceof ShowTableStmt;
    }

    public static boolean supportedByNewAnalyzer(StatementBase statement) {
        return statement instanceof AlterViewStmt
                || statement instanceof AlterWorkGroupStmt
                || statement instanceof CreateTableAsSelectStmt
                || statement instanceof CreateViewStmt
                || statement instanceof CreateWorkGroupStmt
                || statement instanceof DropWorkGroupStmt
                || statement instanceof InsertStmt
                || statement instanceof QueryStatement
                || statement instanceof ShowColumnStmt
                || statement instanceof ShowDbStmt
                || statement instanceof ShowTableStmt
                || statement instanceof ShowTableStatusStmt
                || statement instanceof ShowVariablesStmt
                || statement instanceof ShowWorkGroupStmt;
    }
}
