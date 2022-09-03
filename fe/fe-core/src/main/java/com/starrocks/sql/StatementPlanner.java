// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql;

import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.catalog.Database;
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

    public static ExecPlan plan(StatementBase stmt, ConnectContext session) {
        return plan(stmt, session, true, TResultSinkType.MYSQL_PROTOCAL);
    }

    public static ExecPlan plan(StatementBase stmt, ConnectContext session, boolean lockDb, TResultSinkType resultSinkType) {
        if (stmt instanceof QueryStatement) {
            OptimizerTraceUtil.logQueryStatement(session, "after parse:\n%s", (QueryStatement) stmt);
        }

        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, stmt);
        Map<String, Database> dbLocks = null;
        if (lockDb) {
            dbLocks = dbs;
        }
        try {
            lock(dbLocks);
            Analyzer.analyze(stmt, session);
            PrivilegeChecker.check(stmt, session);
            if (stmt instanceof QueryStatement) {
                OptimizerTraceUtil.logQueryStatement(session, "after analyze:\n%s", (QueryStatement) stmt);
            }

            if (stmt instanceof QueryStatement) {
                session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));
                ExecPlan plan = createQueryPlan(((QueryStatement) stmt).getQueryRelation(), session, resultSinkType);
                setOutfileSink((QueryStatement) stmt, plan);

                return plan;
            } else if (stmt instanceof InsertStmt) {
                return new InsertPlanner().plan((InsertStmt) stmt, session);
            } else if (stmt instanceof UpdateStmt) {
                return new UpdatePlanner().plan((UpdateStmt) stmt, session);
            } else if (stmt instanceof DeleteStmt) {
                return new DeletePlanner().plan((DeleteStmt) stmt, session);
            }
        } finally {
            unLock(dbLocks);
        }
        return null;
    }


    public static ExecPlan createQueryPlan(Relation relation, ConnectContext session, TResultSinkType resultSinkType) {
        QueryRelation query = (QueryRelation) relation;
        List<String> colNames = query.getColumnOutputNames();

        //1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transformWithSelectLimit(query);

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
        return new PlanFragmentBuilder().createPhysicalPlan(
                optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames,
                resultSinkType,
                !session.getSessionVariable().isSingleNodeExecPlan());
    }

    // Lock all database before analyze
    private static void lock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readLock();
        }
    }

    // unLock all database after analyze
    private static void unLock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readUnlock();
        }
    }

    // if query stmt has OUTFILE clause, set info into ResultSink.
    // this should be done after fragments are generated.
    private static void setOutfileSink(QueryStatement queryStmt, ExecPlan plan) {
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
                || statement.isSupportNewPlanner();
    }
}
