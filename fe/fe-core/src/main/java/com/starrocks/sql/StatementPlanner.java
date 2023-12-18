// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StatementPlanner {

    public static ExecPlan plan(StatementBase stmt, ConnectContext session) {
        return plan(stmt, session, TResultSinkType.MYSQL_PROTOCAL);
    }

    public static ExecPlan plan(StatementBase stmt, ConnectContext session, TResultSinkType resultSinkType) {
        if (stmt instanceof QueryStatement) {
            OptimizerTraceUtil.logQueryStatement(session, "after parse:\n%s", (QueryStatement) stmt);
        }

        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, stmt);
        boolean needWholePhaseLock = true;

        try {
            lock(dbs);
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Analyzer")) {
                Analyzer.analyze(stmt, session);
            }

            PrivilegeChecker.check(stmt, session);
            if (stmt instanceof QueryStatement) {
                OptimizerTraceUtil.logQueryStatement(session, "after analyze:\n%s", (QueryStatement) stmt);
            }

            session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));

            // Note: we only could get the olap table after Analyzing phase
            boolean isOnlyOlapTableQueries = AnalyzerUtils.isOnlyHasOlapTables(stmt);
            if (isOnlyOlapTableQueries && stmt instanceof QueryStatement) {
                unLock(dbs);
                needWholePhaseLock = false;
                return planQuery(stmt, resultSinkType, session, true);
            }

            if (stmt instanceof QueryStatement) {
                return planQuery(stmt, resultSinkType, session, false);
            } else if (stmt instanceof InsertStmt) {
                return new InsertPlanner().plan((InsertStmt) stmt, session);
            } else if (stmt instanceof UpdateStmt) {
                return new UpdatePlanner().plan((UpdateStmt) stmt, session);
            } else if (stmt instanceof DeleteStmt) {
                return new DeletePlanner().plan((DeleteStmt) stmt, session);
            }
        } finally {
            if (needWholePhaseLock) {
                unLock(dbs);
            }
        }
        return null;
    }

    private static ExecPlan planQuery(StatementBase stmt,
                                      TResultSinkType resultSinkType,
                                      ConnectContext session,
                                      boolean isOnlyOlapTable) {
        QueryStatement queryStmt = (QueryStatement) stmt;
        resultSinkType = queryStmt.hasOutFileClause() ? TResultSinkType.FILE : resultSinkType;
        ExecPlan plan;
        if (!isOnlyOlapTable || session.getSessionVariable().isCboUseDBLock()) {
            plan = createQueryPlan(queryStmt.getQueryRelation(), session, resultSinkType);
        } else {
            plan = createQueryPlanWithReTry(queryStmt, session, resultSinkType);
        }
        setOutfileSink(queryStmt, plan);
        return plan;
    }

    public static ExecPlan createQueryPlan(Relation relation, ConnectContext session, TResultSinkType resultSinkType) {
        QueryRelation query = (QueryRelation) relation;
        List<String> colNames = query.getColumnOutputNames();
        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transformWithSelectLimit(query);

        OptExpression optimizedPlan;
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Optimizer")) {
            // 2. Optimize logical plan and build physical plan
            Optimizer optimizer = new Optimizer();
            optimizedPlan = optimizer.optimize(
                    session,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);
        }
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("ExecPlanBuild")) {
            // 3. Build fragment exec plan
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
    }

    public static ExecPlan createQueryPlanWithReTry(QueryStatement queryStmt,
                                                    ConnectContext session,
                                                    TResultSinkType resultSinkType) {
        QueryRelation query = queryStmt.getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();

        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        boolean isSchemaValid = true;

        // Because we don't hold db lock outer, if the olap table schema change, we need to regenerate the query plan
        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, queryStmt);
        session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));
        // TODO: double check relatedMvs for OlapTable
        // only collect once to save the original olapTable info
        Set<OlapTable> olapTables = collectOriginalOlapTables(queryStmt, dbs);
        for (int i = 0; i < Config.max_query_retry_time; ++i) {
            long planStartTime = System.nanoTime();
            if (!isSchemaValid) {
                colNames = reAnalyzeStmt(queryStmt, dbs, session);
            }

            LogicalPlan logicalPlan;
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Transformer")) {
                logicalPlan = new RelationTransformer(columnRefFactory, session).transformWithSelectLimit(query);
            }

            OptExpression optimizedPlan;
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Optimizer")) {
                // 2. Optimize logical plan and build physical plan
                Optimizer optimizer = new Optimizer();
                optimizedPlan = optimizer.optimize(
                        session,
                        logicalPlan.getRoot(),
                        new PhysicalPropertySet(),
                        new ColumnRefSet(logicalPlan.getOutputColumn()),
                        columnRefFactory);
            }
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("ExecPlanBuild")) {
                // 3. Build fragment exec plan
                /*
                 * SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
                 * currently only used in Spark/Flink Connector
                 * Because the connector sends only simple queries, it only needs to remove the output fragment
                 */
                // For only olap table queries, we need to lock db here.
                // Because we need to ensure multi partition visible versions are consistent.
                long buildFragmentStartTime = System.nanoTime();
                ExecPlan plan = PlanFragmentBuilder.createPhysicalPlan(
                        optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames,
                        resultSinkType,
                        !session.getSessionVariable().isSingleNodeExecPlan());
                isSchemaValid = olapTables.stream().noneMatch(t -> t.lastSchemaUpdateTime.get() > planStartTime);
                isSchemaValid = isSchemaValid && olapTables.stream().allMatch(t ->
                        t.lastVersionUpdateEndTime.get() < buildFragmentStartTime &&
                                t.lastVersionUpdateEndTime.get() >= t.lastVersionUpdateStartTime.get());
                if (isSchemaValid) {
                    return plan;
                }
            }
        }
        Preconditions.checkState(false, "The tablet write operation update metadata " +
                "take a long time");
        return null;
    }

    private static Set<OlapTable> collectOriginalOlapTables(QueryStatement queryStmt, Map<String, Database> dbs) {
        Set<OlapTable> olapTables = Sets.newHashSet();
        try {
            // Need lock to avoid olap table metas ConcurrentModificationException
            lock(dbs);
            AnalyzerUtils.copyOlapTable(queryStmt, olapTables);
            return olapTables;
        } finally {
            unLock(dbs);
        }
    }

    public static List<String> reAnalyzeStmt(QueryStatement queryStmt, Map<String, Database> dbs, ConnectContext session) {
        try {
            lock(dbs);
            Analyzer.analyze(queryStmt, session);
            // only copy olap table
            AnalyzerUtils.copyOlapTable(queryStmt, Sets.newHashSet());
            return queryStmt.getQueryRelation().getColumnOutputNames();
        } finally {
            unLock(dbs);
        }
    }

    // Lock all database before analyze
    public static void lock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        List<Database> dbList = new ArrayList<>(dbs.values());
        dbList.sort(Comparator.comparingLong(Database::getId));
        for (Database db : dbList) {
            db.readLock();
        }
    }

    // unLock all database after analyze
    public static void unLock(Map<String, Database> dbs) {
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
        PlanFragment topFragment = plan.getTopFragment();
        if (!(topFragment.getSink() instanceof ResultSink)) {
            return;
        }

        ResultSink resultSink = (ResultSink) topFragment.getSink();
        resultSink.setOutfileInfo(queryStmt.getOutFileClause());
    }
}
