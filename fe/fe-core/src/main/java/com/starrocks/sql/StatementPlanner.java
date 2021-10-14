// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql;

import com.google.common.collect.Maps;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PlannerContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.analyzer.relation.QueryRelation;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatementPlanner {
    public ExecPlan plan(StatementBase stmt, ConnectContext session) throws AnalysisException {
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(session.getCatalog(), session);
        Relation relation = analyzer.analyze(stmt);

        PrivilegeChecker.check(stmt, session.getCatalog().getAuth(), session);

        if (stmt instanceof QueryStmt) {
            QueryStmt queryStmt = (QueryStmt) stmt;

            Map<String, Database> dbs = Maps.newTreeMap();
            queryStmt.getDbs(session, dbs);

            try {
                lock(dbs);
                session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));
                return createQueryPlan(relation, session);
            } finally {
                unLock(dbs);
            }
        } else if (stmt instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) stmt;
            Map<String, Database> dbs = Maps.newTreeMap();
            insertStmt.getDbs(session, dbs);

            try {
                lock(dbs);
                return createInsertPlan(relation, session);
            } finally {
                unLock(dbs);
            }
        }
        return null;
    }

    private ExecPlan createQueryPlan(Relation relation, ConnectContext session) {
        QueryRelation query = (QueryRelation) relation;
        List<String> colNames = query.getColumnOutputNames();

        //1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory).transform(query);

        //2. Optimize logical plan and build physical plan
        Optimizer optimizer = new Optimizer();
        OptExpression optimizedPlan = optimizer.optimize(
                session,
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);

        //3. Build fragment exec plan
        PlannerContext plannerContext = new PlannerContext(null, null, session.getSessionVariable().toThrift(), null);
        return new PlanFragmentBuilder().createPhysicalPlan(
                optimizedPlan, plannerContext, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames);
    }

    private ExecPlan createInsertPlan(Relation relation, ConnectContext session) {
        return new InsertPlanner().plan(relation, session);
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
}
