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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.RemoteTransactionMgr;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TransactionState;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;

public class StatementPlanner {

    public static ExecPlan plan(StatementBase stmt, ConnectContext session) {
        if (session instanceof HttpConnectContext) {
            return plan(stmt, session, TResultSinkType.HTTP_PROTOCAL);
        }
        return plan(stmt, session, TResultSinkType.MYSQL_PROTOCAL);
    }

    public static ExecPlan plan(StatementBase stmt, ConnectContext session,
                                TResultSinkType resultSinkType) {
        if (stmt instanceof QueryStatement) {
            OptimizerTraceUtil.logQueryStatement("after parse:\n%s", (QueryStatement) stmt);
        } else if (stmt instanceof DmlStmt) {
            try {
                beginTransaction((DmlStmt) stmt, session);
            } catch (RunningTxnExceedException | LabelAlreadyUsedException | DuplicatedRequestException | AnalysisException |
                     BeginTransactionException e) {
                throw new SemanticException("fail to begin transaction. " + e.getMessage());
            }
        }

        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, stmt);
        boolean needWholePhaseLock = true;

        // 1. For all queries, we need db lock when analyze phase
        Locker locker = new Locker();
        try (var guard = session.bindScope()) {
            lock(locker, dbs);
            try (Timer ignored = Tracers.watchScope("Analyzer")) {
                Analyzer.analyze(stmt, session);
            }

            Authorizer.check(stmt, session);
            if (stmt instanceof QueryStatement) {
                OptimizerTraceUtil.logQueryStatement("after analyze:\n%s", (QueryStatement) stmt);
            }

            session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));

            // Note: we only could get the olap table after Analyzing phase
            boolean isOnlyOlapTableQueries = AnalyzerUtils.isOnlyHasOlapTables(stmt);
            if (isOnlyOlapTableQueries && stmt instanceof QueryStatement) {
                unLock(locker, dbs);
                needWholePhaseLock = false;
                return planQuery(stmt, resultSinkType, session, true);
            }

            if (stmt instanceof QueryStatement) {
                return planQuery(stmt, resultSinkType, session, false);
            } else if (stmt instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) stmt;
                boolean isSelect = !(insertStmt.getQueryStatement().getQueryRelation() instanceof ValuesRelation);
                boolean useOptimisticLock = isOnlyOlapTableQueries && isSelect &&
                        !session.getSessionVariable().isCboUseDBLock();
                if (useOptimisticLock) {
                    unLock(locker, dbs);
                    needWholePhaseLock = false;
                }
                return new InsertPlanner(dbs, useOptimisticLock).plan((InsertStmt) stmt, session);
            } else if (stmt instanceof UpdateStmt) {
                return new UpdatePlanner().plan((UpdateStmt) stmt, session);
            } else if (stmt instanceof DeleteStmt) {
                return new DeletePlanner().plan((DeleteStmt) stmt, session);
            }
        } finally {
            if (needWholePhaseLock) {
                unLock(locker, dbs);
            }
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
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
        if (!isOnlyOlapTable || !GlobalStateMgr.getCurrentState().isLeader() || session.getSessionVariable().isCboUseDBLock()) {
            plan = createQueryPlan(queryStmt.getQueryRelation(), session, resultSinkType);
        } else {
            plan = createQueryPlanWithReTry(queryStmt, session, resultSinkType);
        }
        setOutfileSink(queryStmt, plan);
        return plan;
    }

    private static ExecPlan createQueryPlan(Relation relation,
                                            ConnectContext session,
                                            TResultSinkType resultSinkType) {
        QueryRelation query = (QueryRelation) relation;
        List<String> colNames = query.getColumnOutputNames();
        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan;

        try (Timer ignored = Tracers.watchScope("Transformer")) {
            // get a logicalPlan without inlining views
            TransformerContext transformerContext = new TransformerContext(columnRefFactory, session);
            logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(query);
        }

        OptExpression optimizedPlan;
        try (Timer ignored = Tracers.watchScope("Optimizer")) {
            // 2. Optimize logical plan and build physical plan
            Optimizer optimizer = new Optimizer();
            optimizedPlan = optimizer.optimize(
                    session,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);
        }
        try (Timer ignored = Tracers.watchScope("ExecPlanBuild")) {
            // 3. Build fragment exec plan
            /*
             * SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
             * currently only used in Spark/Flink Connector
             * Because the connector sends only simple queries, it only needs to remove the output fragment
             */
            return PlanFragmentBuilder.createPhysicalPlan(
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
            long planStartTime = OptimisticVersion.generate();
            if (!isSchemaValid) {
                reAnalyzeStmt(queryStmt, dbs, session);
                colNames = query.getColumnOutputNames();
            }

            LogicalPlan logicalPlan;
            try (Timer ignored = Tracers.watchScope("Transformer")) {
                // get a logicalPlan without inlining views
                TransformerContext transformerContext = new TransformerContext(columnRefFactory, session);
                logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(query);
            }

            OptExpression root = ShortCircuitPlanner.checkSupportShortCircuitRead(logicalPlan.getRoot(), session);

            OptExpression optimizedPlan;
            try (Timer ignored = Tracers.watchScope("Optimizer")) {
                // 2. Optimize logical plan and build physical plan
                Optimizer optimizer = new Optimizer();
                // FIXME: refactor this into Optimizer.optimize() method.
                // set query tables into OptimizeContext so can be added for mv rewrite
                if (Config.skip_whole_phase_lock_mv_limit >= 0) {
                    optimizer.setQueryTables(olapTables);
                }
                optimizedPlan = optimizer.optimize(
                        session,
                        root,
                        new PhysicalPropertySet(),
                        new ColumnRefSet(logicalPlan.getOutputColumn()),
                        columnRefFactory);
            }

            try (Timer ignored = Tracers.watchScope("ExecPlanBuild")) {
                // 3. Build fragment exec plan
                /*
                 * SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
                 * currently only used in Spark/Flink Connector
                 * Because the connector sends only simple queries, it only needs to remove the output fragment
                 */
                // For only olap table queries, we need to lock db here.
                // Because we need to ensure multi partition visible versions are consistent.
                long buildFragmentStartTime = OptimisticVersion.generate();
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

                // if exists table is applying visible log, we wait 10 ms to retry
                if (olapTables.stream().anyMatch(t -> t.lastVersionUpdateStartTime.get() > t.lastVersionUpdateEndTime.get())) {
                    try (Timer timer = Tracers.watchScope("PlanRetrySleepTime")) {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new StarRocksPlannerException("query had been interrupted", INTERNAL_ERROR);
                    }
                }

            }
        }
        Preconditions.checkState(false, "The tablet write operation update metadata " +
                "take a long time");
        return null;
    }

    public static Set<OlapTable> collectOriginalOlapTables(StatementBase queryStmt, Map<String, Database> dbs) {
        Set<OlapTable> olapTables = Sets.newHashSet();
        Locker locker = new Locker();
        try {
            // Need lock to avoid olap table metas ConcurrentModificationException
            lock(locker, dbs);
            AnalyzerUtils.copyOlapTable(queryStmt, olapTables);
            return olapTables;
        } finally {
            unLock(locker, dbs);
        }
    }

    public static Set<OlapTable> reAnalyzeStmt(StatementBase queryStmt, Map<String, Database> dbs,
                                               ConnectContext session) {
        Locker locker = new Locker();
        try {
            lock(locker, dbs);
            // analyze to obtain the latest table from metadata
            Analyzer.analyze(queryStmt, session);
            // only copy the latest olap table
            Set<OlapTable> copiedTables = Sets.newHashSet();
            AnalyzerUtils.copyOlapTable(queryStmt, copiedTables);
            return copiedTables;
        } finally {
            unLock(locker, dbs);
        }
    }

    // Lock all database before analyze
    public static void lock(Locker locker, Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        List<Database> dbList = new ArrayList<>(dbs.values());
        dbList.sort(Comparator.comparingLong(Database::getId));
        for (Database db : dbList) {
            locker.lockDatabase(db, LockType.READ);
        }
    }

    // unLock all database after analyze
    public static void unLock(Locker locker, Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            locker.unLockDatabase(db, LockType.READ);
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

        List<String> columnOutputNames = queryStmt.getQueryRelation().getColumnOutputNames();
        if (columnOutputNames.size() != plan.getOutputExprs().size()) {
            throw new RuntimeException(String.format("output column names size isn't equal output exprs size, %d vs %d",
                    columnOutputNames.size(), plan.getOutputExprs().size()));
        }
        ResultSink resultSink = (ResultSink) topFragment.getSink();
        resultSink.setOutfileInfo(queryStmt.getOutFileClause(), columnOutputNames);
    }

    private static void beginTransaction(DmlStmt stmt, ConnectContext session)
            throws BeginTransactionException, RunningTxnExceedException, AnalysisException, LabelAlreadyUsedException,
            DuplicatedRequestException {
        // not need begin transaction here
        // 1. explain (exclude explain analyze)
        // 2. insert into files
        // 3. old delete
        // 4. insert overwrite
        if (stmt.isExplain() && !StatementBase.ExplainLevel.ANALYZE.equals(stmt.getExplainLevel())) {
            return;
        }
        if (stmt instanceof InsertStmt) {
            if (((InsertStmt) stmt).useTableFunctionAsTargetTable() ||
                    ((InsertStmt) stmt).useBlackHoleTableAsTargetTable()) {
                return;
            }
        }

        MetaUtils.normalizationTableName(session, stmt.getTableName());
        String catalogName = stmt.getTableName().getCatalog();
        String dbName = stmt.getTableName().getDb();
        String tableName = stmt.getTableName().getTbl();
        Database db = MetaUtils.getDatabase(catalogName, dbName);
        Table targetTable = MetaUtils.getTable(catalogName, dbName, tableName);
        if (stmt instanceof DeleteStmt && targetTable instanceof OlapTable &&
                ((OlapTable) targetTable).getKeysType() != KeysType.PRIMARY_KEYS) {
            return;
        }
        if (stmt instanceof InsertStmt && ((InsertStmt) stmt).isOverwrite() &&
                !((InsertStmt) stmt).hasOverwriteJob() &&
                !(targetTable.isIcebergTable() || targetTable.isHiveTable())) {
            return;
        }

        String label;
        if (stmt instanceof InsertStmt) {
            String stmtLabel = ((InsertStmt) stmt).getLabel();
            label = Strings.isNullOrEmpty(stmtLabel) ? MetaUtils.genInsertLabel(session.getExecutionId()) : stmtLabel;
        } else if (stmt instanceof UpdateStmt) {
            label = MetaUtils.genUpdateLabel(session.getExecutionId());
        } else if (stmt instanceof DeleteStmt) {
            label = MetaUtils.genDeleteLabel(session.getExecutionId());
        } else {
            throw UnsupportedException.unsupportedException(
                    "Unsupported dml statement " + stmt.getClass().getSimpleName());
        }

        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        long txnId = -1L;
        if (targetTable instanceof ExternalOlapTable) {
            if (!(stmt instanceof InsertStmt)) {
                throw UnsupportedException.unsupportedException("External OLAP table only supports insert statement");
            }
            // sync OLAP external table meta here,
            // because beginRemoteTransaction will use the dbId and tableId as request param.
            ExternalOlapTable tbl = MetaUtils.syncOLAPExternalTableMeta((ExternalOlapTable) targetTable);
            ((InsertStmt) stmt).setTargetTable(tbl);
            TAuthenticateParams authenticateParams = new TAuthenticateParams();
            authenticateParams.setUser(tbl.getSourceTableUser());
            authenticateParams.setPasswd(tbl.getSourceTablePassword());
            authenticateParams.setHost(session.getRemoteIP());
            authenticateParams.setDb_name(tbl.getSourceTableDbName());
            authenticateParams.setTable_names(Lists.newArrayList(tbl.getSourceTableName()));
            txnId = RemoteTransactionMgr.beginTransaction(
                    tbl.getSourceTableDbId(),
                    Lists.newArrayList(tbl.getSourceTableId()),
                    label,
                    sourceType,
                    session.getSessionVariable().getQueryTimeoutS(),
                    tbl.getSourceTableHost(),
                    tbl.getSourceTablePort(),
                    authenticateParams);
        } else if (targetTable instanceof SystemTable || targetTable.isIcebergTable() || targetTable.isHiveTable()
                || targetTable.isTableFunctionTable() || targetTable.isBlackHoleTable()) {
            // schema table and iceberg and hive table does not need txn
        } else {
            long dbId = db.getId();
            txnId = transactionMgr.beginTransaction(
                    dbId,
                    Lists.newArrayList(targetTable.getId()),
                    label,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                            FrontendOptions.getLocalHostAddress()),
                    sourceType,
                    session.getSessionVariable().getQueryTimeoutS());

            // add table indexes to transaction state
            if (targetTable instanceof OlapTable) {
                TransactionState txnState = transactionMgr.getTransactionState(dbId, txnId);
                txnState.addTableIndexes((OlapTable) targetTable);
            }
        }

        stmt.setTxnId(txnId);
    }
}
