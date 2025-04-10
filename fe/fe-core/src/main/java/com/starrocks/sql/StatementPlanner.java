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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.InsertAnalyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
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
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatementPlanner {
    private static final Logger LOG = LogManager.getLogger(StatementPlanner.class);

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
            } catch (BeginTransactionException | LabelAlreadyUsedException | DuplicatedRequestException | AnalysisException e) {
                throw new SemanticException("fail to begin transaction. " + e.getMessage());
            }
        }

        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, stmt);
        boolean needWholePhaseLock = true;

        // 1. For all queries, we need db lock when analyze phase
        try (ConnectContext.ScopeGuard guard = session.bindScope()) {
            analyzeStatement(stmt, session, dbs);

            // Authorization check
            if (!session.isBypassAuthorizerCheck()) {
                Authorizer.check(stmt, session);
            }
            if (stmt instanceof QueryStatement) {
                OptimizerTraceUtil.logQueryStatement("after analyze:\n%s", (QueryStatement) stmt);
            }

            session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));

            // Note: we only could get the olap table after Analyzing phase
            if (stmt instanceof QueryStatement) {
                QueryStatement queryStmt = (QueryStatement) stmt;
                resultSinkType = queryStmt.hasOutFileClause() ? TResultSinkType.FILE : resultSinkType;
                boolean areTablesCopySafe = AnalyzerUtils.areTablesCopySafe(queryStmt);
                needWholePhaseLock = isLockFree(areTablesCopySafe, session) ? false : true;
                ExecPlan plan;
                if (needWholePhaseLock) {
                    plan = createQueryPlan(queryStmt, session, resultSinkType);
                } else {
                    long planStartTime = OptimisticVersion.generate();
                    unLock(dbs);
                    plan = createQueryPlanWithReTry(queryStmt, session, resultSinkType, planStartTime);
                }
                setOutfileSink(queryStmt, plan);
                return plan;
            } else if (stmt instanceof InsertStmt) {
                return planInsertStmt(dbs, (InsertStmt) stmt, session);
            } else if (stmt instanceof UpdateStmt) {
                return new UpdatePlanner().plan((UpdateStmt) stmt, session);
            } else if (stmt instanceof DeleteStmt) {
                return new DeletePlanner().plan((DeleteStmt) stmt, session);
            }
        } catch (OutOfMemoryError e) {
            LOG.warn("planner out of memory, sql is:" + stmt.getOrigStmt().getOrigStmt());
            throw e;
        } catch (Throwable e) {
            if (stmt instanceof DmlStmt) {
                abortTransaction((DmlStmt) stmt, session, e.getMessage());
            }
            throw e;
        } finally {
            if (needWholePhaseLock) {
                unLock(dbs);
            }
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
        }

        return null;
    }

    /**
     * Analyze the statement.
     * 1. Optimization for INSERT-SELECT: if the SELECT doesn't need the lock, we can defer the lock acquisition
     * after analyzing the SELECT. That can help the case which SELECT is a time-consuming external table access.
     */
    private static void analyzeStatement(StatementBase statement, ConnectContext session,
                                         Map<String, Database> lockDatabases) {
        boolean deferredLock = false;
        Runnable takeLock = () -> {
            try (Timer lockerTime = Tracers.watchScope("Lock")) {
                lock(lockDatabases);
            }
        };
        try (Timer ignored = Tracers.watchScope("Analyzer")) {
            if (statement instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) statement;
                Map<Long, Database> dbs = Maps.newHashMap();
                Map<Long, Set<Long>> tables = Maps.newHashMap();
                PlannerMetaLocker.collectTablesNeedLock(insertStmt.getQueryStatement(), session, dbs, tables);

                if (tables.isEmpty()) {
                    deferredLock = true;
                }
            }

            if (deferredLock) {
                InsertAnalyzer.analyzeWithDeferredLock((InsertStmt) statement, session, takeLock);
            } else {
                takeLock.run();
                Analyzer.analyze(statement, session);
            }
        }
    }

    public static ExecPlan planInsertStmt(Map<String, Database> dbs,
                                          InsertStmt insertStmt,
                                          ConnectContext connectContext) {
        // if use optimistic lock, we will unlock it in InsertPlanner#buildExecPlanWithRetrye
        boolean useOptimisticLock = isLockFreeInsertStmt(insertStmt, connectContext);
        return new InsertPlanner(dbs, useOptimisticLock).plan(insertStmt, connectContext);
    }

    private static boolean isLockFreeInsertStmt(InsertStmt insertStmt,
                                                ConnectContext connectContext) {
        boolean isSelect = !(insertStmt.getQueryStatement().getQueryRelation() instanceof ValuesRelation);
        boolean areTablesCopySafe = AnalyzerUtils.areTablesCopySafe(insertStmt);
        return areTablesCopySafe && isSelect && !connectContext.getSessionVariable().isCboUseDBLock();
    }

    private static boolean isLockFree(boolean areTablesCopySafe, ConnectContext session) {
        // condition can use conflict detection to replace db lock
        // 1. all tables are copy safe
        // 2. cbo_use_lock_db = false
        return areTablesCopySafe && !session.getSessionVariable().isCboUseDBLock();
    }

    /**
     * Create a map from opt expression to parse node for the optimizer to use which only used in text match rewrite for mv.
     */
    public static MVTransformerContext makeMVTransformerContext(SessionVariable sessionVariable) {
        if (sessionVariable.isEnableMaterializedViewTextMatchRewrite()) {
            return new MVTransformerContext();
        }
        return null;
    }

    private static ExecPlan createQueryPlan(StatementBase stmt,
                                            ConnectContext session,
                                            TResultSinkType resultSinkType) {
        QueryStatement queryStmt = (QueryStatement) stmt;
        QueryRelation query = (QueryRelation) queryStmt.getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan;
        MVTransformerContext mvTransformerContext  = makeMVTransformerContext(session.getSessionVariable());

        try (Timer ignored = Tracers.watchScope("Transformer")) {
            // get a logicalPlan without inlining views
            TransformerContext transformerContext = new TransformerContext(columnRefFactory, session, mvTransformerContext);
            logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(query);
        }

        OptExpression root = ShortCircuitPlanner.checkSupportShortCircuitRead(logicalPlan.getRoot(), session);

        OptExpression optimizedPlan;
        try (Timer ignored = Tracers.watchScope("Optimizer")) {
            // 2. Optimize logical plan and build physical plan
            Optimizer optimizer = new Optimizer();
            optimizedPlan = optimizer.optimize(
                    session,
                    root,
                    mvTransformerContext,
                    stmt,
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
                                                    TResultSinkType resultSinkType,
                                                    long planStartTime) {
        QueryRelation query = queryStmt.getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();

        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        boolean isSchemaValid = true;

        // Because we don't hold db lock outer, if the olap table schema change, we need to regenerate the query plan
        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(session, queryStmt);
        session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));
        // TODO: double check relatedMvs for OlapTable
        // the original olapTable in queryStmt had been replaced with the copied olapTable
        Set<OlapTable> olapTables = collectOriginalOlapTables(queryStmt, dbs);
        for (int i = 0; i < Config.max_query_retry_time; ++i) {
            if (!isSchemaValid) {
                planStartTime = OptimisticVersion.generate();
                reAnalyzeStmt(queryStmt, dbs, session);
                colNames = queryStmt.getQueryRelation().getColumnOutputNames();
                isSchemaValid = true;
            }

            LogicalPlan logicalPlan;
            MVTransformerContext mvTransformerContext = makeMVTransformerContext(session.getSessionVariable());
            try (Timer ignored = Tracers.watchScope("Transformer")) {
                // get a logicalPlan without inlining views
                TransformerContext transformerContext = new TransformerContext(columnRefFactory, session, mvTransformerContext);
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
                        mvTransformerContext,
                        queryStmt,
                        new PhysicalPropertySet(),
                        new ColumnRefSet(logicalPlan.getOutputColumn()),
                        columnRefFactory);
            }

            try (Timer ignored = Tracers.watchScope("ExecPlanBuild")) {
                // 3. Build fragment exec plan
                // SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
                // currently only used in Spark/Flink Connector
                // Because the connector sends only simple queries, it only needs to remove the output fragment
                ExecPlan plan = PlanFragmentBuilder.createPhysicalPlan(
                        optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames,
                        resultSinkType,
                        !session.getSessionVariable().isSingleNodeExecPlan());
                final long finalPlanStartTime = planStartTime;
                isSchemaValid = olapTables.stream().allMatch(t -> OptimisticVersion.validateTableUpdate(t,
                        finalPlanStartTime));
                if (isSchemaValid) {
                    return plan;
                }
            }
        }
        List<String> updatedTables = Lists.newArrayList();
        for (OlapTable olapTable : olapTables) {
            if (!OptimisticVersion.validateTableUpdate(olapTable, planStartTime)) {
                updatedTables.add(olapTable.getName());
            }
        }

        throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR,
                "schema of %s had been updated frequently during the plan generation", updatedTables);
    }

    public static Set<OlapTable> collectOriginalOlapTables(StatementBase queryStmt, Map<String, Database> dbs) {
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

    public static Set<OlapTable> reAnalyzeStmt(StatementBase queryStmt, Map<String, Database> dbs,
                                               ConnectContext session) {
        try {
            lock(dbs);
            // analyze to obtain the latest table from metadata
            Analyzer.analyze(queryStmt, session);
            // only copy the latest olap table
            Set<OlapTable> copiedTables = Sets.newHashSet();
            AnalyzerUtils.copyOlapTable(queryStmt, copiedTables);
            return copiedTables;
        } finally {
            unLock(dbs);
        }
    }

    public static void lockDatabases(List<Database> dbs) {
        if (dbs == null) {
            return;
        }
        dbs.sort(Comparator.comparingLong(Database::getId));
        for (Database db : dbs) {
            db.readLock();
        }
    }
    public static void unlockDatabases(Collection<Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs) {
            db.readUnlock();
        }
    }

    // Lock all database before analyze
    public static void lock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        List<Database> dbList = new ArrayList<>(dbs.values());
        lockDatabases(dbList);
    }

    // unLock all database after analyze
    public static void unLock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        unlockDatabases(dbs.values());
    }

    public static boolean tryLock(Map<String, Database> dbs, long timeout, TimeUnit unit) {
        if (dbs == null) {
            return false;
        }
        List<Database> dbList = new ArrayList<>(dbs.values());
        return tryLockDatabases(dbList, timeout, unit);
    }

    public static boolean tryLockDatabases(List<Database> dbs, long timeout, TimeUnit unit) {
        if (dbs == null) {
            return false;
        }
        dbs.sort(Comparator.comparingLong(Database::getId));
        List<Database> lockedDbs = Lists.newArrayList();
        boolean isLockSuccess = false;
        try {
            for (Database db : dbs) {
                if (!db.tryReadLock(timeout, unit)) {
                    return false;
                }
                lockedDbs.add(db);
            }
            isLockSuccess = true;
        } finally {
            if (!isLockSuccess) {
                lockedDbs.stream().forEach(t -> t.readUnlock());
            }
        }
        return true;
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
            throws BeginTransactionException, AnalysisException, LabelAlreadyUsedException, DuplicatedRequestException {
        // not need begin transaction here
        // 1. explain (exclude explain analyze)
        // 2. insert into files
        // 3. old delete
        // 4. insert overwrite
        if (stmt.isExplain() && !StatementBase.ExplainLevel.ANALYZE.equals(stmt.getExplainLevel())) {
            return;
        }
        if (stmt instanceof InsertStmt && ((InsertStmt) stmt).useTableFunctionAsTargetTable()) {
            return;
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
        long txnId = DmlStmt.INVALID_TXN_ID;
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
            txnId = transactionMgr.beginRemoteTransaction(tbl.getSourceTableDbId(), Lists.newArrayList(tbl.getSourceTableId()),
                    label, tbl.getSourceTableHost(), tbl.getSourceTablePort(),
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    sourceType, session.getSessionVariable().getQueryTimeoutS(), authenticateParams);
        } else if (targetTable instanceof SystemTable || targetTable.isIcebergTable() || targetTable.isHiveTable()
                || targetTable.isTableFunctionTable()) {
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

    private static void abortTransaction(DmlStmt stmt, ConnectContext session, String errMsg) {
        long txnId = stmt.getTxnId();
        if (txnId == DmlStmt.INVALID_TXN_ID) {
            return;
        }

        MetaUtils.normalizationTableName(session, stmt.getTableName());
        String catalogName = stmt.getTableName().getCatalog();
        String dbName = stmt.getTableName().getDb();
        String tableName = stmt.getTableName().getTbl();
        Database db = MetaUtils.getDatabase(catalogName, dbName);
        Table targetTable = MetaUtils.getTable(catalogName, dbName, tableName);
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        try {
            if (targetTable instanceof ExternalOlapTable) {
                ExternalOlapTable tbl = (ExternalOlapTable) targetTable;
                transactionMgr.abortRemoteTransaction(tbl.getSourceTableDbId(), txnId, tbl.getSourceTableHost(),
                        tbl.getSourceTablePort(), errMsg, Collections.emptyList(), Collections.emptyList());
            } else if (targetTable instanceof OlapTable) {
                transactionMgr.abortTransaction(
                        db.getId(),
                        txnId,
                        errMsg,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null);
            }
        } catch (Throwable e) {
            // Just print a log if abort txn failed, this failure do not need to pass to user.
            LOG.warn("errors when abort txn", e);
        }
    }
}
