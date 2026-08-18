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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
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
import com.starrocks.sql.analyzer.CommonSubqueryCTEHoister;
import com.starrocks.sql.analyzer.InsertAnalyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.statistics.StatisticsLoadBudget;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.sql.spm.SPMPlanner;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.ExplicitTxnStatementValidator;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.RemoteTransactionMgr;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.qe.StmtExecutor.buildExplainString;

public class StatementPlanner {
    private static final Logger LOG = LogManager.getLogger(StatementPlanner.class);

    public static ExecPlan plan(StatementBase stmt, ConnectContext session) {
        if (session instanceof HttpConnectContext) {
            return plan(stmt, session, TResultSinkType.HTTP_PROTOCAL);
        } else if (session.isArrowFlightSql()) {
            return plan(stmt, session, TResultSinkType.ARROW_FLIGHT_PROTOCAL);
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

        boolean needWholePhaseLock = true;
        PlannerMetaLocker plannerMetaLocker = null;
        // Common-subquery hoisting rewrites the parsed statement in place, because there is no general AST
        // copier in the FE. Undo it once planning is done so that the parsed tree callers keep hold of - the
        // audit log, query dump and any re-planning - is the one the parser produced, per the AST immutability
        // rule in fe/AGENTS.md. The ExecPlan is already built by then and does not reference the AST.
        CommonSubqueryCTEHoister.HoistRecord[] hoisted = new CommonSubqueryCTEHoister.HoistRecord[1];
        // 1. For all queries, we need db lock when analyze phase
        try (var guard = session.bindScope();
                var ignoredBudget = StatisticsLoadBudget.openScope(session)) {
            SPMPlanner spmPlanner = new SPMPlanner(session);
            stmt = spmPlanner.plan(stmt);

            plannerMetaLocker = new PlannerMetaLocker(session, stmt);
            // Analyze
            analyzeStatement(stmt, session, plannerMetaLocker, hoisted);

            // Authorization check
            if (!session.isBypassAuthorizerCheck()) {
                Authorizer.check(stmt, session);
            }
            if (stmt instanceof QueryStatement) {
                OptimizerTraceUtil.logQueryStatement("after analyze:\n%s", (QueryStatement) stmt);
            }

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
                    unLock(plannerMetaLocker);
                    plan = createQueryPlanWithReTry(queryStmt, session, resultSinkType, plannerMetaLocker, planStartTime);
                }
                if (spmPlanner.getBaseline() != null) {
                    plan.setUseBaseline(spmPlanner.getBaseline().getId());
                }
                setOutfileSink(queryStmt, plan);
                setExplainToQueryDetail(plan, stmt, session, ResourceGroupClassifier.QueryType.SELECT);
                return plan;
            } else if (stmt instanceof InsertStmt) {
                ExecPlan plan = planInsertStmt(plannerMetaLocker, (InsertStmt) stmt, session);
                setExplainToQueryDetail(plan, stmt, session, ResourceGroupClassifier.QueryType.INSERT);
                return plan;
            } else if (stmt instanceof UpdateStmt) {
                return new UpdatePlanner().plan((UpdateStmt) stmt, session);
            } else if (stmt instanceof DeleteStmt) {
                return new DeletePlanner().plan((DeleteStmt) stmt, session);
            } else if (stmt instanceof MergeIntoStmt) {
                return new MergeIntoPlanner().plan((MergeIntoStmt) stmt, session);
            }
        } catch (OutOfMemoryError e) {
            LOG.warn("planner out of memory, sql is:" + stmt.getOrigStmt().getOrigStmt());
            throw e;
        } catch (Throwable e) {
            if (stmt instanceof DmlStmt) {
                //If it is an explicit transaction, the transaction will not be aborted automatically.
                if (session.getTxnId() == 0) {
                    abortTransaction((DmlStmt) stmt, session, e.getMessage());
                }
            }
            throw e;
        } finally {
            if (hoisted[0] != null) {
                hoisted[0].revert();
            }
            if (needWholePhaseLock && plannerMetaLocker != null) {
                unLock(plannerMetaLocker);
            }
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
        }

        return null;
    }

    /**
     * Generate a query plan for query detail.
     * Explaining internal table is very quick, we prefer to use EXPLAIN COSTS.
     * But explaining external table is expensive, may need to access lots of metadata, so have to use EXPLAIN.
     *
     * <p> NOTE that `buildExplainString` will access table metadata, so need be called in the critical section of the lock.
     */
    private static void setExplainToQueryDetail(ExecPlan plan, StatementBase stmt, ConnectContext session,
                                                ResourceGroupClassifier.QueryType queryType) {
        if (plan == null || session.getQueryDetail() == null) {
            return;
        }

        StatementBase.ExplainLevel level = AnalyzerUtils.hasExternalTables(stmt) ?
                StatementBase.ExplainLevel.defaultValue() :
                StatementBase.ExplainLevel.parse(Config.query_detail_explain_level);
        session.getQueryDetail().setExplain(buildExplainString(plan, stmt, session, queryType, level));
    }

    /**
     * Analyze the statement.
     * 1. Optimization for INSERT-SELECT: if the SELECT doesn't need the lock, we can defer the lock acquisition
     * after analyzing the SELECT. That can help the case which SELECT is a time-consuming external table access.
     */
    @VisibleForTesting
    protected static boolean analyzeStatement(StatementBase statement, ConnectContext session,
                                              PlannerMetaLocker locker) {
        return analyzeStatement(statement, session, locker, new CommonSubqueryCTEHoister.HoistRecord[1]);
    }

    /**
     * @param hoisted single-element out parameter receiving the common-subquery rewrite, so that
     *                {@link #plan} can undo it once planning is finished
     */
    protected static boolean analyzeStatement(StatementBase statement, ConnectContext session,
                                              PlannerMetaLocker locker,
                                              CommonSubqueryCTEHoister.HoistRecord[] hoisted) {
        boolean deferredLock = false;
        Runnable takeLock = () -> {
            try (Timer lockerTime = Tracers.watchScope("Lock")) {
                lock(locker);
            }
        };
        try (Timer ignored = Tracers.watchScope("Analyzer")) {
            InsertStmt insertStmt = null;
            if (statement instanceof SubmitTaskStmt submitTaskStmt && submitTaskStmt.getInsertStmt() != null) {
                insertStmt = ((SubmitTaskStmt) statement).getInsertStmt();
            } else if (statement instanceof InsertStmt) {
                insertStmt = (InsertStmt) statement;
            }
            if (insertStmt != null) {
                Map<Long, Database> dbs = Maps.newHashMap();
                Map<Long, Set<Long>> tables = Maps.newHashMap();
                PlannerMetaLocker.collectTablesNeedLock(insertStmt.getQueryStatement(), session, dbs, tables);

                // If SELECT contains files() table function, resolve files() without lock but analyze normal tables
                // under lock. So:
                // - files() present AND normal tables present: pre-analyze files() without lock, then lock and analyze
                //   the whole statement under lock (no overall deferred lock).
                // - files() present AND no normal tables to lock: safe to use overall deferred lock path.
                boolean hasFileTableFunction =
                        !AnalyzerUtils.collectFileTableFunctionRelation(insertStmt.getQueryStatement()).isEmpty();
                boolean hasNormalTablesToLock = !tables.isEmpty();

                if (hasFileTableFunction && hasNormalTablesToLock) {
                    // Pre-analyze files() without lock to fetch schema via BE RPC
                    new QueryAnalyzer(session).analyzeFilesOnly(insertStmt.getQueryStatement());
                    // We will take lock and run full analyze below (no deferred lock)
                    deferredLock = false;
                } else if (hasFileTableFunction || tables.isEmpty() || FeConstants.runningUnitTest) {
                    // Only files() or no tables at all: allow deferred lock
                    deferredLock = true;
                }

                // This external-table-only pre-pass is orthogonal to deferredLock.
                // Even when the statement still needs the normal locked analyzer path,
                // we can pre-resolve or pre-refresh external source tables here so slow
                // connector/filesystem metadata I/O does not remain on the lock critical path.
                if (Config.enable_experimental_external_table_preparse ||
                        session.getSessionVariable().isEnableInsertSelectExternalAutoRefresh()) {
                    new QueryAnalyzer(session).analyzeExternalTablesOnly(statement,
                            session.getSessionVariable().isEnableInsertSelectExternalAutoRefresh());
                }
            }

            if (deferredLock) {
                if (statement instanceof SubmitTaskStmt) {
                    InsertAnalyzer.analyzeWithDeferredLock(insertStmt, session, takeLock);
                    Analyzer.AnalyzerVisitor.analyzeSubmitTaskOnly(insertStmt, (SubmitTaskStmt) statement, session);
                } else {
                    InsertAnalyzer.analyzeWithDeferredLock((InsertStmt) statement, session, takeLock);
                }
                ExplicitTxnStatementValidator.validate(statement, session);
                return true;
            } else {
                // Only pre-resolve external tables when there are internal tables to lock.
                // This avoids holding meta lock while fetching external metadata.
                // Check config first (cheapest), then locker state.
                if (insertStmt == null && Config.enable_experimental_external_table_preparse
                        && locker != null && !locker.isEmpty()) {
                    new QueryAnalyzer(session).analyzeExternalTablesOnly(statement);
                }
                takeLock.run();
                hoisted[0] = analyzeWithCommonSubqueryCte(statement, session);
                ExplicitTxnStatementValidator.validate(statement, session);
                return false;
            }
        }
    }

    /**
     * Analyze a query, optionally hoisting textually identical derived tables into a shared CTE first.
     *
     * <p>The hoist is deliberately placed here rather than earlier: the statement digest
     * ({@code ConnectProcessor#computeStatementDigest}) and any SPM baseline hash are already computed by
     * now, and the paths that persist AST-derived SQL - CREATE/ALTER VIEW, materialized view definitions,
     * CTAS, SUBMIT TASK - do not reach {@code StatementPlanner}, so none of them can pick up a synthetic CTE.
     *
     * <p>Reverting on an analysis error is a safety net, not a load-bearing guard: StarRocks already
     * rejects correlated derived tables in FROM on its own, and the hoist targets the outermost query
     * block, where an unexpected outer reference could only fail to resolve rather than silently rebind.
     * The fallback exists so that a body no one anticipated degrades into the original plan instead of a
     * failed query.
     */
    private static CommonSubqueryCTEHoister.HoistRecord analyzeWithCommonSubqueryCte(StatementBase statement,
                                                                                     ConnectContext session) {
        if (!(statement instanceof QueryStatement)
                || !session.getSessionVariable().isEnableCommonSubqueryCte()) {
            Analyzer.analyze(statement, session);
            return null;
        }

        CommonSubqueryCTEHoister.HoistRecord record;
        try {
            record = CommonSubqueryCTEHoister.hoist((QueryStatement) statement);
        } catch (Exception e) {
            LOG.warn("failed to hoist common subqueries, fall back to the original statement", e);
            Analyzer.analyze(statement, session);
            return null;
        }
        if (record.isEmpty()) {
            Analyzer.analyze(statement, session);
            return null;
        }

        try {
            Analyzer.analyze(statement, session);
        } catch (Exception e) {
            LOG.debug("common subquery hoisting produced an unanalyzable statement, reverting", e);
            record.revert();
            Analyzer.analyze(statement, session);
            return null;
        }

        // The pre-analysis guards read SQL text, which cannot see through a view - views are only expanded
        // during analysis. Re-check the hoisted bodies now that they are resolved, so that two derived tables
        // reading a view that calls rand(), or one carrying a LIMIT, are not silently made to agree.
        //
        // Then stand down if any materialized view could apply. Both checks need resolved tables, which is
        // why they run here and not before analysis.
        if (CommonSubqueryCTEHoister.isUnsafeAfterAnalysis(record) || hasRelatedMaterializedView(statement, session)) {
            record.revert();
            Analyzer.analyze(statement, session);
            return null;
        }
        return record;
    }

    /**
     * Whether any materialized view could rewrite this query, in which case common-subquery hoisting must
     * get out of the way.
     *
     * <p><b>Why the two cannot coexist.</b> Materialized-view rewrite recognizes a query by its AST.
     * Text-based matching turns the query's AST into a {@code CachingMvPlanContextBuilder.AstKey} - the SQL
     * rendered by {@code AST2SQLVisitor} - and looks that string up in the map of MV definitions
     * ({@code AST_TO_MV_MAP}, filled by {@code getAstKeysOfMV} from {@code MaterializedView#getDefineQueryParseNode}).
     * Hoisting rewrites the query's AST but not the MV's: {@code CREATE MATERIALIZED VIEW} is analyzed by
     * {@code MaterializedViewAnalyzer} and never passes through this planner, so its definition keeps the
     * original shape. The two strings stop being equal and the MV is simply never found - the query silently
     * loses its rewrite, with no error and nothing in the plan to hint at why.
     *
     * <p>An MV usually saves far more than computing one subquery twice, so it wins. Normalizing the MV side
     * instead would mean hoisting every MV definition before its cache key is computed, which couples this
     * pass to a persisted cache key - a much larger commitment than the optimization is worth.
     *
     * <p>The test is deliberately coarse: any related MV at all, whether or not it would actually have
     * matched. Erring towards "leave the query alone" only ever costs this optimization, never a rewrite.
     */
    private static boolean hasRelatedMaterializedView(StatementBase statement, ConnectContext session) {
        SessionVariable sessionVariable = session.getSessionVariable();
        if (sessionVariable.isDisableMaterializedViewRewrite() || !sessionVariable.isEnableMaterializedViewRewrite()) {
            return false;
        }
        try {
            Set<Table> tables = Sets.newHashSet(AnalyzerUtils.collectAllTable(statement).values());
            return !tables.isEmpty() && !MvUtils.getRelatedMvs(
                    session, sessionVariable.getNestedMvRewriteMaxLevel(), tables).isEmpty();
        } catch (Exception e) {
            // Never let this probe decide a query's fate; on doubt, keep the rewrite off.
            LOG.debug("failed to look up related materialized views, keeping the original statement", e);
            return true;
        }
    }

    public static ExecPlan planInsertStmt(PlannerMetaLocker plannerMetaLocker,
                                          InsertStmt insertStmt,
                                          ConnectContext connectContext) {
        // if use optimistic lock, we will unlock it in InsertPlanner#buildExecPlanWithRetrye
        boolean useOptimisticLock = isLockFreeInsertStmt(insertStmt, connectContext);
        return new InsertPlanner(plannerMetaLocker, useOptimisticLock).plan(insertStmt, connectContext);
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

    private static ExecPlan createQueryPlan(StatementBase stmt,
                                            ConnectContext session,
                                            TResultSinkType resultSinkType) {
        QueryStatement queryStmt = (QueryStatement) stmt;
        QueryRelation query = queryStmt.getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan;
        MVTransformerContext mvTransformerContext = MVTransformerContext.of(session, true);

        try (Timer ignored = Tracers.watchScope("Transformer")) {
            // get a logicalPlan without inlining views
            TransformerContext transformerContext = new TransformerContext(columnRefFactory, session, mvTransformerContext);
            logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(query);
        }

        boolean isShortCircuit = ShortCircuitPlanner.checkSupportShortCircuitRead(logicalPlan.getRoot(), session);
        OptExpression optimizedPlan;
        try (Timer ignored = Tracers.watchScope("Optimizer")) {
            // 2. Optimize logical plan and build physical plan
            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            optimizerContext.setMvTransformerContext(mvTransformerContext);
            optimizerContext.setStatement(stmt);
            if (isShortCircuit) {
                optimizerContext.setOptimizerOptions(OptimizerOptions.newShortCircuitOpt());
            }

            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            optimizedPlan = optimizer.optimize(logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
        }

        try (Timer ignored = Tracers.watchScope("ExecPlanBuild")) {
            // 3. Build fragment exec plan
            /*
             * SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
             * currently only used in Spark/Flink Connector
             * Because the connector sends only simple queries, it only needs to remove the output fragment
             */
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames,
                    resultSinkType,
                    !session.getSessionVariable().isSingleNodeExecPlan(), isShortCircuit);
            execPlan.setLogicalPlan(logicalPlan);
            execPlan.setColumnRefFactory(columnRefFactory);
            return execPlan;
        }
    }

    public static ExecPlan createQueryPlanWithReTry(QueryStatement queryStmt,
                                                    ConnectContext session,
                                                    TResultSinkType resultSinkType,
                                                    PlannerMetaLocker plannerMetaLocker,
                                                    long planStartTime) {
        QueryRelation query = queryStmt.getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();

        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        boolean isSchemaValid = true;

        int sourceTablesCount = collectSourceTablesCount(session, queryStmt);

        // TODO: double check relatedMvs for OlapTable
        // only collect once to save the original olapTable info
        // the original olapTable in queryStmt had been replaced with the copied olapTable
        Set<OlapTable> olapTables = collectOriginalOlapTables(session, queryStmt);
        for (int i = 0; i < Config.max_query_retry_time; ++i) {
            if (!isSchemaValid) {
                planStartTime = OptimisticVersion.generate();
                reAnalyzeStmt(queryStmt, session, plannerMetaLocker);
                colNames = queryStmt.getQueryRelation().getColumnOutputNames();
                isSchemaValid = true;
            }

            try {
                LogicalPlan logicalPlan;
                MVTransformerContext mvTransformerContext = MVTransformerContext.of(session, true);
                try (Timer ignored = Tracers.watchScope("Transformer")) {
                    // get a logicalPlan without inlining views
                    TransformerContext transformerContext =
                            new TransformerContext(columnRefFactory, session, mvTransformerContext);
                    logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(query);
                }

                boolean isShortCircuit =
                        ShortCircuitPlanner.checkSupportShortCircuitRead(logicalPlan.getRoot(), session);
                OptExpression optimizedPlan;
                try (Timer ignored = Tracers.watchScope("Optimizer")) {
                    OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
                    // 2. Optimize logical plan and build physical plan
                    // FIXME: refactor this into Optimizer.optimize() method.
                    // set query tables into OptimizeContext so can be added for mv rewrite
                    if (Config.skip_whole_phase_lock_mv_limit >= 0) {
                        optimizerContext.setQueryTables(olapTables);
                    }

                    if (isShortCircuit) {
                        optimizerContext.setOptimizerOptions(OptimizerOptions.newShortCircuitOpt());
                    }
                    optimizerContext.setMvTransformerContext(mvTransformerContext);
                    optimizerContext.setStatement(queryStmt);
                    optimizerContext.setSourceTablesCount(sourceTablesCount);

                    Optimizer optimizer = OptimizerFactory.create(optimizerContext);
                    optimizedPlan = optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                            new ColumnRefSet(logicalPlan.getOutputColumn()));
                }

                try (Timer ignored = Tracers.watchScope("ExecPlanBuild")) {
                    // 3. Build fragment exec plan
                    // SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
                    // currently only used in Spark/Flink Connector
                    // Because the connector sends only simple queries, it only needs to remove the output fragment
                    ExecPlan plan = PlanFragmentBuilder.createPhysicalPlan(
                            optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames,
                            resultSinkType,
                            !session.getSessionVariable().isSingleNodeExecPlan(), isShortCircuit);
                    isSchemaValid = checkOlapTableSchemaValid(olapTables, planStartTime);
                    if (isSchemaValid) {
                        plan.setLogicalPlan(logicalPlan);
                        plan.setColumnRefFactory(columnRefFactory);
                        return plan;
                    }
                }
            } catch (RuntimeException exception) {
                isSchemaValid = checkOlapTableSchemaValid(olapTables, planStartTime);
                if (isSchemaValid) {
                    throw exception;
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

    private static boolean checkOlapTableSchemaValid(Set<OlapTable> olapTables, long planStartTime) {
        return olapTables.stream().allMatch(t -> OptimisticVersion.validateTableUpdate(t, planStartTime));
    }

    public static Set<OlapTable> collectOriginalOlapTables(ConnectContext session, StatementBase queryStmt) {
        Set<OlapTable> olapTables = Sets.newHashSet();
        PlannerMetaLocker locker = new PlannerMetaLocker(session, queryStmt);
        try {
            // Need lock to avoid olap table metas ConcurrentModificationException
            lock(locker);
            AnalyzerUtils.copyOlapTable(queryStmt, olapTables);
            return olapTables;
        } finally {
            unLock(locker);
        }
    }

    public static int collectSourceTablesCount(ConnectContext session, StatementBase queryStmt) {
        List<Table> sourceTables = Lists.newArrayList();
        AnalyzerUtils.collectSourceTables(queryStmt, sourceTables);
        return sourceTables.size();
    }

    public static Set<OlapTable> reAnalyzeStmt(StatementBase queryStmt, ConnectContext session, PlannerMetaLocker locker) {
        try {
            lock(locker);
            // analyze to obtain the latest table from metadata
            Analyzer.analyze(queryStmt, session);
            // only copy the latest olap table
            Set<OlapTable> copiedTables = Sets.newHashSet();
            AnalyzerUtils.copyOlapTable(queryStmt, copiedTables);
            return copiedTables;
        } finally {
            unLock(locker);
        }
    }

    // Lock all database before analyze
    public static void lock(PlannerMetaLocker locker) {
        locker.lock();
    }

    // unLock all database after analyze
    public static void unLock(PlannerMetaLocker locker) {
        locker.unlock();
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
        if (session.getTxnId() != 0) {
            stmt.setTxnId(session.getTxnId());
            return;
        }

        // not need begin transaction here
        // 1. explain (exclude explain analyze)
        // 2. insert into files
        // 3. old delete
        // 4. insert overwrite (first plan, before handleInsertOverwrite)
        // 5. txnId already set (e.g., by InsertOverwriteJobRunner for dynamic overwrite re-plan)
        if (stmt.isExplain() && !StatementBase.ExplainLevel.ANALYZE.equals(stmt.getExplainLevel())) {
            return;
        }
        if (stmt instanceof InsertStmt) {
            if (((InsertStmt) stmt).useTableFunctionAsTargetTable() ||
                    ((InsertStmt) stmt).useBlackHoleTableAsTargetTable()) {
                return;
            }
        }
        // If txnId is already set, skip creating a new transaction.
        // This happens when InsertOverwriteJobRunner.executeInsert() re-plans the insertStmt
        // with txnId already set from prepare() phase.
        if (stmt.getTxnId() != DmlStmt.INVALID_TXN_ID) {
            return;
        }

        TableRef tableRef = stmt.getTableRef();
        if (tableRef == null) {
            throw new SemanticException("Table reference is null");
        }
        tableRef = AnalyzerUtils.normalizedTableRef(tableRef, session);
        if (stmt instanceof InsertStmt) {
            ((InsertStmt) stmt).setTableRef(tableRef);
        } else if (stmt instanceof DeleteStmt) {
            ((DeleteStmt) stmt).setTableRef(tableRef);
        } else if (stmt instanceof UpdateStmt) {
            ((UpdateStmt) stmt).setTableRef(tableRef);
        } else if (stmt instanceof MergeIntoStmt) {
            ((MergeIntoStmt) stmt).setTableRef(tableRef);
        }
        String catalogName = tableRef.getCatalogName();
        String dbName = tableRef.getDbName();
        String tableName = tableRef.getTableName();

        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogName, dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        TableName tableNameObj = TableName.fromTableRef(tableRef);
        Table targetTable = MetaUtils.getSessionAwareTable(session, db, tableNameObj);
        if (targetTable == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }
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
        } else if (stmt instanceof MergeIntoStmt) {
            label = MetaUtils.genMergeLabel(session.getExecutionId());
        } else {
            throw UnsupportedException.unsupportedException(
                    "Unsupported dml statement " + stmt.getClass().getSimpleName());
        }

        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        TransactionState.LoadJobSourceType sourceType = (stmt instanceof InsertStmt && ((InsertStmt) stmt).isShadowRewrite())
                ? TransactionState.LoadJobSourceType.SHADOW_REWRITE
                : TransactionState.LoadJobSourceType.INSERT_STREAMING;
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
            txnId = RemoteTransactionMgr.beginTransaction(
                    tbl.getSourceTableDbId(),
                    Lists.newArrayList(tbl.getSourceTableId()),
                    label,
                    sourceType,
                    session.getExecTimeout(),
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
                    session.getExecTimeout(),
                    session.getCurrentComputeResource());
        }

        stmt.setTxnId(txnId);
    }

    private static void abortTransaction(DmlStmt stmt, ConnectContext session, String errMsg) {
        long txnId = stmt.getTxnId();
        if (txnId == DmlStmt.INVALID_TXN_ID) {
            return;
        }

        TableRef tableRef = stmt.getTableRef();
        if (tableRef == null) {
            LOG.warn("Cannot abort transaction {}: table reference is null", txnId);
            return;
        }
        String catalogName = tableRef.getCatalogName();
        String dbName = tableRef.getDbName();
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogName, dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        TableName tableNameObj = TableName.fromTableRef(tableRef);
        Table targetTable = MetaUtils.getSessionAwareTable(session, db, tableNameObj);
        try {
            if (targetTable instanceof ExternalOlapTable) {
                ExternalOlapTable tbl = (ExternalOlapTable) targetTable;
                RemoteTransactionMgr.abortRemoteTransaction(tbl.getSourceTableDbId(), txnId, tbl.getSourceTableHost(),
                        tbl.getSourceTablePort(), errMsg, Collections.emptyList(), Collections.emptyList());
            } else if (targetTable instanceof OlapTable) {
                GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
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
