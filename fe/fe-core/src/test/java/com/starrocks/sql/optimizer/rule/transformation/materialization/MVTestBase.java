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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MaterializedViewOptimizer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.rule.NonDeterministicVisitor;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for materialized view tests.
 */
public class MVTestBase extends StarRocksTestBase {
    protected static final Logger LOG = LogManager.getLogger(MVTestBase.class);
    protected static ConnectContext connectContext;
    protected static PseudoCluster cluster;

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    // default database name
    protected static final String DB_NAME = "test";

    @BeforeClass
    public static void beforeClass() throws Exception {
        CachingMvPlanContextBuilder.getInstance().rebuildCache();
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(100);
        cluster = PseudoCluster.getInstance();

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        connectContext.setDatabase(DB_NAME);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            PseudoCluster.getInstance().shutdown(true);
        } catch (Exception e) {
            // ignore exception
        }
    }

    public String getFragmentPlan(String sql) throws Exception {
        String s = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
        return s;
    }

    public String getFragmentPlan(String sql, String traceModule) throws Exception {
        return getFragmentPlan(sql, TExplainLevel.NORMAL, traceModule);
    }

    public String getFragmentPlan(String sql, TExplainLevel level) throws Exception {
        return getFragmentPlan(sql, level, null);
    }

    public String getFragmentPlan(String sql, TExplainLevel level, String traceModule) throws Exception {
        Pair<String, Pair<ExecPlan, String>> result =
                UtFrameUtils.getFragmentPlanWithTrace(connectContext, sql, traceModule);
        Pair<ExecPlan, String> execPlanWithQuery = result.second;
        String traceLog = execPlanWithQuery.second;
        if (!Strings.isNullOrEmpty(traceLog)) {
            System.out.println(traceLog);
        }
        return execPlanWithQuery.first.getExplainString(level);
    }

    public static Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), mvName);
        Assert.assertNotNull(table);
        return table;
    }

    protected MaterializedView getMv(String mvName) {
        return getMv(DB_NAME, mvName);
    }

    protected MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    protected void refreshMaterializedViewWithPartition(String dbName, String mvName, String partitionStart,
                                                        String partitionEnd) throws SQLException {
        cluster.runSql(dbName, String.format("refresh materialized view %s partition start (\"%s\") " +
                "end (\"%s\") with sync mode", mvName, partitionStart, partitionEnd));
        cluster.runSql(dbName, String.format("analyze table %s with sync mode", mvName));
    }

    protected void refreshMaterializedView(String dbName, String mvName) throws SQLException {
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
        cluster.runSql(dbName, String.format("analyze table %s with sync mode", mvName));
    }

    protected static void withRefreshedMV(String sql, StarRocksAssert.ExceptionRunnable action) {
        TableName mvTableName = null;
        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assert.assertTrue(stmt instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
            mvTableName = createMaterializedViewStatement.getTableName();
            Assert.assertTrue(mvTableName != null);

            createAndRefreshMv(sql);
            action.run();
        } catch (Exception e) {
            Assert.fail();
        } finally {
            String dbName = mvTableName.getDb() == null ? DB_NAME : mvTableName.getDb();
            try {
                dropMv(dbName, mvTableName.getTbl());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected static void createAndRefreshMv(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assert.assertTrue(stmt instanceof CreateMaterializedViewStatement);
        CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
        TableName mvTableName = createMaterializedViewStatement.getTableName();
        Assert.assertTrue(mvTableName != null);
        String dbName = Strings.isNullOrEmpty(mvTableName.getDb()) ? DB_NAME : mvTableName.getDb();
        String mvName = mvTableName.getTbl();
        starRocksAssert.withMaterializedView(sql);
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
    }

    public static void dropMv(String dbName, String mvName) throws Exception {
        starRocksAssert.dropMaterializedView(mvName);
    }

    public static OptExpression getOptimizedPlan(String sql) {
        return getOptimizedPlan(sql, connectContext);
    }

    public static OptExpression getLogicalOptimizedPlan(String sql) {
        return getOptimizedPlan(sql, connectContext, OptimizerOptions.newRuleBaseOpt());
    }

    public static OptExpression getOptimizedPlan(String sql, ConnectContext connectContext) {
        return getOptimizedPlan(sql, connectContext, OptimizerOptions.defaultOpt());
    }

    public static OptExpression getOptimizedPlan(String sql, ConnectContext connectContext,
                                                 OptimizerOptions optimizerOptions) {
        StatementBase mvStmt;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            mvStmt = statementBases.get(0);
        } catch (Exception e) {
            return null;
        }
        Preconditions.checkState(mvStmt instanceof QueryStatement);
        Analyzer.analyze(mvStmt, connectContext);
        QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(query);
        OptimizerContext optimizerContext =
                OptimizerFactory.initContext(connectContext, columnRefFactory, optimizerOptions);
        Optimizer optimizer = OptimizerFactory.create(optimizerContext);
        return optimizer.optimize(
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));
    }

    public List<PhysicalScanOperator> getScanOperators(OptExpression root, String name) {
        List<PhysicalScanOperator> results = Lists.newArrayList();
        getScanOperators(root, name, results);
        return results;
    }

    private void getScanOperators(OptExpression root, String name, List<PhysicalScanOperator> results) {
        if (root.getOp() instanceof PhysicalScanOperator
                && ((PhysicalScanOperator) root.getOp()).getTable().getName().equals(name)) {
            results.add((PhysicalScanOperator) root.getOp());
        }
        for (OptExpression child : root.getInputs()) {
            getScanOperators(child, name, results);
        }
    }

    public static MvUpdateInfo getMvUpdateInfo(MaterializedView mv) {
        return MvRefreshArbiter.getMVTimelinessUpdateInfo(mv, true);
    }

    public static Set<String> getPartitionNamesToRefreshForMv(MaterializedView mv) {
        MvUpdateInfo mvUpdateInfo = MvRefreshArbiter.getMVTimelinessUpdateInfo(mv, true);
        Preconditions.checkState(mvUpdateInfo != null);
        return mvUpdateInfo.getMvToRefreshPartitionNames();
    }

    public static void executeInsertSql(ConnectContext connectContext, String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor.newInternalExecutor(connectContext, statement).execute();
    }

    public static void sql(String sql) throws Exception {
        cluster.runSql(DB_NAME, sql);
    }

    /**
     * Add list partition with one value
     * @param tbl table name
     * @param pName partition name
     * @param pVal partition value
     */
    protected void addListPartition(String tbl, String pName, String pVal) {
        String addPartitionSql = String.format("ALTER TABLE %s ADD PARTITION %s VALUES IN ('%s')", tbl, pName, pVal);
        StatementBase stmt = SqlParser.parseSingleStatement(addPartitionSql, connectContext.getSessionVariable().getSqlMode());
        try {
            StmtExecutor.newInternalExecutor(connectContext, stmt).execute();
        } catch (Exception e) {
            Assert.fail("add partition failed:" + e);
        }
    }

    public static String getAggFunction(String funcName, String aggArg) {
        if (funcName.equals(FunctionSet.ARRAY_AGG)) {
            funcName = String.format("array_agg(distinct %s)", aggArg);
        } else if (funcName.equals(FunctionSet.BITMAP_UNION)) {
            funcName = String.format("bitmap_union(to_bitmap(%s))", aggArg);
        } else if (funcName.equals(FunctionSet.PERCENTILE_UNION)) {
            funcName = String.format("percentile_union(percentile_hash(%s))", aggArg);
        } else if (funcName.equals(FunctionSet.HLL_UNION)) {
            funcName = String.format("hll_union(hll_hash(%s))", aggArg);
        } else {
            funcName = String.format("%s(%s)", funcName, aggArg);
        }
        return funcName;
    }

    public static void setGlobalVariableVariable(String key, String value) throws DdlException {
        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(connectContext.getSessionVariable(),
                new SystemVariable(key, new StringLiteral(value)), true);
    }

    public static void executeInsertSql(String sql) throws Exception {
        executeInsertSql(connectContext, sql);
    }

    protected TaskRun buildMVTaskRun(MaterializedView mv, String dbName) {
        Task task = TaskBuilder.buildMvTask(mv, dbName);
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        return taskRun;
    }

    protected void refreshMVRange(String mvName, boolean force) throws Exception {
        refreshMVRange(mvName, null, null, force);
    }

    protected PartitionBasedMvRefreshProcessor refreshMV(String dbName, MaterializedView mv) throws Exception {
        Task task = TaskBuilder.buildMvTask(mv, dbName);
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        return (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
    }

    protected void refreshMVRange(String mvName, String start, String end, boolean force) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("refresh materialized view " + mvName);
        if (start != null && end != null) {
            sb.append(String.format(" partition start('%s') end('%s')", start, end));
        }
        if (force) {
            sb.append(" force");
        }
        sb.append(" with sync mode");
        String sql = sb.toString();
        starRocksAssert.getCtx().executeSql(sql);
    }

    protected QueryMaterializationContext.QueryCacheStats getQueryCacheStats(RuntimeProfile profile) {
        Map<String, String> infoStrings = profile.getInfoStrings();
        Assert.assertTrue(infoStrings.containsKey("MVQueryCacheStats"));
        String cacheStats = infoStrings.get("MVQueryCacheStats");
        return GsonUtils.GSON.fromJson(cacheStats,
                QueryMaterializationContext.QueryCacheStats.class);
    }

    protected Map<Table, Set<String>> getRefTableRefreshedPartitions(PartitionBasedMvRefreshProcessor processor) {
        Map<TableSnapshotInfo, Set<String>> baseTables = processor
                .getRefTableRefreshPartitions(Sets.newHashSet("p20220101"));
        Assert.assertEquals(2, baseTables.size());
        return baseTables.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getBaseTable(), x -> x.getValue()));
    }

    protected void assertPlanContains(ExecPlan execPlan, String... explain) throws Exception {
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    protected static ExecPlan getMVRefreshExecPlan(TaskRun taskRun) throws Exception {
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();
        MvTaskRunContext mvTaskRunContext = processor.getMvContext();
        return mvTaskRunContext.getExecPlan();
    }

    protected static void initAndExecuteTaskRun(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
    }

    protected String toPartitionVal(String val) {
        return val == null ? "NULL" : String.format("'%s'", val);
    }

    protected void addRangePartition(String tbl, String pName, String pVal1, String pVal2) {
        addRangePartition(tbl, pName, pVal1, pVal2, false);
    }

    protected void addRangePartition(String tbl, String pName, String pVal1, String pVal2, boolean isInsertValue) {
        // mock the check to ensure test can run
        new MockUp<ExpressionRangePartitionInfo>() {
            @Mock
            public boolean isAutomaticPartition() {
                return false;
            }
        };
        new MockUp<ExpressionRangePartitionInfoV2>() {
            @Mock
            public boolean isAutomaticPartition() {
                return false;
            }
        };
        try {
            String addPartitionSql = String.format("ALTER TABLE %s ADD " +
                    "PARTITION %s VALUES [(%s),(%s))", tbl, pName, toPartitionVal(pVal1), toPartitionVal(pVal2));
            System.out.println(addPartitionSql);
            starRocksAssert.alterTable(addPartitionSql);

            // insert values
            if (isInsertValue) {
                String insertSql = String.format("insert into %s partition(%s) values('%s', 1, 1);",
                        tbl, pName, pVal1);
                executeInsertSql(insertSql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Failed to add partition", e);
        }
    }

    protected void addListPartition(String tbl, String pName, String pVal1, String pVal2) {
        addListPartition(tbl, pName, pVal1, pVal2, false);
    }

    protected void addListPartition(String tbl, String pName, String pVal1, String pVal2, boolean isInsertValues) {
        String addPartitionSql = String.format("ALTER TABLE %s ADD PARTITION IF NOT EXISTS %s VALUES IN ((%s, %s))",
                tbl, pName, toPartitionVal(pVal1), toPartitionVal(pVal2));
        StatementBase stmt = SqlParser.parseSingleStatement(addPartitionSql, connectContext.getSessionVariable().getSqlMode());
        try {
            // add a new partition
            new StmtExecutor(connectContext, stmt).execute();

            // insert values
            if (isInsertValues) {
                String insertSql = String.format("insert into %s partition(%s) values(1, 1, '%s', '%s');",
                        tbl, pName, pVal1, pVal2);
                executeInsertSql(insertSql);
            }
        } catch (Exception e) {
            Assert.fail("add partition failed:" + e);
        }
    }

    protected MvPlanContext getOptimizedPlan(MaterializedView mv, boolean isInlineView, boolean isCheckNonDeterministicFunction) {
        return new MaterializedViewOptimizer().optimize(mv, connectContext, isInlineView, isCheckNonDeterministicFunction);

    }

    protected MvPlanContext getOptimizedPlan(MaterializedView mv, boolean isInlineView) {
        return new MaterializedViewOptimizer().optimize(mv, connectContext, isInlineView, true);
    }

    protected boolean hasNonDeterministicFunction(OptExpression root) {
        return root.getOp().accept(new NonDeterministicVisitor(), root, null);
    }
}