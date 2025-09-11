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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunProcessor;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVPCTBasedRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Base class for materialized view tests.
 */
public abstract class MVTestBase extends StarRocksTestBase {

    public interface ExceptionRunnable {
        void run() throws Exception;
    }

    public interface ExecPlanChecker {
        void check(ExecPlan execPlan) throws Exception;
    }

    public interface MVActionRunner {
        void run(MaterializedView mv) throws Exception;
    }

    protected static final Logger LOG = LogManager.getLogger(MVTestBase.class);
    protected static ConnectContext connectContext;
    protected static PseudoCluster cluster;

    @TempDir
    public static File temp;
    // default database name
    protected static final String DB_NAME = "test";

    @BeforeAll
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

    @AfterAll
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
        Assertions.assertNotNull(table);
        return table;
    }

    protected MaterializedView getMv(String mvName) {
        return getMv(DB_NAME, mvName);
    }

    protected MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assertions.assertTrue(table instanceof MaterializedView);
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
            Assertions.assertTrue(stmt instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
            mvTableName = createMaterializedViewStatement.getTableName();
            Assertions.assertTrue(mvTableName != null);

            createAndRefreshMv(sql);
            action.run();
        } catch (Exception e) {
            Assertions.fail();
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
        Assertions.assertTrue(stmt instanceof CreateMaterializedViewStatement);
        CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
        TableName mvTableName = createMaterializedViewStatement.getTableName();
        Assertions.assertTrue(mvTableName != null);
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

    public static StatementBase getAnalyzedPlan(String sql, ConnectContext connectContext) {
        StatementBase statementBase;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            statementBase = statementBases.get(0);
        } catch (Exception e) {
            return null;
        }
        Analyzer.analyze(statementBase, connectContext);
        return statementBase;
    }

    public static OptExpression getOptimizedPlan(String sql, ConnectContext connectContext,
                                                 OptimizerOptions optimizerOptions) {
        StatementBase mvStmt = getAnalyzedPlan(sql, connectContext);
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
            Assertions.fail("add partition failed:" + e);
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

    protected TaskRun withMVRefreshTaskRun(String dbName, MaterializedView mv) throws Exception {
        Task task = TaskBuilder.buildMvTask(mv, dbName);
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        return taskRun;
    }

    protected MVTaskRunProcessor getMVTaskRunProcessor(String dbName, MaterializedView mv) throws Exception {
        TaskRun taskRun = withMVRefreshTaskRun(dbName, mv);
        return getMVTaskRunProcessor(taskRun);
    }

    protected MVPCTBasedRefreshProcessor refreshMV(String dbName, MaterializedView mv) throws Exception {
        TaskRun taskRun = withMVRefreshTaskRun(dbName, mv);
        return getPartitionBasedRefreshProcessor(taskRun);
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
        Assertions.assertTrue(infoStrings.containsKey("MVQueryCacheStats"));
        String cacheStats = infoStrings.get("MVQueryCacheStats");
        return GsonUtils.GSON.fromJson(cacheStats,
                QueryMaterializationContext.QueryCacheStats.class);
    }

    protected Map<Table, Set<String>> getRefTableRefreshedPartitions(MVPCTBasedRefreshProcessor processor) {
        Map<BaseTableSnapshotInfo, Set<String>> baseTables = processor
                .getPCTRefTableRefreshPartitions(Sets.newHashSet("p20220101"));
        Assertions.assertEquals(2, baseTables.size());
        return baseTables.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getBaseTable(), x -> x.getValue()));
    }

    protected void assertPlanContains(ExecPlan execPlan, String... explain) throws Exception {
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);

        for (String expected : explain) {
            Assertions.assertTrue(StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected),
                    "expected is: " + expected + " but plan is \n" + explainString);
        }
    }

    protected static ExecPlan getMVRefreshExecPlan(TaskRun taskRun) throws Exception {
        initAndExecuteTaskRun(taskRun);
        TaskRunProcessor processor = taskRun.getProcessor();
        Assertions.assertTrue(processor instanceof MVTaskRunProcessor);
        MVTaskRunProcessor mvTaskRunProcessor = (MVTaskRunProcessor) processor;
        MvTaskRunContext mvTaskRunContext = mvTaskRunProcessor.getMvTaskRunContext();
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
            Assertions.fail("add partition failed:" + e);
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

    /**
     * TestListener is a base class that can be used to test cases in multi-variable environments.
     */
    public abstract class TestListener {
        protected boolean val;

        public abstract void onBeforeCase(ConnectContext connectContext);

        public abstract void onAfterCase(ConnectContext connectContext);
    }

    public class EnableMVRewriteListener extends TestListener {
        @Override
        public void onBeforeCase(ConnectContext connectContext) {
            this.val = connectContext.getSessionVariable().isEnableMaterializedViewRewrite();
            connectContext.getSessionVariable().setEnableMaterializedViewRewrite(true);
        }

        @Override
        public void onAfterCase(ConnectContext connectContext) {
            connectContext.getSessionVariable().setEnableMaterializedViewRewrite(val);
        }
    }

    public class DisableMVRewriteListener extends TestListener {
        @Override
        public void onBeforeCase(ConnectContext connectContext) {
            this.val = connectContext.getSessionVariable().isEnableMaterializedViewRewrite();
            connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        }

        @Override
        public void onAfterCase(ConnectContext connectContext) {
            connectContext.getSessionVariable().setEnableMaterializedViewRewrite(val);
        }
    }

    public class EnableMVMultiStageRewriteListener extends TestListener {
        @Override
        public void onBeforeCase(ConnectContext connectContext) {
            this.val = connectContext.getSessionVariable().isEnableMaterializedViewMultiStagesRewrite();
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
        }

        @Override
        public void onAfterCase(ConnectContext connectContext) {
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(val);
        }
    }

    public class DisableMVMultiStageRewriteListener extends TestListener {
        @Override
        public void onBeforeCase(ConnectContext connectContext) {
            this.val = connectContext.getSessionVariable().isEnableMaterializedViewMultiStagesRewrite();
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(false);
        }

        @Override
        public void onAfterCase(ConnectContext connectContext) {
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(val);
        }
    }



    protected void doTest(List<TestListener> listeners, ExceptionRunnable testCase) {
        for (TestListener listener : listeners) {
            listener.onBeforeCase(connectContext);
            try {
                testCase.run();
            } catch (Exception e) {
                Assertions.fail(e.getMessage());
            } finally {
                listener.onAfterCase(connectContext);
            }
        }
    }

    protected void testMVRefreshWithOnePartitionAndOneUnPartitionTable(String t1,
                                                                       String s1,
                                                                       String mvQuery,
                                                                       String... expect) throws Exception {
        String t2 = "CREATE TABLE non_partition_table (dt2 date, int2 int);";
        String s2 = "INSERT INTO non_partition_table VALUES (\"2020-06-23\",1),(\"2020-07-23\",1),(\"2020-07-23\",1)" +
                ",(\"2020-08-23\",1),(null,null);\n";
        if (!Strings.isNullOrEmpty(t1)) {
            starRocksAssert.withTable(t1);
        }
        starRocksAssert.withTable(t2);
        executeInsertSql(s1);
        executeInsertSql(s2);
        starRocksAssert.withMaterializedView(mvQuery, (obj) -> {
            String mvName = (String) obj;
            MaterializedView mv = starRocksAssert.getMv("test", mvName);
            ExecuteOption executeOption = new ExecuteOption(70, false, new HashMap<>());
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).setExecuteOption(executeOption).build();
            initAndExecuteTaskRun(taskRun);
            MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
            MvTaskRunContext mvTaskRunContext = processor.getMvContext();
            ExecPlan execPlan = mvTaskRunContext.getExecPlan();
            Assertions.assertTrue(execPlan != null);
            for (String expectStr : expect) {
                assertPlanContains(execPlan, expectStr);
            }
        });
    }

    public static MVTaskRunProcessor getMVTaskRunProcessor(TaskRun taskRun) {
        Assertions.assertTrue(taskRun.getProcessor() instanceof MVTaskRunProcessor);
        return (MVTaskRunProcessor) taskRun.getProcessor();
    }

    public static MVPCTBasedRefreshProcessor getPartitionBasedRefreshProcessor(TaskRun taskRun) {
        Assertions.assertTrue(taskRun.getProcessor() instanceof MVTaskRunProcessor);
        MVTaskRunProcessor mvTaskRunProcessor = (MVTaskRunProcessor) taskRun.getProcessor();
        return (MVPCTBasedRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
    }

    protected void withMVQuery(String mvQuery,
                               MVActionRunner runner) throws Exception {
        String ddl = String.format("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"refresh_mode\" = \"incremental\"" +
                ")\n" +
                "AS %s;", mvQuery);
        starRocksAssert.withMaterializedView(ddl);
        MaterializedView mv = getMv("test_mv1");
        runner.run(mv);
    }

    /**
     * Get the explain plan for the MV refresh task.
     */
    protected String explainMVRefreshExecPlan(MaterializedView mv, String explainQuery) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Assertions.assertTrue(task != null, "Task for MV " + mv.getName() + " not found:" + explainQuery);
        StatementBase stmt = getAnalyzedPlan(explainQuery, connectContext);
        Assertions.assertTrue(stmt != null, "Expected a valid StatementBase but got null:" + explainQuery);
        ExecuteOption executeOption = new ExecuteOption(70, false, new HashMap<>());
        return taskManager.getMVRefreshExplain(task, executeOption, stmt);
    }

    /**
     * Get the execution plan for the MV refresh task.
     */
    protected ExecPlan getMVRefreshExecPlan(MaterializedView mv, String explainQuery) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Assertions.assertTrue(task != null, "Task for MV " + mv.getName() + " not found:" + explainQuery);
        StatementBase stmt = getAnalyzedPlan(explainQuery, connectContext);
        Assertions.assertTrue(stmt != null, "Expected a valid StatementBase but got null:" + explainQuery);
        ExecuteOption executeOption = new ExecuteOption(70, false, new HashMap<>());
        TaskRun taskRun = taskManager.buildTaskRun(task, executeOption);
        return taskManager.getMVRefreshExecPlan(taskRun, task, executeOption, stmt);
    }

    public static List<String> extractColumnValues(String sql, int columnIndex) {
        List<String> result = new ArrayList<>();

        // Regex pattern to match the VALUES clause and all parenthesized value groups
        Pattern pattern = Pattern.compile("(?i)values\\s*([^)]+)(?:\\s*,\\s*([^)]+))*\\s*;?");
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            // Process first set of values
            processValueGroup(matcher.group(1), columnIndex, result);

            // Process subsequent value groups
            for (int i = 2; i <= matcher.groupCount(); i++) {
                if (matcher.group(i) != null) {
                    processValueGroup(matcher.group(i), columnIndex, result);
                }
            }
        }

        return result;
    }

    private static void processValueGroup(String valueGroup, int columnIndex, List<String> result) {
        // Split by commas but ignore commas inside quotes
        String[] values = valueGroup.split(",(?=(?:[^']*'[^']*')*[^']*$)");

        if (columnIndex < values.length) {
            String value = values[columnIndex].trim();
            // Remove surrounding quotes if present
            value = cleanSqlValue(value);
            result.add(value);
        }
    }

    public static String cleanSqlValue(String input) {
        if (input == null) {
            return null;
        }
        String result = input.trim();
        int len = result.length();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (result.charAt(i) == '\'' || result.charAt(i) == '\"' || result.charAt(i) == '`' ||
                    result.charAt(i) == '(' || result.charAt(i) == ')') {
                continue;
            } else {
                sb.append(result.charAt(i));
            }
        }
        return sb.toString();
    }

    protected void addListPartition(String tbl, List<String> values) {
        Map<String, PCell> partitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (String val : values) {
            PListCell pListCell = new PListCell(val);
            String pName = AnalyzerUtils.getFormatPartitionValue(val);
            if (partitions.containsKey(pName)) {
                try {
                    pName = AnalyzerUtils.calculateUniquePartitionName(pName, pListCell, partitions);
                } catch (Exception e) {
                    Assertions.fail("add partition failed:" + e);
                }
            }
            partitions.put(pName, pListCell);
            addListPartition(tbl, pName, val);
        }
    }

    public View getView(String viewName) {
        Table table = getTable(DB_NAME, viewName);
        Assertions.assertTrue(table instanceof View);
        return (View) table;
    }
}