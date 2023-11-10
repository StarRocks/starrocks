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
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.sql.SQLException;
import java.util.List;

public class MvRewriteTestBase {
    protected static ConnectContext connectContext;
    protected static PseudoCluster cluster;
    protected static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.dynamic_partition_check_interval_seconds = 10000;
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
<<<<<<< HEAD
=======
        Config.alter_scheduler_interval_millisecond = 100;
        FeConstants.enablePruneEmptyOutputScan = false;
>>>>>>> 9bdcdf8608 ([UT] refactor mv ut (#34128))

        // build a small cache for test
        Config.mv_plan_cache_max_size = 10;
        CachingMvPlanContextBuilder.getInstance().rebuildCache();

        // Use sync analyze
        Config.mv_auto_analyze_async = false;

        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();

        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        connectContext.getSessionVariable().setEnableOptimizerTraceLog(true);

        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");

        Config.enable_experimental_mv = true;

        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
                    OlapTable tbl = ((OlapTable) testDb.getTable(tableName.getTbl()));
                    if (tbl != null) {
                        for (Partition partition : tbl.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    }
                }
            }
        };
    }

    private static void setPartitionVersion(Partition partition, long version) {
        partition.setVisibleVersion(version, System.currentTimeMillis());
        MaterializedIndex baseIndex = partition.getBaseIndex();
        List<Tablet> tablets = baseIndex.getTablets();
        for (Tablet tablet : tablets) {
            List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
            for (Replica replica : replicas) {
                replica.updateVersionInfo(version, -1, version);
            }
        }
    }

    public static void prepareDefaultDatas() throws Exception {
        starRocksAssert.withTable("create table emps (\n" +
                        "    empid int not null,\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null,\n" +
                        "    salary double\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table emps_par (\n" +
                        "    empid int not null,\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null,\n" +
                        "    salary double\n" +
                        ")\n" +
                        "PARTITION BY RANGE(`deptno`)\n" +
                        "(PARTITION p1 VALUES [(\"-2147483648\"), (\"2\")),\n" +
                        "PARTITION p2 VALUES [(\"2\"), (\"3\")),\n" +
                        "PARTITION p3 VALUES [(\"3\"), (\"4\")))\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table depts (\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`deptno`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table dependents (\n" +
                        "    empid int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table locations (\n" +
                        "    empid int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
        starRocksAssert.withTable("CREATE TABLE `test_all_type` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `table_with_partition` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`,`id_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`id_date`)\n" +
                "(PARTITION p1991 VALUES [('1991-01-01'), ('1992-01-01')),\n" +
                "PARTITION p1992 VALUES [('1992-01-01'), ('1993-01-01')),\n" +
                "PARTITION p1993 VALUES [('1993-01-01'), ('1994-01-01')))" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `table_with_day_partition` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`,`id_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`id_date`)\n" +
                "(PARTITION p19910330 VALUES [('1991-03-30'), ('1991-03-31')),\n" +
                "PARTITION p19910331 VALUES [('1991-03-31'), ('1991-04-01')),\n" +
                "PARTITION p19910401 VALUES [('1991-04-01'), ('1991-04-02')),\n" +
                "PARTITION p19910402 VALUES [('1991-04-02'), ('1991-04-03')))" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("create table test_base_part(c1 int, c2 bigint, c3 bigint, c4 bigint)" +
                " partition by range(c3) (" +
                " partition p1 values less than (\"100\")," +
                " partition p2 values less than (\"200\")," +
                " partition p3 values less than (\"1000\")," +
                " PARTITION p4 values less than (\"2000\")," +
                " PARTITION p5 values less than (\"3000\"))" +
                " distributed by hash(c1)" +
                " properties (\"replication_num\"=\"1\");");

        cluster.runSql("test", "insert into emps values(1, 1, \"emp_name1\", 100);");
        cluster.runSql("test", "insert into emps values(2, 1, \"emp_name1\", 120);");
        cluster.runSql("test", "insert into emps values(3, 1, \"emp_name1\", 150);");
        cluster.runSql("test", "insert into depts values(1, \"dept_name1\")");
        cluster.runSql("test", "insert into depts values(2, \"dept_name2\")");
        cluster.runSql("test", "insert into depts values(3, \"dept_name3\")");
        cluster.runSql("test", "insert into dependents values(1, \"dependent_name1\")");
        cluster.runSql("test", "insert into locations values(1, \"location1\")");
        cluster.runSql("test", "insert into t0 values(1, 2, 3)");
        cluster.runSql("test", "insert into test_all_type values(" +
                "\"value1\", 1, 2, 3, 4.0, 5.0, 6, \"2022-11-11 10:00:01\", \"2022-11-11\", 10.12)");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `k1` int(11) NULL COMMENT \"\",\n" +
                "  `v1` int(11) NULL COMMENT \"\",\n" +
                "  `v2` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES [(\"-2147483648\"), (\"2\")),\n" +
                "PARTITION p2 VALUES [(\"2\"), (\"3\")),\n" +
                "PARTITION p3 VALUES [(\"3\"), (\"4\")),\n" +
                "PARTITION p4 VALUES [(\"4\"), (\"5\")),\n" +
                "PARTITION p5 VALUES [(\"5\"), (\"6\")),\n" +
                "PARTITION p6 VALUES [(\"6\"), (\"7\")))\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 6\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        cluster.runSql("test", "insert into t1 values (1,1,1),(1,1,2),(1,1,3),(1,2,1),(1,2,2),(1,2,3)," +
                " (1,3,1),(1,3,2),(1,3,3)\n" +
                " ,(2,1,1),(2,1,2),(2,1,3),(2,2,1),(2,2,2),(2,2,3),(2,3,1),(2,3,2),(2,3,3)\n" +
                " ,(3,1,1),(3,1,2),(3,1,3),(3,2,1),(3,2,2),(3,2,3),(3,3,1),(3,3,2),(3,3,3)");

        starRocksAssert.withTable("CREATE TABLE `json_tbl` (\n" +
                "  `p_dt` date NULL COMMENT \"\",\n" +
                "  `d_user` json NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_dt`)\n" +
                "DISTRIBUTED BY HASH(p_dt) BUCKETS 2\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");
        cluster.runSql("test", "insert into json_tbl values('2020-01-01', '{\"a\": 1, \"gender\": \"man\"}') ");
        cluster.runSql("test", "insert into table_with_partition values" +
                "(\"varchar1\", '1991-02-01', 1, 1, 1), " +
                "(\"varchar2\",'1992-02-01', 2, 1, 1), " +
                "(\"varchar3\", '1993-02-01', 3, 1, 1)");
        cluster.runSql("test", "insert into table_with_day_partition values " +
                "(\"varchar1\", '1991-03-30', 1, 1, 1)," +
                "(\"varchar2\", '1991-03-31', 2, 1, 1), " +
                "(\"varchar3\", '1991-04-01', 3, 1, 1)," +
                "(\"varchar3\", '1991-04-02', 4, 1, 1)");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    public String getFragmentPlan(String sql) throws Exception {
        String s = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
        return s;
    }

    public static Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        return table;
    }

    protected MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    protected void refreshMaterializedView(String dbName, String mvName) throws SQLException {
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
        cluster.runSql(dbName, String.format("analyze table %s with sync mode", mvName));
    }

    protected void createAndRefreshMv(String dbName, String mvName, String sql) throws Exception {
        starRocksAssert.withMaterializedView(sql);
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
    }

    protected void createMv(String dbName, String mvName, String sql) throws Exception {
        starRocksAssert.withMaterializedView(sql);
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
    }

    protected void dropMv(String dbName, String mvName) throws Exception {
        starRocksAssert.dropMaterializedView(mvName);
    }

    public static OptExpression getOptimizedPlan(String sql, ConnectContext connectContext) {
        StatementBase mvStmt;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            mvStmt = statementBases.get(0);
        } catch (ParsingException parsingException) {
            return null;
        }
        Preconditions.checkState(mvStmt instanceof QueryStatement);
        Analyzer.analyze(mvStmt, connectContext);
        QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(query);
        Optimizer optimizer = new Optimizer();
        return optimizer.optimize(
                connectContext,
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);
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
}
