// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.analysis.DmlStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MaterializedViewRewriteOptimizationTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.enable_experimental_mv = true;
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);

        /*
        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_1" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv" +
                " where lo_orderpriority='5-LOW';");
        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_2" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000;");

        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_3" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000 and lo_orderpriority='5-LOW';");

        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_4" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE, LO_SUPPLYCOST + 1 as add_one from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000 and lo_orderpriority='5-LOW';");

        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_5" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';");

         */

        /*
        starRocksAssert.withNewMaterializedView("create materialized view join_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 = 1");

         */
        starRocksAssert.withNewMaterializedView("create materialized view join_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 = 1");
    }

    @Test
    public void testFilterScan() throws Exception {
        /*
        String query1 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv where lo_orderpriority='5-LOW';";
        String plan1 = getFragmentPlan(query1);
        assertContains(plan1, "1:Project\n" +
                "  |  <slot 1> : 40: LO_ORDERDATE\n" +
                "  |  <slot 2> : 41: LO_ORDERKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_1");

        String query2 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv where LO_REVENUE < 100000 ;";
        String plan2 = getFragmentPlan(query2);
        assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : 42: LO_ORDERDATE\n" +
                "  |  <slot 2> : 43: LO_ORDERKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_2");

        String query3 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv where LO_REVENUE < 50000 ;";
        String plan3 = getFragmentPlan(query3);
        assertContains(plan3, "2:Project\n" +
                "  |  <slot 1> : 40: LO_ORDERDATE\n" +
                "  |  <slot 2> : 41: LO_ORDERKEY\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 42: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_2");

         */

        String query4 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';";
        String plan4 = getFragmentPlan(query4);
        assertContains(plan4, "2:Project\n" +
                "  |  <slot 1> : 43: LO_ORDERDATE\n" +
                "  |  <slot 2> : 44: LO_ORDERKEY\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 45: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_3");
        /*
        String query5 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000 and lo_orderpriority='5-LOW';";
        String plan5 = getFragmentPlan(query5);
        assertContains(plan5, "1:Project\n" +
                "  |  <slot 1> : 43: LO_ORDERDATE\n" +
                "  |  <slot 2> : 44: LO_ORDERKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_3");

        String query6 = "select LO_ORDERDATE, LO_ORDERKEY, (LO_SUPPLYCOST + 1) * 2 from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';";
        String plan6 = getFragmentPlan(query6);
        assertContains(plan6, "2:Project\n" +
                "  |  <slot 1> : 41: LO_ORDERDATE\n" +
                "  |  <slot 2> : 42: LO_ORDERKEY\n" +
                "  |  <slot 40> : 44: add_one * 2\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 43: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_4\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_4");

        String query7 = "select LO_ORDERKEY, (LO_SUPPLYCOST + 1) * 2, LO_ORDERDATE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';";
        String plan7 = getFragmentPlan(query7);
        assertContains(plan7, "2:Project\n" +
                "  |  <slot 1> : 41: LO_ORDERDATE\n" +
                "  |  <slot 2> : 42: LO_ORDERKEY\n" +
                "  |  <slot 40> : 44: add_one * 2\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 43: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_4\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_4");

         */
    }

    @Test
    public void testJoin() throws Exception {
        /*
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
                    if (tableName.getTbl().equals("t0")) {
                        OlapTable tbl1 = ((OlapTable) testDb.getTable("t0"));
                        for (Partition partition : tbl1.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    } else if (tableName.getTbl().equals("test_all_type")) {
                        OlapTable tbl1 = ((OlapTable) testDb.getTable("test_all_type"));
                        for (Partition partition : tbl1.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    }
                }
            }
        };
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        MaterializedView materializedView = (MaterializedView) db.getTable("join_mv_1");
        Task task = TaskBuilder.buildMvTask(materializedView, db.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

         */

        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1";
        String plan1 = getFragmentPlan(query1);
        assertContains(plan1, "OUTPUT EXPRS:1: v1 | 7: t1d | 6: t1c\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 14: v1\n" +
                "  |  <slot 6> : 16: t1c\n" +
                "  |  <slot 7> : 14: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: join_mv_1");


        /*
        String query2 = "SELECT test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d = 1";
        String plan2 = getFragmentPlan(query2);
        assertContains(plan2, "OUTPUT EXPRS:1: v1 | 7: t1d | 6: t1c\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 14: v1\n" +
                "  |  <slot 6> : 16: t1c\n" +
                "  |  <slot 7> : 14: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: join_mv_1");

         */

    }

    private void setPartitionVersion(Partition partition, long version) {
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
}
