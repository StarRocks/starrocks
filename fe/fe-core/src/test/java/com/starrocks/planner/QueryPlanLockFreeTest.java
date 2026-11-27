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

package com.starrocks.planner;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanningVersionRecheck;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QueryPlanLockFreeTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String DB_NAME = "test";

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        FeConstants.enablePruneEmptyOutputScan = false;

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `t0` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `k2` int(11) NULL,\n" +
                "  `k3` int(11) NULL,\n" +
                "  `v1` int SUM NULL,\n" +
                "  `v2` bigint SUM NULL,\n" +
                "  `v3` largeint SUM NULL,\n" +
                "  `v4` double SUM NULL,\n" +
                "  `v5` decimal(10, 3) SUM NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `t1` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `k2` int(11) NULL,\n" +
                "  `k3` int(11) NULL,\n" +
                "  `v1` int SUM NULL,\n" +
                "  `v2` bigint SUM NULL,\n" +
                "  `v3` largeint SUM NULL,\n" +
                "  `v4` double SUM NULL,\n" +
                "  `v5` decimal(10, 3) SUM NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `t_two_partitions` (\n" +
                "  `k1` int NULL,\n" +
                "  `v1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`) (\n" +
                "  PARTITION p1 VALUES LESS THAN (\"10\"),\n" +
                "  PARTITION p2 VALUES LESS THAN (\"20\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        for (String name : List.of("t_limit_moved", "t_minmax_moved", "t_mv_base", "t_sync_base", "t_reread_bound",
                "t_count_moved", "t_reread_held", "t_insert_bound", "t_insert_bound_target", "t_transparent_base")) {
            starRocksAssert.withTable(twoPartitionTable(name));
        }
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `mv_moved`\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `k1`, `v1` FROM `t_mv_base`;");
        // Never refreshed: a query of it is answered by its defined query, over the published base table.
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `mv_transparent`\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\", \"transparent_mv_rewrite_mode\" = \"true\")\n" +
                "AS SELECT `k1`, `v1` FROM `t_transparent_base`;");
    }

    private static String twoPartitionTable(String name) {
        return "CREATE TABLE IF NOT EXISTS `" + name + "` (\n" +
                "  `k1` int NULL,\n" +
                "  `v1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`) (\n" +
                "  PARTITION p1 VALUES LESS THAN (\"10\"),\n" +
                "  PARTITION p2 VALUES LESS THAN (\"20\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");";
    }

    // Every replica of the partition already holds its next version, rowsPerTablet rows in each tablet: a load that
    // reached the replicas and is not visible yet.
    private static void holdNextLoad(PhysicalPartition partition, long rowsPerTablet) {
        long next = partition.getVisibleVersion() + 1;
        for (Tablet tablet : partition.getQueryableBaseIndex().getTablets()) {
            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                replica.updateRowCount(next, 1024L, rowsPerTablet);
            }
        }
    }

    // Plans sql the way a lock-free query is planned, running duringPlanning once the table copies were taken and the
    // locks dropped, before the scan ranges are built.
    private static ExecPlan planWhile(String sql, Runnable duringPlanning) throws Exception {
        connectContext.getSessionVariable().setCboUseDBLock(false);
        AtomicBoolean armed = new AtomicBoolean(true);
        new MockUp<OptimizerFactory>() {
            @Mock
            public Optimizer create(Invocation invocation, OptimizerContext context) {
                if (armed.compareAndSet(true, false)) {
                    duringPlanning.run();
                }
                return invocation.proceed(context);
            }
        };
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setThreadLocalInfo();
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan plan = StatementPlanner.plan(stmt, connectContext);
        Assertions.assertFalse(armed.get(), "the plan was not built through the optimizer");
        return plan;
    }

    @Test
    public void testPlanStrategy() throws Exception {
        String sql = "select * from t0";
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), "default_catalog", DB_NAME, "t0");
        table.lastSchemaUpdateTime.set(System.nanoTime() + 10000000000L);
        Assertions.assertThrows(StarRocksPlannerException.class, () -> UtFrameUtils.getPlanAndFragment(connectContext, sql), "schema of [t0] had been updated frequently during the plan generation");

        connectContext.getSessionVariable().setCboUseDBLock(true);
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        Assertions.assertTrue(plan.first.contains("SCAN"), plan.first);
        connectContext.getSessionVariable().setCboUseDBLock(false);

        // follower node
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);
        Assertions.assertThrows(StarRocksPlannerException.class, () -> UtFrameUtils.getPlanAndFragment(connectContext, sql), "schema of [t0] had been updated frequently during the plan generation");
    }

    @Test
    public void testCopiedTable() throws Exception {
        String sql = "select t1.* from t1 t1 join t1 t2 on t1.k1 = t2.k2";
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), "default_catalog", DB_NAME, "t1");
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        OlapScanNode node1 = (OlapScanNode) plan.second.getScanNodes().get(0);
        OlapScanNode node2 = (OlapScanNode) plan.second.getScanNodes().get(1);
        Assertions.assertTrue(table != node1.getOlapTable(), "original table should different from copied table in plan");
        Assertions.assertTrue(table != node2.getOlapTable(), "original table should different from copied table in plan");

        Assertions.assertTrue(node2.getOlapTable() == node1.getOlapTable(), "copied tables should share the same object");
    }

    /**
     * StarRocksTest#12648. A transaction that wrote two partitions publishes them one after the other under the
     * table WRITE lock -- on the leader in finishTransaction, on a follower in replay. A lock-free plan copied the
     * table before that and builds its scan ranges while the publication is half done. Holding the WRITE lock does
     * not stop it from reading the partitions then, so the plan must read the versions it copied: both from before
     * the transaction, never one from each side of it.
     */
    @Test
    public void testScanVersionsComeFromThePlanningSnapshot() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_two_partitions");
        List<PhysicalPartition> partitions = List.of(table.getPartition("p1").getDefaultPhysicalPartition(),
                table.getPartition("p2").getDefaultPhysicalPartition());
        // Both partitions hold one load (version 2), and every replica already has the next one (version 3): it has
        // been published to the replicas but not made visible yet.
        for (PhysicalPartition partition : partitions) {
            partition.updateVisibleVersion(2L);
            for (Tablet tablet : partition.getQueryableBaseIndex().getTablets()) {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    replica.updateVersion(3L);
                }
            }
        }

        CountDownLatch firstPublished = new CountDownLatch(1);
        CountDownLatch planBuilt = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread publisher = new Thread(() -> {
            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            try {
                partitions.get(0).updateVisibleVersion(3L);
                firstPublished.countDown();
                Assertions.assertTrue(planBuilt.await(10, TimeUnit.SECONDS));
                partitions.get(1).updateVisibleVersion(3L);
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }
        }, "half-done-publish");
        AtomicBoolean armed = new AtomicBoolean(true);
        new MockUp<OptimizerFactory>() {
            // Runs after the snapshot was taken and the lock dropped, before the scan ranges are built.
            @Mock
            public Optimizer create(Invocation invocation, OptimizerContext context) throws InterruptedException {
                if (armed.compareAndSet(true, false)) {
                    publisher.start();
                    Assertions.assertTrue(firstPublished.await(10, TimeUnit.SECONDS));
                }
                return invocation.proceed(context);
            }
        };

        OlapScanNode scanNode;
        Map<Long, Long> scanVersions;
        try {
            connectContext.setQueryId(UUIDUtil.genUUID());
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
            connectContext.setThreadLocalInfo();
            StatementBase stmt = SqlParser.parse("select * from t_two_partitions",
                    connectContext.getSessionVariable()).get(0);
            // Not UtFrameUtils.getPlanAndFragment: it plans the statement again as a view, which needs the lock the
            // publisher holds until this plan is built.
            ExecPlan plan = StatementPlanner.plan(stmt, connectContext);
            scanNode = (OlapScanNode) plan.getScanNodes().get(0);
            scanVersions = Map.copyOf(scanNode.getScanPartitionVersions());
        } finally {
            planBuilt.countDown();
            publisher.join(10_000);
        }
        Assertions.assertNull(failure.get());
        Assertions.assertFalse(armed.get());
        Assertions.assertTrue(scanNode.getOlapTable().isPlanningSnapshot(), "the query was not planned off a snapshot");
        Assertions.assertEquals(Map.of(partitions.get(0).getId(), 2L, partitions.get(1).getId(), 2L), scanVersions,
                "the plan must read the transaction in all of its partitions or in none");
        Assertions.assertEquals(3L, partitions.get(1).getVisibleVersion());
    }

    /**
     * LIMIT tablet pruning chooses the tablets by their live row counts, which can count a load the versions the table
     * copy recorded predate. When a load moves a partition the plan reads while it is planned, the plan re-reads the
     * versions before building its scan ranges, so the tablets it chose hold the rows it counted.
     */
    @Test
    public void testLimitPrunedPlanRereadsVersionsThatMovedWhilePlanning() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_limit_moved");
        List<PhysicalPartition> partitions = List.of(table.getPartition("p1").getDefaultPhysicalPartition(),
                table.getPartition("p2").getDefaultPhysicalPartition());
        for (PhysicalPartition partition : partitions) {
            holdNextLoad(partition, 10L);
        }

        // The load becomes visible after the copy was taken and before the scan ranges are built.
        ExecPlan plan = planWhile("select * from t_limit_moved limit 1", () -> {
            for (PhysicalPartition partition : partitions) {
                partition.updateVisibleVersion(partition.getVisibleVersion() + 1);
            }
        });

        OlapScanNode scanNode = (OlapScanNode) plan.getScanNodes().get(0);
        Assertions.assertTrue(scanNode.getOlapTable().isPlanningVersionsMarkedForRecheck(),
                "LIMIT tablet pruning chose the tablets by their row counts");
        Assertions.assertEquals(1, scanNode.getScanTabletIds().size());
        Assertions.assertFalse(scanNode.getScanPartitionVersions().isEmpty());
        scanNode.getScanPartitionVersions().forEach((partitionId, version) ->
                Assertions.assertEquals(table.getPhysicalPartition(partitionId).getVisibleVersion(),
                        version.longValue(), "the plan reads the versions the load made visible"));
    }

    /**
     * The partition-column MIN/MAX rewrite chooses the partition by whether it holds data, read live. A plan it
     * changed re-reads the versions of what it reads when a load moved them while it was planned.
     */
    @Test
    public void testPartitionMinMaxPlanRereadsVersionsThatMovedWhilePlanning() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_minmax_moved");
        PhysicalPartition p2 = table.getPartition("p2").getDefaultPhysicalPartition();
        holdNextLoad(p2, 10L);
        long next = p2.getVisibleVersion() + 1;

        ExecPlan plan = planWhile("select max(k1) from t_minmax_moved", () -> p2.updateVisibleVersion(next));

        OlapScanNode scanNode = (OlapScanNode) plan.getScanNodes().get(0);
        Assertions.assertTrue(scanNode.getOlapTable().isPlanningVersionsMarkedForRecheck(),
                "the MIN/MAX rewrite chose the partition by whether it holds data");
        Assertions.assertEquals(Map.of(p2.getId(), next), Map.copyOf(scanNode.getScanPartitionVersions()));
    }

    @Test
    public void testCountIsLeftToAMetaScanOnceAVersionMovedWhilePlanning() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_count_moved");
        // Every tablet's count proven at its partition's visible version, as a stat collection leaves it.
        for (PhysicalPartition physical : table.getPhysicalPartitions()) {
            for (Tablet tablet : physical.getQueryableBaseIndex().getTablets()) {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    replica.updateStat(1024L, 10L, 1L, physical.getVisibleVersion());
                }
            }
        }
        String sql = "select count(*) from t_count_moved";
        boolean rewriteToMetaScan = connectContext.getSessionVariable().isEnableRewriteSimpleAggToMetaScan();
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        try {
            Assertions.assertTrue(UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getScanNodes().isEmpty(),
                    "the count is folded from the proven stats");

            // A load becomes visible after the copy, and the stats still prove the version the copy recorded. A recheck
            // could re-read the plan's other scans at the new version, which a folded count would not follow.
            PhysicalPartition p2 = table.getPartition("p2").getDefaultPhysicalPartition();
            long next = p2.getVisibleVersion() + 1;
            ExecPlan plan = planWhile(sql, () -> p2.updateVisibleVersion(next));
            Assertions.assertEquals(1, plan.getScanNodes().size());
            Assertions.assertTrue(plan.getScanNodes().get(0) instanceof MetaScanNode,
                    plan.getScanNodes().get(0).getClass().getName());
        } finally {
            connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(rewriteToMetaScan);
        }
    }

    /**
     * A plan that reads an asynchronous MV counts as marked, since an MV rewrite judges the MV fresh against the live
     * base tables. Read directly, an MV whose partition a refresh moved while the plan was built is re-read.
     */
    @Test
    public void testPlanReadingAnMvRereadsVersionsThatMovedWhilePlanning() throws Exception {
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "mv_moved");
        PhysicalPartition partition = mv.getPartitions().iterator().next().getDefaultPhysicalPartition();
        holdNextLoad(partition, 10L);
        long next = partition.getVisibleVersion() + 1;

        ExecPlan plan = planWhile("select * from mv_moved", () -> partition.updateVisibleVersion(next));

        OlapScanNode scanNode = (OlapScanNode) plan.getScanNodes().get(0);
        Assertions.assertTrue(scanNode.getOlapTable() instanceof MaterializedView);
        Assertions.assertTrue(scanNode.getOlapTable().isPlanningSnapshot(), "the MV was not read off a planning copy");
        Assertions.assertEquals(Map.of(partition.getId(), next), Map.copyOf(scanNode.getScanPartitionVersions()));
    }

    /**
     * MV rewrite builds a synchronous MV over a rollup index of its base table. The MV carries the index's id, which
     * names no table, and shares the base table's physical partitions, so the base table's lock guards it: a plan that
     * reads a partition through the MV and through the base table re-reads it in both when a load moved it.
     */
    @Test
    public void testSyncMvIsRereadUnderItsBaseTableLock() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable base = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_sync_base");
        // The analyzer records the database of each table it resolves, and the copies keep it.
        base.maySetDatabaseId(db.getId());
        // MV rewrite builds the MV from a rollup index; the base index stands in for one here.
        MaterializedView syncMv = new MaterializedView(db, base.getName(),
                base.getIndexMetaByMetaId(base.getBaseIndexMetaId()), base, base.getPartitionInfo(),
                base.getDefaultDistributionInfo().copy(),
                new MaterializedView.MvRefreshScheme(MaterializedViewRefreshType.SYNC));
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), syncMv.getId()),
                "the MV's id names no table");
        Assertions.assertEquals(Pair.create(db.getId(), base.getId()), syncMv.getPartitionLockTarget());

        OlapTable baseCopy = new OlapTable();
        MaterializedView mvCopy = new MaterializedView();
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), base.getId(), LockType.READ);
        try {
            base.copyOnlyForQuery(baseCopy);
            syncMv.copyOnlyForQuery(mvCopy);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), base.getId(), LockType.READ);
        }

        // A load moves p1 after both copies were taken and before the scan ranges are built.
        PhysicalPartition p1 = base.getPartition("p1").getDefaultPhysicalPartition();
        long next = p1.getVisibleVersion() + 1;
        p1.updateVisibleVersion(next);

        List<Long> partitionIds = List.of(base.getPartition("p1").getId(), base.getPartition("p2").getId());
        OptExpression plan = OptExpression.create(
                new PhysicalUnionOperator(List.of(), List.of(), true, Operator.DEFAULT_LIMIT, null, null),
                OptExpression.create(scanOf(baseCopy, partitionIds)), OptExpression.create(scanOf(mvCopy, partitionIds)));
        PlanningVersionRecheck.recheck(plan);

        Assertions.assertEquals(next, baseCopy.getVisibleVersionForQuery(p1));
        Assertions.assertEquals(next, mvCopy.getVisibleVersionForQuery(p1),
                "the MV reads the partition at the version its base table re-read");
    }

    /**
     * A transparent MV that is not fresh is answered by its defined query, whose scans read the published base tables
     * at their live versions. Joined with one of those base tables, the plan reads the table through its planning copy
     * too. It counts as marked: a load that became visible while it was planned is read by both scans or by neither,
     * as on main, where both read the live version when their scan ranges are built.
     */
    @Test
    public void testCopyBesideAScanOfThePublishedTableIsReread() throws Exception {
        OlapTable base = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_transparent_base");
        List<PhysicalPartition> partitions = List.of(base.getPartition("p1").getDefaultPhysicalPartition(),
                base.getPartition("p2").getDefaultPhysicalPartition());
        // Both partitions hold one load (version 2), and every replica already has the next one (version 3).
        for (PhysicalPartition partition : partitions) {
            partition.updateVisibleVersion(2L);
            for (Tablet tablet : partition.getQueryableBaseIndex().getTablets()) {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    replica.updateVersion(3L);
                }
            }
        }

        // The load becomes visible after the copy was taken and before the scan ranges are built.
        ExecPlan plan = planWhile(
                "select m.k1, b.v1 from mv_transparent m join t_transparent_base b on m.k1 = b.k1",
                () -> partitions.forEach(partition -> partition.updateVisibleVersion(3L)));

        List<OlapScanNode> baseScans = plan.getScanNodes().stream()
                .filter(node -> node instanceof OlapScanNode).map(node -> (OlapScanNode) node)
                .filter(node -> node.getOlapTable().getId() == base.getId()).toList();
        Assertions.assertTrue(baseScans.stream().anyMatch(node -> node.getOlapTable().isPlanningSnapshot()),
                "the statement's scan of the table reads its planning copy");
        Assertions.assertTrue(baseScans.stream().anyMatch(node -> !node.getOlapTable().isPlanningSnapshot()),
                "the MV's defined query scans the published table");
        Map<Long, Long> live = Map.of(partitions.get(0).getId(), 3L, partitions.get(1).getId(), 3L);
        for (OlapScanNode scanNode : baseScans) {
            Assertions.assertEquals(live, Map.copyOf(scanNode.getScanPartitionVersions()),
                    "every scan of the table reads the load");
        }
    }

    /**
     * MV rewrite scans the live MV, not a copy, when it cannot lock the MV for the copy or does not copy MVs at all.
     * The MV was still judged fresh against the live base tables, so such a plan counts as marked too: the base-table
     * copies read beside the MV are re-read when a load moved them.
     */
    @Test
    public void testPlanReadingALiveMvRereadsTheCopiesBesideIt() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable base = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_mv_base");
        MaterializedView liveMv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "mv_moved");
        base.maySetDatabaseId(db.getId());
        OlapTable baseCopy = new OlapTable();
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), base.getId(), LockType.READ);
        try {
            base.copyOnlyForQuery(baseCopy);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), base.getId(), LockType.READ);
        }
        Assertions.assertFalse(liveMv.hasPlanningVisibleVersions(), "the MV is the published one");

        // A load moves p1 after the copy was taken and before the scan ranges are built.
        PhysicalPartition p1 = base.getPartition("p1").getDefaultPhysicalPartition();
        holdNextLoad(p1, 10L);
        long next = p1.getVisibleVersion() + 1;
        p1.updateVisibleVersion(next);

        List<Long> basePartitionIds = List.of(base.getPartition("p1").getId(), base.getPartition("p2").getId());
        List<Long> mvPartitionIds = liveMv.getPartitions().stream().map(Partition::getId).toList();
        OptExpression plan = OptExpression.create(
                new PhysicalUnionOperator(List.of(), List.of(), true, Operator.DEFAULT_LIMIT, null, null),
                OptExpression.create(scanOf(baseCopy, basePartitionIds)),
                OptExpression.create(scanOf(liveMv, mvPartitionIds)));
        PlanningVersionRecheck.recheck(plan);

        Assertions.assertEquals(next, baseCopy.getVisibleVersionForQuery(p1),
                "the base table is read at a version no older than the one the MV was judged against");
    }

    /**
     * A planner that takes its lock back within a bound (an MV refresh) re-reads within the same bound, and gives up
     * as taking the planner lock back does.
     */
    @Test
    public void testRereadWaitsNoLongerThanThePlannerLockBound() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_reread_bound");
        // The analyzer records the database of each table it resolves, and the copies keep it.
        table.maySetDatabaseId(db.getId());
        OlapTable copy = new OlapTable();
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            table.copyOnlyForQuery(copy);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
        copy.markPlanningVersionsForRecheck();

        // A load moves p1 after the copy was taken, and a writer holds the table while the plan re-reads.
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        long recorded = p1.getVisibleVersion();
        p1.updateVisibleVersion(recorded + 1);
        CountDownLatch writeLocked = new CountDownLatch(1);
        CountDownLatch rereadDone = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            Locker writeLocker = new Locker();
            writeLocker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            try {
                writeLocked.countDown();
                rereadDone.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writeLocker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }
        }, "write-lock-holder");
        writer.start();
        List<Long> partitionIds = List.of(table.getPartition("p1").getId(), table.getPartition("p2").getId());
        OptExpression plan = OptExpression.create(scanOf(copy, partitionIds));
        try {
            Assertions.assertTrue(writeLocked.await(10, TimeUnit.SECONDS));
            long startMs = System.currentTimeMillis();
            try (PlanningVersionRecheck.LockWaitBound ignored = PlanningVersionRecheck.boundLockWaits(200)) {
                Assertions.assertThrows(PlannerMetaLocker.AcquireTimeoutException.class,
                        () -> PlanningVersionRecheck.recheck(plan));
            }
            Assertions.assertTrue(System.currentTimeMillis() - startMs < 5_000, "the re-read waited past its bound");
        } finally {
            rereadDone.countDown();
            writer.join(10_000);
        }
        Assertions.assertEquals(recorded, copy.getVisibleVersionForQuery(p1), "a re-read that gave up re-read nothing");
    }

    /**
     * An INSERT planned off table copies under a planner lock bound, as an MV refresh plans one, re-reads within
     * that bound too: behind a writer it gives up as taking the planner lock back does, and does not wait for the
     * writer.
     */
    @Test
    public void testInsertRereadsWithinItsPlannerLockBound() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_insert_bound");
        List<PhysicalPartition> partitions = List.of(table.getPartition("p1").getDefaultPhysicalPartition(),
                table.getPartition("p2").getDefaultPhysicalPartition());
        for (PhysicalPartition partition : partitions) {
            holdNextLoad(partition, 10L);
        }
        CountDownLatch writeLocked = new CountDownLatch(1);
        CountDownLatch planned = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            Locker writeLocker = new Locker();
            writeLocker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            try {
                writeLocked.countDown();
                planned.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writeLocker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }
        }, "write-lock-holder");

        // Once the copies were taken and the planner lock dropped, a load moves both partitions the LIMIT-pruned scan
        // reads, and a writer takes the table.
        AtomicBoolean armed = new AtomicBoolean(true);
        new MockUp<OptimizerFactory>() {
            @Mock
            public Optimizer create(Invocation invocation, OptimizerContext context) {
                if (armed.compareAndSet(true, false)) {
                    for (PhysicalPartition partition : partitions) {
                        partition.updateVisibleVersion(partition.getVisibleVersion() + 1);
                    }
                    writer.start();
                    try {
                        writeLocked.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return invocation.proceed(context);
            }
        };
        connectContext.getSessionVariable().setCboUseDBLock(false);
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setThreadLocalInfo();
        InsertStmt insertStmt = (InsertStmt) SqlParser.parse(
                "insert into t_insert_bound_target select * from t_insert_bound limit 1",
                connectContext.getSessionVariable()).get(0);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, insertStmt);
        locker.setAcquireTimeoutMs(200);
        Assertions.assertTrue(locker.tryLock(10, TimeUnit.SECONDS));
        try {
            Analyzer.analyze(insertStmt, connectContext);
            long startMs = System.currentTimeMillis();
            Assertions.assertThrows(PlannerMetaLocker.AcquireTimeoutException.class,
                    () -> new InsertPlanner(locker, true).plan(insertStmt, connectContext));
            Assertions.assertTrue(System.currentTimeMillis() - startMs < 5_000, "the re-read waited past its bound");
        } finally {
            planned.countDown();
            writer.join(10_000);
            // A no-op once the planner gave up taking the lock back.
            locker.unlock();
        }
        Assertions.assertFalse(armed.get(), "the INSERT was not planned through the optimizer");
        Assertions.assertEquals(0, writeLocked.getCount(), "the writer never took the table");
    }

    @Test
    public void testRereadUnderAHeldLockTakesNoMoreLocks() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_reread_held");
        OlapTable held = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB_NAME, "t0");
        table.maySetDatabaseId(db.getId());
        OlapTable copy = new OlapTable();
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            table.copyOnlyForQuery(copy);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
        copy.markPlanningVersionsForRecheck();

        // A load moves p1 after the copy was taken, and a writer holds the table while the plan re-reads. The
        // statement holds no lock on this table, as a [_SYNC_MV_] relation's base table is not in its lock set.
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        long next = p1.getVisibleVersion() + 1;
        p1.updateVisibleVersion(next);
        CountDownLatch writeLocked = new CountDownLatch(1);
        CountDownLatch rereadDone = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            Locker writeLocker = new Locker();
            writeLocker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            try {
                writeLocked.countDown();
                rereadDone.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writeLocker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }
        }, "write-lock-holder");
        writer.start();
        List<Long> partitionIds = List.of(table.getPartition("p1").getId(), table.getPartition("p2").getId());
        OptExpression plan = OptExpression.create(scanOf(copy, partitionIds));
        // This thread plans under a statement lock on another table, as the locked INSERT and DML paths do.
        Locker statementLocker = new Locker();
        try {
            Assertions.assertTrue(writeLocked.await(10, TimeUnit.SECONDS));
            statementLocker.lockTableWithIntensiveDbLock(db.getId(), held.getId(), LockType.READ);
            long startMs = System.currentTimeMillis();
            try {
                PlanningVersionRecheck.recheck(plan);
            } finally {
                statementLocker.unLockTableWithIntensiveDbLock(db.getId(), held.getId(), LockType.READ);
            }
            Assertions.assertTrue(System.currentTimeMillis() - startMs < 5_000,
                    "the re-read waited for a lock while holding one");
        } finally {
            rereadDone.countDown();
            writer.join(10_000);
        }
        Assertions.assertEquals(next, copy.getVisibleVersionForQuery(p1), "re-read without a lock");
    }

    private static PhysicalOlapScanOperator scanOf(OlapTable table, List<Long> partitionIds) {
        return new PhysicalOlapScanOperator(table, Map.of(), null, Operator.DEFAULT_LIMIT, null,
                table.getBaseIndexMetaId(), partitionIds, List.of(), List.of(), List.of(), null, false, null);
    }
}
