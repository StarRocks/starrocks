package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.analysis.DmlStmt;
import com.starrocks.analysis.InsertStmt;
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
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionBasedMaterializedViewRefreshProcessorTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2022-02-01'),\n" +
                        "    PARTITION p2 values less than('2022-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view test.mv1\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2)\n" +
                        "refresh manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;")
                .withNewMaterializedView("create materialized view test.mv_inactive\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2)\n" +
                        "refresh manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;")
                .withNewMaterializedView("create materialized view test.mv_without_partition\n" +
                        "distributed by hash(k2)\n" +
                        "refresh manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select k2, sum(v1) as total_sum from tbl3 group by k2;");
    }

    @Test
    public void test() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
                    if (tableName.getTbl().equals("tbl1")) {
                        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
                        for (Partition partition : tbl1.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    } else if (tableName.getTbl().equals("tbl2")) {
                        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl2"));
                        for (Partition partition : tbl1.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    }
                }
            }
        };
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        try {
            // first sync partition
            taskRun.executeTaskRun();
            Collection<Partition> partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // add tbl1 partition p5
            String addPartitionSql = "ALTER TABLE test.tbl1 ADD\n" +
                    "PARTITION p5 VALUES [('2022-05-01'),('2022-06-01'))";
            new StmtExecutor(connectContext, addPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(6, partitions.size());
            // drop tbl2 partition p5
            String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p5\n";
            new StmtExecutor(connectContext, dropPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // add tbl2 partition p3
            addPartitionSql = "ALTER TABLE test.tbl2 ADD PARTITION p3 values less than('2022-04-01')";
            new StmtExecutor(connectContext, addPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // drop tbl2 partition p3
            dropPartitionSql = "ALTER TABLE test.tbl2 DROP PARTITION p3";
            new StmtExecutor(connectContext, dropPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // base table partition insert data
            testBaseTablePartitionInsertData(testDb, materializedView, taskRun);

            // base table partition rename
            testBaseTablePartitionRenameWhileSync(testDb, materializedView, taskRun);
            testBaseTablePartitionRenameWhileInsert(testDb, materializedView, taskRun);

            // base table partition replace
            testBaseTablePartitionReplace(testDb, materializedView, taskRun);

            // base table add partition
            testBaseTableAddPartitionWhileSync(testDb, materializedView, taskRun);
            testBaseTableAddPartitionWhileRefresh(testDb, materializedView, taskRun);

            // base table drop partition
            testBaseTableDropPartitionWhileSync(testDb, materializedView, taskRun);
            testBaseTableDropPartitionWhileRefresh(testDb, materializedView, taskRun);

            testRefreshWithFailure(testDb, materializedView, taskRun);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testInactive() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_inactive"));
        materializedView.setActive(false);
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        try {
            taskRun.executeTaskRun();
            Assert.fail("should not be here. executeTaskRun will throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is not active, skip sync partition and data with base tables"));
        }
    }

    @Test
    public void testMvWithoutPartition() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
                    if (tableName.getTbl().equals("tbl1")) {
                        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
                        for (Partition partition : tbl1.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    } else if (tableName.getTbl().equals("tbl2")) {
                        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl2"));
                        for (Partition partition : tbl1.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    }
                }
            }
        };
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_without_partition"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        try {
            taskRun.executeTaskRun();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("refresh failed");
        }
    }

    private void testBaseTablePartitionInsertData(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo basePartitionInfo = baseTableVisibleVersionMap.get(tbl1.getId()).get("p0");
        Assert.assertEquals(1, basePartitionInfo.getVersion());
        // insert new data into tbl1's p0 partition
        // update base table tbl1's p0 version to 2
        String insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();

        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap2 =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo newP0PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p0");
        Assert.assertEquals(2, newP0PartitionInfo.getVersion());

        MaterializedView.BasePartitionInfo p1PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p1");
        Assert.assertEquals(1, p1PartitionInfo.getVersion());
    }

    private void testRefreshWithFailure(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        // insert new data into tbl1's p0 partition
        // update base table tbl1's p0 version to 3
        String insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();

        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            public void processTaskRun(TaskRunContext context) throws Exception {
                throw new RuntimeException("new exception");
            }
        };
        try {
            taskRun.executeTaskRun();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap2 =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo newP0PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p0");
        Assert.assertEquals(3, newP0PartitionInfo.getVersion());
    }

    public void testBaseTablePartitionRenameWhileSync(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p1, p1 renamed with p10 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            private Map<Long, OlapTable> collectBaseTables(MaterializedView materializedView, Database database) {
                Map<Long, OlapTable> olapTables = Maps.newHashMap();
                Set<Long> baseTableIds = materializedView.getBaseTableIds();
                database.readLock();
                try {
                    for (Long baseTableId : baseTableIds) {
                        OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
                        if (olapTable == null) {
                            throw new SemanticException("Materialized view base table: " + baseTableId + " not exist.");
                        }
                        OlapTable copied = new OlapTable();
                        if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                            throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                        }
                        olapTables.put(olapTable.getId(), copied);
                    }
                } finally {
                    database.readUnlock();
                }
                String renamePartitionSql = "ALTER TABLE test.tbl1 RENAME PARTITION p1 p1_1";
                try {
                    // will fail when retry in second times
                    new StmtExecutor(connectContext, renamePartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };
        // insert new data into tbl1's p1 partition
        // update base table tbl1's p1 version to 2
        String insertSql = "insert into tbl1 partition(p1) values('2022-01-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo p1BasePartitionInfo =
                baseTableVisibleVersionMap.get(tbl1.getId()).get("p1_1");
        Assert.assertEquals(2, p1BasePartitionInfo.getVersion());
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p1"));
    }

    public void testBaseTablePartitionRenameWhileInsert(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p1, p1 renamed with p10 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt) throws Exception {
                // rename
                String renamePartitionSql = "ALTER TABLE test.tbl1 RENAME PARTITION p2 p2_1";
                try {
                    // will fail when retry in second times
                    new StmtExecutor(connectContext, renamePartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ConnectContext ctx = mvContext.getCtx();
                StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
                ctx.setExecutor(executor);
                ctx.setThreadLocalInfo();
                ctx.setStmtId(new AtomicInteger().incrementAndGet());
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
                executor.handleDMLStmt(execPlan, insertStmt);
            }
        };
        // insert new data into tbl1's p2 partition
        String insertSql = "insert into tbl1 values('2022-02-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo p2BasePartitionInfo =
                baseTableVisibleVersionMap.get(tbl1.getId()).get("p2");
        Assert.assertEquals(2, p2BasePartitionInfo.getVersion());
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p2_1"));
    }

    private void testBaseTablePartitionReplace(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p2, p2 replace with tp2 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            public Map<Long, OlapTable> collectBaseTables(MaterializedView materializedView, Database database) {
                Map<Long, OlapTable> olapTables = Maps.newHashMap();
                Set<Long> baseTableIds = materializedView.getBaseTableIds();
                database.readLock();
                try {
                    for (Long baseTableId : baseTableIds) {
                        OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
                        if (olapTable == null) {
                            throw new SemanticException("Materialized view base table: " + baseTableId + " not exist.");
                        }
                        OlapTable copied = new OlapTable();
                        if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                            throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                        }
                        olapTables.put(olapTable.getId(), copied);
                    }
                } finally {
                    database.readUnlock();
                }
                String replacePartitionSql = "ALTER TABLE test.tbl1 REPLACE PARTITION (p3) WITH TEMPORARY PARTITION (tp3)\n" +
                        "PROPERTIES (\n" +
                        "    \"strict_range\" = \"false\",\n" +
                        "    \"use_temp_partition_name\" = \"false\"\n" +
                        ");";
                try {
                    new StmtExecutor(connectContext, replacePartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };
        // change partition and replica versions
        Partition partition = tbl1.getPartition("p3");
        String createTempPartitionSql =
                "ALTER TABLE test.tbl1 ADD TEMPORARY PARTITION tp3 values [('2022-03-01'),('2022-04-01'))";
        new StmtExecutor(connectContext, createTempPartitionSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo basePartitionInfo = baseTableVisibleVersionMap.get(tbl1.getId()).get("p3");
        Assert.assertNotEquals(partition.getId(), basePartitionInfo.getId());
        Assert.assertNotEquals(partition.getVisibleVersion(), basePartitionInfo.getVersion());
    }

    public void testBaseTableAddPartitionWhileSync(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p3, add partition p99 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            public Map<Long, OlapTable> collectBaseTables(MaterializedView materializedView, Database database) {
                Map<Long, OlapTable> olapTables = Maps.newHashMap();
                Set<Long> baseTableIds = materializedView.getBaseTableIds();
                database.readLock();
                try {
                    for (Long baseTableId : baseTableIds) {
                        OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
                        if (olapTable == null) {
                            throw new SemanticException("Materialized view base table: " + baseTableId + " not exist.");
                        }
                        OlapTable copied = new OlapTable();
                        if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                            throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                        }
                        olapTables.put(olapTable.getId(), copied);
                    }
                } finally {
                    database.readUnlock();
                }
                String addPartitionSql = "ALTER TABLE test.tbl1 ADD PARTITION p99 VALUES [('9999-03-01'),('9999-04-01'))";
                try {
                    new StmtExecutor(connectContext, addPartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(2, baseTableVisibleVersionMap.get(tbl1.getId()).get("p3").getVersion());
        Assert.assertNotNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p99"));
        Assert.assertEquals(1, baseTableVisibleVersionMap.get(tbl1.getId()).get("p99").getVersion());
    }

    public void testBaseTableAddPartitionWhileRefresh(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p3, add partition p99 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt) throws Exception {
                String addPartitionSql = "ALTER TABLE test.tbl1 ADD PARTITION p100 VALUES [('9999-04-01'),('9999-05-01'))";
                try {
                    new StmtExecutor(connectContext, addPartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ConnectContext ctx = mvContext.getCtx();
                StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
                ctx.setExecutor(executor);
                ctx.setThreadLocalInfo();
                ctx.setStmtId(new AtomicInteger().incrementAndGet());
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
                executor.handleDMLStmt(execPlan, insertStmt);
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        new StmtExecutor(connectContext, insertSql).execute();

        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(3, baseTableVisibleVersionMap.get(tbl1.getId()).get("p3").getVersion());
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p100"));
    }

    public void testBaseTableDropPartitionWhileSync(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p4, drop partition p4 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            private Map<Long, OlapTable> collectBaseTables(MaterializedView materializedView, Database database) {
                Map<Long, OlapTable> olapTables = Maps.newHashMap();
                Set<Long> baseTableIds = materializedView.getBaseTableIds();
                database.readLock();
                try {
                    for (Long baseTableId : baseTableIds) {
                        OlapTable olapTable = (OlapTable) database.getTable(baseTableId);
                        if (olapTable == null) {
                            throw new SemanticException("Materialized view base table: " + baseTableId + " not exist.");
                        }
                        OlapTable copied = new OlapTable();
                        if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                            throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                        }
                        olapTables.put(olapTable.getId(), copied);
                    }
                } finally {
                    database.readUnlock();
                }
                String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p4";
                try {
                    new StmtExecutor(connectContext, dropPartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p4"));
    }

    public void testBaseTableDropPartitionWhileRefresh(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // drop partition p4 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMaterializedViewRefreshProcessor>() {
            @Mock
            public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt) throws Exception {
                String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p100";
                try {
                    new StmtExecutor(connectContext, dropPartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ConnectContext ctx = mvContext.getCtx();
                StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
                ctx.setExecutor(executor);
                ctx.setThreadLocalInfo();
                ctx.setStmtId(new AtomicInteger().incrementAndGet());
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
                executor.handleDMLStmt(execPlan, insertStmt);
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertNotNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p100"));
        Assert.assertEquals(1, baseTableVisibleVersionMap.get(tbl1.getId()).get("p100").getVersion());
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
