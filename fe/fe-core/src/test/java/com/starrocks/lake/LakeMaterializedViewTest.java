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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedView.MvRefreshScheme;
import com.starrocks.catalog.MaterializedView.RefreshType;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Task;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.util.ThreadUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LakeMaterializedViewTest {
    private static final String DB = "db_for_lake_mv";

    private static PseudoCluster cluster;
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        Config.run_mode = "shared_data";
        Config.enable_experimental_mv = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        cluster = PseudoCluster.getInstance();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        starRocksAssert.withTable("CREATE TABLE base_table\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3");
    }

    @AfterClass
    public static void tearDown() {
        PseudoCluster.getInstance().shutdown(true);
        Config.run_mode = "shared_nothing";
    }

    @Test
    public void testMaterializedView() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.META_VERSION;
            }
        };

        long dbId = 1L;
        long mvId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 10L;
        long tablet2Id = 11L;

        // Schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet
        Tablet tablet1 = new LakeTablet(tablet1Id);
        Tablet tablet2 = new LakeTablet(tablet2Id);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, mvId, partitionId, indexId, 0, TStorageMedium.HDD, true);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);

        // refresh scheme
        MvRefreshScheme mvRefreshScheme = new MvRefreshScheme();
        mvRefreshScheme.setType(RefreshType.SYNC);

        // Lake mv
        LakeMaterializedView mv = new LakeMaterializedView(mvId, dbId, "mv1", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, mvRefreshScheme);
        Deencapsulation.setField(mv, "baseIndexId", indexId);
        mv.addPartition(partition);
        mv.setIndexMeta(indexId, "mv1", columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);

        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/1/");
        FilePathInfo pathInfo = builder.build();
        mv.setStorageInfo(pathInfo, true, 3600, true);

        // Test serialize and deserialize
        FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            mv.write(out);
            out.flush();
        }

        Table newTable = null;
        try (DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream())) {
            newTable = Table.read(in);
        }
        byteArrayOutputStream.close();

        // Check lake mv and lake tablet
        Assert.assertTrue(newTable.isCloudNativeMaterializedView());
        Assert.assertTrue(newTable.isCloudNativeTableOrMaterializedView());
        LakeMaterializedView newMv = (LakeMaterializedView) newTable;

        Assert.assertEquals("s3://test-bucket/1/", newMv.getStoragePath());
        FileCacheInfo cacheInfo = newMv.getPartitionFileCacheInfo(partitionId);
        Assert.assertTrue(cacheInfo.getEnableCache());
        Assert.assertEquals(3600, cacheInfo.getTtlSeconds());
        Assert.assertTrue(cacheInfo.getAsyncWriteBack());

        Partition p1 = newMv.getPartition(partitionId);
        MaterializedIndex newIndex = p1.getBaseIndex();
        long expectedTabletId = 10L;
        for (Tablet tablet : newIndex.getTablets()) {
            Assert.assertTrue(tablet instanceof LakeTablet);
            LakeTablet lakeTablet = (LakeTablet) tablet;
            Assert.assertEquals(expectedTabletId, lakeTablet.getId());
            Assert.assertEquals(expectedTabletId, lakeTablet.getShardId());
            ++expectedTabletId;
        }

        // Test selectiveCopy
        MaterializedView newMv2 = mv.selectiveCopy(Lists.newArrayList("p1"), true, IndexExtState.ALL);
        Assert.assertTrue(newMv2.isCloudNativeMaterializedView());
        Assert.assertEquals("s3://test-bucket/1/", newMv.getStoragePath());
        cacheInfo = newMv.getPartitionFileCacheInfo(partitionId);
        Assert.assertTrue(cacheInfo.getEnableCache());
        Assert.assertEquals(3600, cacheInfo.getTtlSeconds());
        Assert.assertTrue(cacheInfo.getAsyncWriteBack());

        // Test appendUniqueProperties
        StringBuilder sb = new StringBuilder();
        Deencapsulation.invoke(newMv2, "appendUniqueProperties", sb);
        String baseProperties = sb.toString();
        Assert.assertTrue(baseProperties.contains("\"enable_storage_cache\" = \"true\""));
        Assert.assertTrue(baseProperties.contains("\"storage_cache_ttl\" = \"3600\""));
        Assert.assertTrue(baseProperties.contains("\"enable_async_write_back\" = \"true\""));

        Assert.assertNull(mv.delete(true));
        Assert.assertNotNull(mv.delete(false));
    }

    @Test
    public void testCreateMaterializedView() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "PROPERTIES(\n" +
                        "   'enable_storage_cache' = 'true',\n" +
                        "   'storage_cache_ttl' = '3600',\n" +
                        "   'enable_async_write_back' = 'true'\n" +
                        ")\n" +
                        "refresh async\n" +
                        "as select k2, sum(k3) as total from base_table group by k2;");

        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        MaterializedView mv = (MaterializedView) db.getTable("mv1");
        Assert.assertTrue(mv.isCloudNativeMaterializedView());
        Assert.assertTrue(mv.isActive());

        LakeMaterializedView lakeMv = (LakeMaterializedView) mv;
        // same as PseudoStarOSAgent.allocateFilePath
        Assert.assertEquals("s3://test-bucket/1/", lakeMv.getStoragePath());
        // check table default cache info
        FileCacheInfo cacheInfo = lakeMv.getPartitionFileCacheInfo(0L);
        Assert.assertTrue(cacheInfo.getEnableCache());
        Assert.assertEquals(3600, cacheInfo.getTtlSeconds());
        Assert.assertTrue(cacheInfo.getAsyncWriteBack());

        // replication num
        Assert.assertEquals(1L, lakeMv.getDefaultReplicationNum().longValue());

        // show create materialized view
        String ddlStmt = lakeMv.getMaterializedViewDdlStmt(true);
        System.out.println(ddlStmt);
        Assert.assertTrue(ddlStmt.contains("\"replication_num\" = \"1\""));
        Assert.assertTrue(ddlStmt.contains("\"enable_storage_cache\" = \"true\""));
        Assert.assertTrue(ddlStmt.contains("\"storage_cache_ttl\" = \"3600\""));
        Assert.assertTrue(ddlStmt.contains("\"enable_async_write_back\" = \"true\""));

        // check task
        String mvTaskName = "mv-" + mv.getId();
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(mvTaskName);
        Assert.assertNotNull(task);

        starRocksAssert.dropMaterializedView("mv1");
        Assert.assertNull(db.getTable("mv1"));
    }

    @Test
    public void testInactiveMaterializedView() throws Exception {
        starRocksAssert.withTable("create table base_table2\n" +
                "(\n" +
                "    k4 date,\n" +
                "    k5 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k4) BUCKETS 3");
        starRocksAssert.withMaterializedView("create materialized view mv2\n" +
                "distributed by hash(k2) buckets 3\n" +
                "refresh async\n" +
                "as select k1, k2, sum(k3) as total from base_table, base_table2 where k1 = k4 group by k1, k2;");

        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        MaterializedView mv = (MaterializedView) db.getTable("mv2");
        Assert.assertTrue(mv.isCloudNativeMaterializedView());
        Assert.assertTrue(mv.isActive());

        // drop base table and inactive mv
        starRocksAssert.dropTable("base_table2");
        Assert.assertNull(db.getTable("base_table2"));
        Assert.assertFalse(mv.isActive());

        starRocksAssert.dropMaterializedView("mv2");
        Assert.assertNull(db.getTable("mv2"));
    }

    @Test
    public void testAlterAsyncMaterializedViewInterval() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv3\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH async START('2122-12-31 20:45:11') EVERY(INTERVAL 1 DAY)\n" +
                        "as select k1,k2 from base_table;");

        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        MaterializedView mv = (MaterializedView) db.getTable("mv3");
        Assert.assertTrue(mv.isCloudNativeMaterializedView());

        MaterializedView.AsyncRefreshContext asyncRefreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        Assert.assertEquals(4828164311L, asyncRefreshContext.getStartTime());
        Assert.assertEquals(1, asyncRefreshContext.getStep());
        Assert.assertEquals("DAY", asyncRefreshContext.getTimeUnit());

        // alter interval
        String alterMvSql = "ALTER MATERIALIZED VIEW mv3 REFRESH ASYNC EVERY(INTERVAL 2 DAY);";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, alterMvSql);
        stmtExecutor.execute();
        asyncRefreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        Assert.assertEquals(2, asyncRefreshContext.getStep());

        starRocksAssert.dropMaterializedView("mv3");
        Assert.assertNull(db.getTable("mv3"));
    }

    private void waitForSchemaChangeAlterJobFinish() throws Exception {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }

    @Test
    public void testModifyRelatedColumnWithMaterializedView() {
        try {
            starRocksAssert.withTable("create table base_table4\n" +
                    "(\n" +
                    "    k4 date,\n" +
                    "    k5 int\n" +
                    ")\n" +
                    "duplicate key(k4) distributed by hash(k4) buckets 3;");
            starRocksAssert.withMaterializedView("create materialized view mv4\n" +
                    "distributed by hash(k1) buckets 3\n" +
                    "refresh async\n" +
                    "as select k1, k5, sum(k3) as total from base_table, base_table4 where k1 = k4 group by k1, k5;");

            Database db = GlobalStateMgr.getCurrentState().getDb(DB);
            MaterializedView mv = (MaterializedView) db.getTable("mv4");
            Assert.assertTrue(mv.isCloudNativeMaterializedView());
            Assert.assertTrue(mv.isActive());

            // modify column which defined in mv
            String alterSql = "alter table base_table4 modify column k5 varchar(10)";
            AlterTableStmt
                    alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

            waitForSchemaChangeAlterJobFinish();

            // check mv is not active
            Assert.assertFalse(mv.isActive());

            starRocksAssert.dropMaterializedView("mv4");
            Assert.assertNull(db.getTable("mv4"));
            starRocksAssert.dropTable("base_table4");
            Assert.assertNull(db.getTable("base_table4"));
        } catch (Exception e) {
            System.out.println(e);
            Assert.fail();
        }
    }
}
