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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedView.MvRefreshScheme;
import com.starrocks.catalog.MaterializedView.RefreshType;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.RecyclePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Task;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threeten.extra.PeriodDuration;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.MVTestUtils.waitForSchemaChangeAlterJobFinish;

public class LakeMaterializedViewTest {
    private static final String DB = "db_for_lake_mv";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

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
        long physicalPartitionId = 6L;
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
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

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
        mv.setStorageInfo(pathInfo, new DataCacheInfo(true, true));

        // Test selectiveCopy
        MaterializedView newMv = mv.selectiveCopy(Lists.newArrayList("p1"), true, IndexExtState.ALL);
        Assert.assertTrue(newMv.isCloudNativeMaterializedView());
        Assert.assertEquals("s3://test-bucket/1/", newMv.getDefaultFilePathInfo().getFullPath());
        FileCacheInfo cacheInfo = newMv.getPartitionFileCacheInfo(partitionId);
        Assert.assertTrue(cacheInfo.getEnableCache());
        Assert.assertEquals(-1, cacheInfo.getTtlSeconds());
        Assert.assertTrue(cacheInfo.getAsyncWriteBack());

        // Test appendUniqueProperties
        StringBuilder sb = new StringBuilder();
        Deencapsulation.invoke(newMv, "appendUniqueProperties", sb);
        String baseProperties = sb.toString();
        Assert.assertTrue(baseProperties.contains("\"datacache.enable\" = \"true\""));
        Assert.assertTrue(baseProperties.contains("\"enable_async_write_back\" = \"true\""));

        Assert.assertTrue(mv.delete(dbId, false));
    }

    @Test
    public void testCreateMaterializedView() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                    "distributed by hash(k2) buckets 3\n" +
                    "PROPERTIES(\n" +
                    "   'datacache.enable' = 'true',\n" +
                    "   'enable_async_write_back' = 'false',\n" +
                    "   'datacache.partition_duration' = '6 day'\n" +
                    ")\n" +
                    "refresh async\n" +
                    "as select k2, sum(k3) as total from base_table group by k2;");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        MaterializedView mv =
                    (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv1");
        Assert.assertTrue(mv.isCloudNativeMaterializedView());
        Assert.assertTrue(mv.isActive());

        LakeMaterializedView lakeMv = (LakeMaterializedView) mv;
        // same as PseudoStarOSAgent.allocateFilePath
        Assert.assertTrue(lakeMv.getDefaultFilePathInfo().getFullPath().startsWith("s3://dummy_unittest_bucket/dummy_sub_path"));
        // check table default cache info
        FileCacheInfo cacheInfo = lakeMv.getPartitionFileCacheInfo(0L);
        Assert.assertTrue(cacheInfo.getEnableCache());
        Assert.assertEquals(-1, cacheInfo.getTtlSeconds());
        Assert.assertFalse(cacheInfo.getAsyncWriteBack());

        // replication num
        Assert.assertEquals(1L, lakeMv.getDefaultReplicationNum().longValue());

        // show create materialized view
        String ddlStmt = lakeMv.getMaterializedViewDdlStmt(true);
        System.out.println(ddlStmt);
        Assert.assertTrue(ddlStmt.contains("\"replication_num\" = \"1\""));
        Assert.assertTrue(ddlStmt.contains("\"datacache.enable\" = \"true\""));
        Assert.assertTrue(ddlStmt.contains("\"enable_async_write_back\" = \"false\""));
        Assert.assertTrue(ddlStmt.contains("\"storage_volume\" = \"builtin_storage_volume\""));
        Assert.assertTrue(ddlStmt.contains("\"datacache.partition_duration\" = \"6 days\""));

        // check task
        String mvTaskName = "mv-" + mv.getId();
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(mvTaskName);
        Assert.assertNotNull(task);

        starRocksAssert.dropMaterializedView("mv1");
        Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv1"));
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

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        MaterializedView mv =
                    (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv2");
        Assert.assertTrue(mv.isCloudNativeMaterializedView());
        Assert.assertTrue(mv.isActive());

        // drop base table and inactive mv
        starRocksAssert.dropTable("base_table2");
        Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "base_table2"));
        Assert.assertFalse(mv.isActive());

        starRocksAssert.dropMaterializedView("mv2");
        Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv2"));
    }

    @Test
    public void testAlterAsyncMaterializedViewInterval() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv3\n" +
                    "PARTITION BY k1\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                    "REFRESH async START('2122-12-31 20:45:11') EVERY(INTERVAL 1 DAY)\n" +
                    "as select k1,k2 from base_table;");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        MaterializedView mv =
                    (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv3");
        Assert.assertTrue(mv.isCloudNativeMaterializedView());

        MaterializedView.AsyncRefreshContext asyncRefreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        Assert.assertEquals(4828164311L, asyncRefreshContext.getStartTime());
        Assert.assertEquals(1, asyncRefreshContext.getStep());
        Assert.assertEquals("DAY", asyncRefreshContext.getTimeUnit());

        // alter interval
        String alterMvSql = "ALTER MATERIALIZED VIEW mv3 REFRESH ASYNC EVERY(INTERVAL 2 DAY);";
        StatementBase statement = SqlParser.parseSingleStatement(alterMvSql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        asyncRefreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        Assert.assertEquals(2, asyncRefreshContext.getStep());

        starRocksAssert.dropMaterializedView("mv3");
        Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv3"));
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

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
            MaterializedView mv =
                        (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv4");
            Assert.assertTrue(mv.isCloudNativeMaterializedView());
            Assert.assertTrue(mv.isActive());

            // modify column which defined in mv
            String alterSql = "alter table base_table4 modify column k5 varchar(10)";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);

            waitForSchemaChangeAlterJobFinish();

            // check mv is not active
            Assert.assertFalse(mv.isActive());

            starRocksAssert.dropMaterializedView("mv4");
            Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv4"));
            starRocksAssert.dropTable("base_table4");
            Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "base_table4"));
        } catch (Exception e) {
            System.out.println(e);
            Assert.fail();
        }
    }

    @Test
    public void testNonPartitionMvEnableFillDataCache() {
        try {
            starRocksAssert.withTable("create table base_table5\n" +
                        "(\n" +
                        "    k4 date,\n" +
                        "    k5 int\n" +
                        ")\n" +
                        "duplicate key(k4) distributed by hash(k4) buckets 3;");
            starRocksAssert.withMaterializedView("create materialized view mv5\n" +
                        "distributed by hash(k1) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k5, sum(k3) as total from base_table, base_table5 where k1 = k4 group by k1, k5;");

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
            MaterializedView mv =
                        (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv5");
            Assert.assertTrue(mv.isCloudNativeMaterializedView());

            Partition p = mv.getPartition("mv5");
            Assert.assertTrue(mv.isEnableFillDataCache(p));

            starRocksAssert.dropMaterializedView("mv5");
            Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv5"));
            starRocksAssert.dropTable("base_table5");
            Assert.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "base_table5"));
        } catch (Exception e) {
            System.out.println(e);
            Assert.fail();
        }
    }

    @Test
    public void testPartitionMvEnableFillDataCache() throws AnalysisException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.META_VERSION;
            }
        };

        long dbId = 1L;
        long mvId = 2L;
        long indexId = 3L;
        long partition1Id = 20L;
        long partition2Id = 21L;
        long physicalPartitionId1 = 22L;
        long physicalPartitionId2 = 23L;

        long tablet1Id = 10L;
        long tablet2Id = 11L;

        // schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", Type.DATE, true, null, "", "");
        columns.add(k1);
        Column k2 = new Column("k2", Type.BIGINT, true, null, "", "");
        columns.add(k2);
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k2));
        RangePartitionInfo partitionInfo = new RangePartitionInfo(Lists.newArrayList(k1));

        String durationStr = "7 DAY";
        PeriodDuration duration = TimeUtils.parseHumanReadablePeriodOrDuration(durationStr);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, durationStr);
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildDataCachePartitionDuration();

        // partition1
        MaterializedIndex index1 = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta1 = new TabletMeta(dbId, mvId, partition1Id, indexId, 0, TStorageMedium.HDD, true);
        Tablet tablet1 = new LakeTablet(tablet1Id);
        index1.addTablet(tablet1, tabletMeta1);
        Partition partition1 = new Partition(partition1Id, physicalPartitionId1, "p1", index1, distributionInfo);

        LocalDate upper1 = LocalDate.now().minus(duration);
        LocalDate lower1 = upper1.minus(duration);
        Range<PartitionKey> range1 = Range.closedOpen(PartitionKey.ofDate(lower1), PartitionKey.ofDate(upper1));
        partitionInfo.addPartition(partition1Id, false, range1, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, false,
                    new DataCacheInfo(true, false));

        // partition2
        MaterializedIndex index2 = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta2 = new TabletMeta(dbId, mvId, partition2Id, indexId, 0, TStorageMedium.HDD, true);
        Tablet tablet2 = new LakeTablet(tablet2Id);
        index2.addTablet(tablet2, tabletMeta2);
        Partition partition2 = new Partition(partition2Id, physicalPartitionId2, "p2", index1, distributionInfo);

        LocalDate upper2 = LocalDate.now();
        LocalDate lower2 = upper2.minus(duration);
        Range<PartitionKey> range2 = Range.closedOpen(PartitionKey.ofDate(lower2), PartitionKey.ofDate(upper2));
        partitionInfo.addPartition(partition2Id, false, range2, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, false,
                    new DataCacheInfo(true, false));

        // refresh scheme
        MvRefreshScheme mvRefreshScheme = new MvRefreshScheme();
        mvRefreshScheme.setType(RefreshType.SYNC);

        // Lake mv
        LakeMaterializedView mv = new LakeMaterializedView(mvId, dbId, "mv1", columns, KeysType.AGG_KEYS,
                    partitionInfo, distributionInfo, mvRefreshScheme);
        Deencapsulation.setField(mv, "baseIndexId", indexId);
        mv.addPartition(partition1);
        mv.addPartition(partition2);
        mv.setIndexMeta(indexId, "mv1", columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);
        mv.setTableProperty(tableProperty);

        // Test
        Assert.assertFalse(mv.isEnableFillDataCache(partition1));
        Assert.assertTrue(mv.isEnableFillDataCache(partition2));
    }

    @Test
    public void testBuildRecyclePartitionInfo() {
        long dbId = 1L;
        long mvId = 2L;
        long partitionId = 3L;
        Partition partition = new Partition(partitionId, null, null);

        // range partition
        PartitionInfo rangePartitionInfo = new RangePartitionInfo(Lists.newArrayList());
        rangePartitionInfo.setIsInMemory(partitionId, true);
        LakeMaterializedView mv1 =
                new LakeMaterializedView(mvId, dbId, "mv1", null, null, rangePartitionInfo, null, null);

        RecyclePartitionInfo recyclePartitionInfo = mv1.buildRecyclePartitionInfo(dbId, partition);
        Assert.assertTrue(recyclePartitionInfo instanceof RecycleLakeRangePartitionInfo);

        // un-partitioned
        PartitionInfo singlePartitionInfo = new PartitionInfo(PartitionType.UNPARTITIONED);
        singlePartitionInfo.setIsInMemory(partitionId, true);
        LakeMaterializedView mv2 =
                new LakeMaterializedView(mvId, dbId, "mv1", null, null, singlePartitionInfo, null, null);
        recyclePartitionInfo = mv2.buildRecyclePartitionInfo(dbId, partition);
        Assert.assertTrue(recyclePartitionInfo instanceof RecycleLakeUnPartitionInfo);

        // list partition
        PartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, Lists.newArrayList());
        listPartitionInfo.setIsInMemory(partitionId, true);
        LakeMaterializedView mv3 =
                new LakeMaterializedView(mvId, dbId, "mv1", null, null, listPartitionInfo, null, null);
        recyclePartitionInfo = mv3.buildRecyclePartitionInfo(dbId, partition);
        Assert.assertTrue(recyclePartitionInfo instanceof RecycleLakeListPartitionInfo);
    }
}
