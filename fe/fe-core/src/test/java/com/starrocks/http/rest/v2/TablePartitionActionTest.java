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

package com.starrocks.http.rest.v2;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.http.rest.v2.RestBaseResultV2.PagedResult;
import com.starrocks.http.rest.v2.vo.PartitionInfoView.PartitionView;
import com.starrocks.http.rest.v2.vo.TabletView;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.client.utils.URIBuilder;
import org.assertj.core.util.Lists;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.JVM)
public class TablePartitionActionTest extends StarRocksHttpTestCase {

    private static final String TABLE_PARTITION_URL_PATTERN =
            BASE_URL + "/api/v2/catalogs/%s/databases/%s/tables/%s/partition";

    private static final String PAGE_NUM_KEY = "page_num";
    private static final String PAGE_SIZE_KEY = "page_size";

    private static final Long TB_OLAP_TABLE_ID = testTableId + 11000L;
    private static final String TB_OLAP_TABLE_NAME = "tb_olap_table_test";

    private static final Long TB_LAKE_TABLE_ID = testTableId + 12000L;
    private static final String TB_LAKE_TABLE_NAME = "tb_lake_table_test";

    private static final Long BASE_PARTITION_ID = testPartitionId + 11000L;

    private static final int PARTITION_SIZE = 7;

    @Test
    public void testNonOlapTable() throws Exception {
        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, ES_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("403", resp.getCode());
            assertTrue(resp.getMessage().contains("is not a OLAP table"));
        }
    }

    @Test
    public void testOlapTable() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newOlapTable(
                TB_OLAP_TABLE_ID, TB_OLAP_TABLE_NAME, PARTITION_SIZE));

        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_OLAP_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            PagedResult<PartitionView> tablePartition = resp.getResult();

            List<PartitionView> partitionItems = tablePartition.getItems();
            assertEquals(PARTITION_SIZE, partitionItems.size());

            {
                PartitionView partition = partitionItems.get(0);
                assertEquals(BASE_PARTITION_ID, partition.getId());
                assertEquals("testPartition_0", partition.getName());
                assertEquals(8, partition.getBucketNum().intValue());
                assertEquals("HASH", partition.getDistributionType());
                assertEquals(testStartVersion, partition.getVisibleVersion().longValue());
                assertTrue(partition.getVisibleVersionTime() > 0L);
                assertEquals(testStartVersion + 1, partition.getNextVersion().longValue());
                // assertFalse(partition.getMinPartition());
                // assertFalse(partition.getMaxPartition());
                assertArrayEquals(new Object[] {0.0D}, partition.getStartKeys().toArray(new Object[0]));
                assertArrayEquals(new Object[] {10.0D}, partition.getEndKeys().toArray(new Object[0]));
                assertNull(partition.getStoragePath());

                List<TabletView> tablets = partition.getTablets();
                assertEquals(1, tablets.size());
                {
                    TabletView tablet = tablets.get(0);
                    assertEquals(tabletId, tablet.getId().longValue());
                    assertNull(tablet.getPrimaryComputeNodeId());
                    assertArrayEquals(
                            new Long[] {testBackendId1, testBackendId2, testBackendId3},
                            tablet.getBackendIds().toArray(new Long[0]));
                }
            }

        } finally {
            db.dropTable(TB_OLAP_TABLE_ID);
        }
    }

    @Test
    public void testLakeTable() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newLakeTable(
                TB_LAKE_TABLE_ID, TB_LAKE_TABLE_NAME, PARTITION_SIZE));

        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_LAKE_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            PagedResult<PartitionView> tablePartition = resp.getResult();

            List<PartitionView> partitionItems = tablePartition.getItems();
            assertEquals(PARTITION_SIZE, partitionItems.size());

            {
                PartitionView partition = partitionItems.get(0);
                assertEquals(BASE_PARTITION_ID, partition.getId());
                assertEquals("testPartition_0", partition.getName());
                assertEquals(8, partition.getBucketNum().intValue());
                assertEquals("HASH", partition.getDistributionType());
                assertEquals(testStartVersion, partition.getVisibleVersion().longValue());
                assertTrue(partition.getVisibleVersionTime() > 0L);
                assertEquals(testStartVersion + 1, partition.getNextVersion().longValue());
                // assertFalse(partition.getMinPartition());
                // assertFalse(partition.getMaxPartition());
                assertArrayEquals(new Object[] {0.0D}, partition.getStartKeys().toArray(new Object[0]));
                assertArrayEquals(new Object[] {10.0D}, partition.getEndKeys().toArray(new Object[0]));
                assertEquals("s3://test-bucket/" + TB_LAKE_TABLE_NAME, partition.getStoragePath());

                List<TabletView> tablets = partition.getTablets();
                assertEquals(1, tablets.size());
                {
                    TabletView tablet = tablets.get(0);
                    assertEquals(tabletId, tablet.getId().longValue());
                    assertEquals(testBackendId1, tablet.getPrimaryComputeNodeId().longValue());
                    assertArrayEquals(
                            new Long[] {testBackendId1},
                            tablet.getBackendIds().toArray(new Long[0]));
                }
            }

        } finally {
            db.dropTable(TB_LAKE_TABLE_ID);
        }
    }

    @Test
    public void testPages() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newOlapTable(
                TB_OLAP_TABLE_ID, TB_OLAP_TABLE_NAME, PARTITION_SIZE));

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_OLAP_TABLE_NAME);
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(0, tablePartition.getPageNum().intValue());
                assertEquals(100, tablePartition.getPageSize().intValue());
                assertEquals(1, tablePartition.getPages().intValue());
                assertEquals(7, tablePartition.getTotal().intValue());
                assertEquals(7, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_OLAP_TABLE_NAME,
                    uriBuilder -> {
                        uriBuilder.addParameter(PAGE_NUM_KEY, "-1");
                        uriBuilder.addParameter(PAGE_SIZE_KEY, "-1");
                    });
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(0, tablePartition.getPageNum().intValue());
                assertEquals(100, tablePartition.getPageSize().intValue());
                assertEquals(1, tablePartition.getPages().intValue());
                assertEquals(7, tablePartition.getTotal().intValue());
                assertEquals(7, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_OLAP_TABLE_NAME,
                    uriBuilder -> {
                        uriBuilder.addParameter(PAGE_NUM_KEY, "0");
                        uriBuilder.addParameter(PAGE_SIZE_KEY, "1");
                    });
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(0, tablePartition.getPageNum().intValue());
                assertEquals(1, tablePartition.getPageSize().intValue());
                assertEquals(7, tablePartition.getPages().intValue());
                assertEquals(7, tablePartition.getTotal().intValue());
                assertEquals(1, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_OLAP_TABLE_NAME,
                    uriBuilder -> {
                        uriBuilder.addParameter(PAGE_NUM_KEY, "0");
                        uriBuilder.addParameter(PAGE_SIZE_KEY, "7");
                    });
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(0, tablePartition.getPageNum().intValue());
                assertEquals(7, tablePartition.getPageSize().intValue());
                assertEquals(1, tablePartition.getPages().intValue());
                assertEquals(7, tablePartition.getTotal().intValue());
                assertEquals(7, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_OLAP_TABLE_NAME,
                    uriBuilder -> {
                        uriBuilder.addParameter(PAGE_NUM_KEY, "1");
                        uriBuilder.addParameter(PAGE_SIZE_KEY, "3");
                    });
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(1, tablePartition.getPageNum().intValue());
                assertEquals(3, tablePartition.getPageSize().intValue());
                assertEquals(3, tablePartition.getPages().intValue());
                assertEquals(7, tablePartition.getTotal().intValue());
                assertEquals(3, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_OLAP_TABLE_NAME,
                    uriBuilder -> {
                        uriBuilder.addParameter(PAGE_NUM_KEY, "1");
                        uriBuilder.addParameter(PAGE_SIZE_KEY, "7");
                    });
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(1, tablePartition.getPageNum().intValue());
                assertEquals(7, tablePartition.getPageSize().intValue());
                assertEquals(1, tablePartition.getPages().intValue());
                assertEquals(7, tablePartition.getTotal().intValue());
                assertEquals(0, tablePartition.getItems().size());
            }
        }

        db.dropTable(TB_OLAP_TABLE_ID);
    }

    private static OlapTable newOlapTable(Long tableId, String tableName, int partitionSize) throws Exception {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Column c1 = new Column("c1", Type.DOUBLE, true, null, null, false, null, "cc1", 1);
        Column c2 = new Column("c2", Type.DEFAULT_DECIMAL64, false, AggregateType.SUM, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "cc2", 2);
        List<Column> columns = Lists.newArrayList(c1, c2);

        RangePartitionInfo partitionInfo = new RangePartitionInfo(
                Lists.newArrayList(c1)
        );

        DistributionInfo defaultDistributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c1));

        Index idx1 = new Index(
                testIndexId, "idx1", Lists.newArrayList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "c_idx1", new HashMap<>());
        Index idx2 = new Index(
                testIndexId + 1, "idx2", Lists.newArrayList(ColumnId.create("c2")),
                IndexDef.IndexType.NGRAMBF, "c_idx2", new HashMap<>());
        TableIndexes indexes = new TableIndexes(
                Lists.newArrayList(idx1, idx2)
        );
        OlapTable olapTable = new OlapTable(
                tableId,
                tableName,
                columns,
                KeysType.DUP_KEYS,
                partitionInfo,
                defaultDistributionInfo,
                indexes,
                Table.TableType.OLAP
        );

        // tablet
        LocalTablet tablet = new LocalTablet(tabletId);

        // index
        MaterializedIndex baseIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(
                testDbId, TB_OLAP_TABLE_ID, BASE_PARTITION_ID, testIndexId, testSchemaHash, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        tablet.addReplica(new Replica(
                testReplicaId1, testBackendId1, testStartVersion, testSchemaHash,
                1024000L, 2000L, Replica.ReplicaState.NORMAL, -1, 0));
        tablet.addReplica(new Replica(
                testReplicaId2, testBackendId2, testStartVersion, testSchemaHash,
                1024000L, 2000L, Replica.ReplicaState.NORMAL, -1, 0));
        tablet.addReplica(new Replica(
                testReplicaId3, testBackendId3, testStartVersion, testSchemaHash,
                1024000L, 2000L, Replica.ReplicaState.NORMAL, -1, 0));

        for (int i = 0; i < partitionSize; i++) {
            DistributionInfo distributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c1));

            long partitionId = BASE_PARTITION_ID + i;
            Partition partition = new Partition(partitionId, "testPartition_" + i, baseIndex, distributionInfo);
            partition.setVisibleVersion(testStartVersion, System.currentTimeMillis());
            partition.setNextVersion(testStartVersion + 1);

            PartitionKey rangeLower = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue(String.valueOf(i * 10))), Lists.newArrayList(c1));
            PartitionKey rangeUpper = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue(String.valueOf((i + 1) * 10))), Lists.newArrayList(c1));

            partitionInfo.setRange(partitionId, false, Range.closedOpen(rangeLower, rangeUpper));

            olapTable.addPartition(partition);
        }

        return olapTable;
    }

    private static LakeTable newLakeTable(Long tableId, String tableName, int partitionSize) throws Exception {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Column c1 = new Column("c1", Type.DOUBLE, true, null, null, false, null, "cc1", 1);
        Column c2 = new Column("c2", Type.DEFAULT_DECIMAL64, false, AggregateType.SUM, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "cc2", 2);
        List<Column> columns = Lists.newArrayList(c1, c2);

        RangePartitionInfo partitionInfo = new RangePartitionInfo(
                Lists.newArrayList(c1)
        );

        DistributionInfo defaultDistributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c1));

        LakeTable lakeTable = new LakeTable(
                tableId,
                tableName,
                columns,
                KeysType.DUP_KEYS,
                partitionInfo,
                defaultDistributionInfo
        );

        // tablet
        LakeTablet tablet = new LakeTablet(tabletId);

        // index
        MaterializedIndex baseIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(
                testDbId, TB_LAKE_TABLE_ID, BASE_PARTITION_ID, testIndexId, testSchemaHash, TStorageMedium.HDD, true);
        baseIndex.addTablet(tablet, tabletMeta);

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
        builder.setFullPath("s3://test-bucket/" + tableName);
        FilePathInfo pathInfo = builder.build();
        lakeTable.setStorageInfo(pathInfo, new DataCacheInfo(false, false));

        new Expectations(tablet) {
            {
                GlobalStateMgr.getCurrentState().getStarOSAgent().getShardInfo(anyLong, anyLong);
                minTimes = 0;
                result = ShardInfo.newBuilder()
                        .setFilePath(pathInfo)
                        .build();

                tablet.getBackendIds();
                minTimes = 0;
                result = Sets.newHashSet(testBackendId1);

                GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getComputeNodeId(anyLong, (LakeTablet) any);
                minTimes = 0;
                result = testBackendId1;
            }
        };

        for (int i = 0; i < partitionSize; i++) {
            DistributionInfo distributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c1));

            long partitionId = BASE_PARTITION_ID + i;
            Partition partition = new Partition(partitionId, "testPartition_" + i, baseIndex, distributionInfo);
            partition.setVisibleVersion(testStartVersion, System.currentTimeMillis());
            partition.setNextVersion(testStartVersion + 1);

            PartitionKey rangeLower = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue(String.valueOf(i * 10))), Lists.newArrayList(c1));
            PartitionKey rangeUpper = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue(String.valueOf((i + 1) * 10))), Lists.newArrayList(c1));

            partitionInfo.setRange(partitionId, false, Range.closedOpen(rangeLower, rangeUpper));

            lakeTable.addPartition(partition);
        }

        return lakeTable;
    }

    private static RestBaseResultV2<PagedResult<PartitionView>> parseResponseBody(String body) {
        try {
            return GsonUtils.GSON.fromJson(
                    body,
                    new TypeToken<RestBaseResultV2<PagedResult<PartitionView>>>() {
                    }.getType());
        } catch (Exception e) {
            fail(e.getMessage() + ", resp: " + body);
            throw new IllegalStateException(e);
        }
    }

    private Request newRequest(String catalog, String database, String table) throws Exception {
        return newRequest(catalog, database, table, null);
    }

    private Request newRequest(String catalog,
                               String database,
                               String table,
                               Consumer<URIBuilder> consumer) throws Exception {
        URIBuilder uriBuilder = new URIBuilder(toPartitionUrl(catalog, database, table));
        if (null != consumer) {
            consumer.accept(uriBuilder);
        }
        return new Request.Builder()
                .get()
                .addHeader(AUTH_KEY, rootAuth)
                .url(uriBuilder.build().toURL())
                .build();
    }

    private static String toPartitionUrl(String catalog, String database, String table) {
        return String.format(TABLE_PARTITION_URL_PATTERN, catalog, database, table);
    }

}