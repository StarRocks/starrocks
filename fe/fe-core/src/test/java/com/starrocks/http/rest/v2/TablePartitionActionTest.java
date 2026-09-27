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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
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
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.client.utils.URIBuilder;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodName.class)
public class TablePartitionActionTest extends StarRocksHttpTestCase {

    private static final String TABLE_PARTITION_URL_PATTERN =
            BASE_URL + "/api/v2/catalogs/%s/databases/%s/tables/%s/partition";

    private static final String PAGE_NUM_KEY = "page_num";
    private static final String PAGE_SIZE_KEY = "page_size";

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final Long UNPARTITIONED_TABLE_ID = testTableId + 11000L;
    private static final String UNPARTITIONED_TABLE_NAME = "tb_unpartitioned";

    private static final Long RANGE_PARTITION_TABLE_ID = testTableId + 12000L;
    private static final String RANGE_PARTITION_TABLE_NAME = "tb_range_partition";

    private static final Long LIST_PARTITION_TABLE_ID = testTableId + 13000L;
    private static final String LIST_PARTITION_TABLE_NAME = "tb_list_partition";

    private static final Long RANGE_DISTRIBUTION_TABLE_ID = testTableId + 14000L;
    private static final String RANGE_DISTRIBUTION_TABLE_NAME = "tb_range_distribution";

    private static final Long HASH_ROLLUP_TABLE_ID = testTableId + 15000L;
    private static final String HASH_ROLLUP_TABLE_NAME = "tb_hash_rollup";

    private static final Long BASE_PARTITION_ID = testPartitionId + 11000L;


    @Test
    public void testNonOlapTable() throws Exception {
        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, ES_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("403", resp.getCode());
            assertTrue(resp.getMessage().contains("is not a OLAP table"));
        }
    }

    private static OlapTable newUnpartitionedOlapTable(Long tableId, String tableName) {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Column c0 = new Column("c0", IntegerType.BIGINT, true, null, null, false, null, "cc0", 1);
        Column c1 = new Column("c1", DateType.DATETIME, true, null, null, false, null, "cc1", 2);
        Column c2 = new Column("c2", VarcharType.VARCHAR, true, null, null, false, null, "cc2", 3);
        Column c3 = new Column("c3",
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 8),
                false, null, null, true, new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "cc3", 4
        );
        List<Column> columns = Lists.newArrayList(c0, c1, c2, c3);

        return new OlapTable(
                tableId,
                tableName,
                columns,
                KeysType.DUP_KEYS,
                new SinglePartitionInfo(),
                new HashDistributionInfo(8, Lists.newArrayList(c0)),
                new TableIndexes(),
                Table.TableType.OLAP
        );

    }

    @Test
    public void testUnPartitionedOlapTable() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newUnpartitionedOlapTable(UNPARTITIONED_TABLE_ID, UNPARTITIONED_TABLE_NAME));

        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, UNPARTITIONED_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            PagedResult<PartitionView> tablePartition = resp.getResult();
            List<PartitionView> partitionItems = tablePartition.getItems();
            assertEquals(0, partitionItems.size());
        } finally {
            db.dropTable(UNPARTITIONED_TABLE_ID);
        }
    }

    private static LakeTable newRangePartitionLakeTable(Long tableId, String tableName, int partitionSize) throws Exception {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Column c0 = new Column("c0", IntegerType.BIGINT, true, null, null, false, null, "cc0", 1);
        Column c1 = new Column("c1", DateType.DATETIME, true, null, null, false, null, "cc1", 2);
        Column c2 = new Column("c2", VarcharType.VARCHAR, true, null, null, false, null, "cc2", 3);
        Column c3 = new Column("c3",
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 8),
                false, null, null, true, new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "cc3", 4
        );
        List<Column> columns = Lists.newArrayList(c0, c1, c2, c3);

        RangePartitionInfo partitionInfo = new RangePartitionInfo(Lists.newArrayList(c1));
        LakeTable lakeTable = new LakeTable(
                tableId,
                tableName,
                columns,
                KeysType.DUP_KEYS,
                partitionInfo,
                new HashDistributionInfo(8, Lists.newArrayList(c1))
        );

        // tablet
        LakeTablet tablet = new LakeTablet(tabletId);

        // index
        MaterializedIndex baseIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(
                testDbId, RANGE_PARTITION_TABLE_ID, BASE_PARTITION_ID, testIndexId, TStorageMedium.HDD, true);
        baseIndex.addTablet(tablet, tabletMeta);

        FilePathInfo pathInfo = newTestPathInfo(tableName);
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
                        .getComputeNodeId((ComputeResource) any, anyLong);

                minTimes = 0;
                result = testBackendId1;

                GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getAliveComputeNodeId((ComputeResource) any, anyLong);

                minTimes = 0;
                result = testBackendId1;
            }
        };

        LocalDate today = LocalDate.now();
        for (int i = 0; i < partitionSize; i++) {
            DistributionInfo distributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c1));

            long partitionId = BASE_PARTITION_ID + i;
            long physicalPartitionId = partitionId + partitionSize;
            Partition partition = new Partition(partitionId, physicalPartitionId,
                    "range_partition_" + i, baseIndex, distributionInfo);
            partition.getDefaultPhysicalPartition().setVisibleVersion(testStartVersion, System.currentTimeMillis());
            partition.getDefaultPhysicalPartition().setNextVersion(testStartVersion + 1);

            PartitionKey rangeLower = PartitionKey.createPartitionKey(
                    Lists.newArrayList(PartitionValue.ofDate(today.minusDays(partitionSize - i))),
                    Lists.newArrayList(c1)
            );
            PartitionKey rangeUpper = PartitionKey.createPartitionKey(
                    Lists.newArrayList(PartitionValue.ofDate(today.minusDays(partitionSize - i - 1))),
                    Lists.newArrayList(c1)
            );

            partitionInfo.setRange(partitionId, false, Range.closedOpen(rangeLower, rangeUpper));

            lakeTable.addPartition(partition);
        }

        return lakeTable;
    }

    @Test
    public void testRangePartitionOlapTable() throws Exception {
        int partitionSize = 10;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newRangePartitionLakeTable(
                RANGE_PARTITION_TABLE_ID, RANGE_PARTITION_TABLE_NAME, partitionSize));

        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_PARTITION_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            PagedResult<PartitionView> tablePartition = resp.getResult();

            List<PartitionView> partitionItems = tablePartition.getItems();
            assertEquals(partitionSize, partitionItems.size());

            LocalDate today = LocalDate.now();
            for (int i = 0; i < partitionSize; i++) {
                {
                    PartitionView partition = partitionItems.get(i);
                    assertEquals(Long.valueOf(BASE_PARTITION_ID + i), partition.getId());
                    assertEquals("range_partition_" + i, partition.getName());
                    assertEquals(8, partition.getBucketNum().intValue());
                    assertEquals("HASH", partition.getDistributionType());
                    assertEquals(testStartVersion, partition.getVisibleVersion().longValue());
                    assertTrue(partition.getVisibleVersionTime() > 0L);
                    assertEquals(testStartVersion + 1, partition.getNextVersion().longValue());
                    // assertFalse(partition.getMinPartition());
                    // assertFalse(partition.getMaxPartition());
                    assertEquals(1, partition.getStartKeys().size());
                    long starKey = ((Double) partition.getStartKeys().get(0)).longValue();
                    assertEquals(Long.parseLong(today.minusDays(partitionSize - i).format(DTF) + "000000"), starKey);

                    assertEquals(1, partition.getEndKeys().size());
                    long endKey = ((Double) partition.getEndKeys().get(0)).longValue();
                    assertEquals(Long.parseLong(today.minusDays(partitionSize - i - 1).format(DTF) + "000000"), endKey);

                    assertNull(partition.getInKeys());

                    assertEquals("s3://test-bucket/" + RANGE_PARTITION_TABLE_NAME, partition.getStoragePath());

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
            }
        } finally {
            db.dropTable(RANGE_PARTITION_TABLE_ID);
        }
    }

    private static OlapTable newListPartitionOlapTable(Long tableId,
                                                       String tableName,
                                                       int partitionSize) throws Exception {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Map<ColumnId, Column> idToColumn = new HashMap<>();
        Column c0 = new Column("c0", IntegerType.BIGINT, true, null, null, false, null, "cc0", 1);
        Column c1 = new Column("c1", DateType.DATETIME, true, null, null, false, null, "cc1", 2);
        Column c2 = new Column("c2", VarcharType.VARCHAR, true, null, null, false, null, "cc2", 3);
        Column c3 = new Column("c3",
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 8),
                false, null, null, true, new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "cc3", 4
        );
        List<Column> columns = Lists.newArrayList(c0, c1, c2, c3);
        for (Column column : columns) {
            idToColumn.put(column.getColumnId(), column);
        }

        ListPartitionInfo partitionInfo = new ListPartitionInfo(PartitionType.LIST, Lists.newArrayList(c2));
        OlapTable olapTable = new OlapTable(
                tableId,
                tableName,
                columns,
                KeysType.DUP_KEYS,
                partitionInfo,
                new HashDistributionInfo(8, Lists.newArrayList(c0)),
                new TableIndexes(),
                Table.TableType.OLAP
        );

        // tablet
        LocalTablet tablet = new LocalTablet(tabletId);

        // index
        MaterializedIndex baseIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(
                testDbId, LIST_PARTITION_TABLE_ID, BASE_PARTITION_ID, testIndexId, TStorageMedium.HDD);
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
            DistributionInfo distributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c0));

            long partitionId = BASE_PARTITION_ID + i;
            long physicalPartitionId = partitionId + partitionSize;
            Partition partition = new Partition(partitionId, physicalPartitionId,
                    "list_partition_" + i, baseIndex, distributionInfo);
            partition.getDefaultPhysicalPartition().setVisibleVersion(testStartVersion, System.currentTimeMillis());
            partition.getDefaultPhysicalPartition().setNextVersion(testStartVersion + 1);

            partitionInfo.addPartition(
                    idToColumn,
                    partitionId,
                    new DataProperty(TStorageMedium.SSD),
                    (short) 1,
                    null,
                    new ArrayList<>(),
                    Collections.singletonList(Lists.newArrayList("list_partition_" + i)));

            olapTable.addPartition(partition);
        }

        return olapTable;
    }

    @Test
    public void testListPartitionOlapTable() throws Exception {
        int partitionSize = 10;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newListPartitionOlapTable(
                LIST_PARTITION_TABLE_ID, LIST_PARTITION_TABLE_NAME, partitionSize));

        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, LIST_PARTITION_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            PagedResult<PartitionView> tablePartition = resp.getResult();

            List<PartitionView> partitionItems = tablePartition.getItems();
            assertEquals(partitionSize, partitionItems.size());

            for (int i = 0; i < partitionSize; i++) {
                PartitionView partition = partitionItems.get(i);
                assertEquals(Long.valueOf(BASE_PARTITION_ID + i), partition.getId());
                assertEquals("list_partition_" + i, partition.getName());
                assertEquals(8, partition.getBucketNum().intValue());
                assertEquals("HASH", partition.getDistributionType());
                assertEquals(testStartVersion, partition.getVisibleVersion().longValue());
                assertTrue(partition.getVisibleVersionTime() > 0L);
                assertEquals(testStartVersion + 1, partition.getNextVersion().longValue());
                // assertFalse(partition.getMinPartition());
                // assertFalse(partition.getMaxPartition());
                assertNull(partition.getStartKeys());
                assertNull(partition.getEndKeys());
                assertEquals(1, partition.getInKeys().size());
                assertEquals("list_partition_" + i, partition.getInKeys().get(0).get(0).toString());
                assertNull(partition.getStoragePath());

                List<TabletView> tablets = partition.getTablets();
                assertEquals(1, tablets.size());
                {
                    TabletView tablet = tablets.get(0);
                    assertEquals(tabletId, tablet.getId().longValue());
                    assertArrayEquals(
                            new Long[] {testBackendId1, testBackendId2, testBackendId3},
                            tablet.getBackendIds().toArray(new Long[0])
                    );
                }
            }

        } finally {
            db.dropTable(LIST_PARTITION_TABLE_NAME);
        }
    }

    @Test
    public void testPages() throws Exception {
        int partitionSize = 7;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newRangePartitionLakeTable(
                RANGE_PARTITION_TABLE_ID, RANGE_PARTITION_TABLE_NAME, partitionSize));

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_PARTITION_TABLE_NAME);
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(0, tablePartition.getPageNum().intValue());
                assertEquals(100, tablePartition.getPageSize().intValue());
                assertEquals(1, tablePartition.getPages().intValue());
                assertEquals(partitionSize, tablePartition.getTotal().intValue());
                assertEquals(partitionSize, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_PARTITION_TABLE_NAME,
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
                assertEquals(partitionSize, tablePartition.getTotal().intValue());
                assertEquals(partitionSize, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_PARTITION_TABLE_NAME,
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
                assertEquals(partitionSize, tablePartition.getPages().intValue());
                assertEquals(partitionSize, tablePartition.getTotal().intValue());
                assertEquals(1, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_PARTITION_TABLE_NAME,
                    uriBuilder -> {
                        uriBuilder.addParameter(PAGE_NUM_KEY, "0");
                        uriBuilder.addParameter(PAGE_SIZE_KEY, Objects.toString(partitionSize));
                    });
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(0, tablePartition.getPageNum().intValue());
                assertEquals(partitionSize, tablePartition.getPageSize().intValue());
                assertEquals(1, tablePartition.getPages().intValue());
                assertEquals(partitionSize, tablePartition.getTotal().intValue());
                assertEquals(partitionSize, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_PARTITION_TABLE_NAME,
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
                assertEquals(partitionSize, tablePartition.getTotal().intValue());
                assertEquals(3, tablePartition.getItems().size());
            }
        }

        {
            Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_PARTITION_TABLE_NAME,
                    uriBuilder -> {
                        uriBuilder.addParameter(PAGE_NUM_KEY, "1");
                        uriBuilder.addParameter(PAGE_SIZE_KEY, Objects.toString(partitionSize));
                    });
            try (Response response = networkClient.newCall(request).execute()) {
                RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
                assertEquals("0", resp.getCode());
                PagedResult<PartitionView> tablePartition = resp.getResult();
                assertEquals(1, tablePartition.getPageNum().intValue());
                assertEquals(partitionSize, tablePartition.getPageSize().intValue());
                assertEquals(1, tablePartition.getPages().intValue());
                assertEquals(partitionSize, tablePartition.getTotal().intValue());
                assertEquals(0, tablePartition.getItems().size());
            }
        }

        db.dropTable(UNPARTITIONED_TABLE_ID);
    }

    private static MaterializedIndex newIndexWithTablets(long indexId, long metaId, int tabletNum,
                                                         long tableId, long physicalPartitionId) {
        MaterializedIndex index = new MaterializedIndex(indexId, metaId, MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        TabletMeta tabletMeta = new TabletMeta(
                testDbId, tableId, physicalPartitionId, indexId, TStorageMedium.HDD, true);
        for (int i = 0; i < tabletNum; i++) {
            index.addTablet(new LakeTablet(indexId + i), tabletMeta);
        }
        return index;
    }

    /**
     * A range-distribution lake table whose base index holds 3 tablets and whose two rollups hold 2
     * and 4, so the base-index reading (3) is distinguishable from the old fixed 1, the minimum (2)
     * and the maximum (4).
     */
    private static LakeTable newRangeDistributionLakeTable() throws Exception {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Column c0 = new Column("c0", IntegerType.BIGINT, true, null, null, false, null, "cc0", 1);
        List<Column> columns = Lists.newArrayList(c0);

        long partitionId = BASE_PARTITION_ID + 500L;
        long physicalPartitionId = partitionId + 1L;

        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        LakeTable lakeTable = new LakeTable(RANGE_DISTRIBUTION_TABLE_ID, RANGE_DISTRIBUTION_TABLE_NAME,
                columns, KeysType.DUP_KEYS, partitionInfo, new RangeDistributionInfo());

        MaterializedIndex baseIndex = newIndexWithTablets(30000L, 3000L, 3,
                RANGE_DISTRIBUTION_TABLE_ID, physicalPartitionId);
        lakeTable.setBaseIndexMetaId(baseIndex.getMetaId());
        lakeTable.setIndexMeta(baseIndex.getMetaId(), RANGE_DISTRIBUTION_TABLE_NAME, columns, 0, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);

        FilePathInfo pathInfo = newTestPathInfo(RANGE_DISTRIBUTION_TABLE_NAME);
        lakeTable.setStorageInfo(pathInfo, new DataCacheInfo(false, false));
        mockLakeLookups(pathInfo);

        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", baseIndex,
                new RangeDistributionInfo());
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        physicalPartition.createRollupIndex(newIndexWithTablets(10000L, 1000L, 2,
                RANGE_DISTRIBUTION_TABLE_ID, physicalPartitionId));
        physicalPartition.createRollupIndex(newIndexWithTablets(20000L, 2000L, 4,
                RANGE_DISTRIBUTION_TABLE_ID, physicalPartitionId));
        partition.getDefaultPhysicalPartition().setVisibleVersion(testStartVersion, System.currentTimeMillis());
        partition.getDefaultPhysicalPartition().setNextVersion(testStartVersion + 1);
        lakeTable.addPartition(partition);

        return lakeTable;
    }

    @Test
    public void testRangeDistributionPartitionReportsBaseIndexTablets() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        LakeTable table = newRangeDistributionLakeTable();
        db.registerTableUnlocked(table);

        PhysicalPartition physicalPartition = table.getPartitions().iterator().next()
                .getDefaultPhysicalPartition();
        // Precondition: a wrong implementation taking element 0 of the generic enumeration must not
        // coincidentally land on the base index.
        assertNotEquals(30000L,
                physicalPartition.getLatestMaterializedIndices(IndexExtState.ALL).get(0).getId());

        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, RANGE_DISTRIBUTION_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            PartitionView partition = resp.getResult().getItems().get(0);

            assertEquals(3, partition.getBucketNum().intValue());
            assertEquals("RANGE", partition.getDistributionType());

            // The tablet list must come from the same index the bucket count was taken from.
            List<Long> tabletIds = partition.getTablets().stream()
                    .map(TabletView::getId).collect(Collectors.toList());
            assertEquals(Lists.newArrayList(30000L, 30001L, 30002L), tabletIds);
        } finally {
            db.dropTable(RANGE_DISTRIBUTION_TABLE_ID);
        }
    }

    /**
     * The base-index selection is confined to range distribution: a hash table carrying a rollup must
     * still serialize the index the generic enumeration yields, so existing clients see the same ids.
     */
    @Test
    public void testHashDistributionWithRollupKeepsGenericIndexSelection() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        LakeTable table = newHashDistributionLakeTableWithRollup();
        db.registerTableUnlocked(table);

        PhysicalPartition physicalPartition = table.getPartitions().iterator().next()
                .getDefaultPhysicalPartition();
        MaterializedIndex expectedIndex = physicalPartition
                .getLatestMaterializedIndices(IndexExtState.ALL).get(0);
        assertNotEquals(physicalPartition.getLatestBaseIndexOrNull().getId(), expectedIndex.getId());
        List<Long> expectedTabletIds = expectedIndex.getTablets().stream()
                .map(Tablet::getId).collect(Collectors.toList());

        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, HASH_ROLLUP_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<PagedResult<PartitionView>> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            PartitionView partition = resp.getResult().getItems().get(0);

            assertEquals(8, partition.getBucketNum().intValue());
            List<Long> tabletIds = partition.getTablets().stream()
                    .map(TabletView::getId).collect(Collectors.toList());
            assertEquals(expectedTabletIds, tabletIds);
        } finally {
            db.dropTable(HASH_ROLLUP_TABLE_ID);
        }
    }

    /**
     * Same shape as {@link #newRangeDistributionLakeTable()} but hash-distributed, so the generic
     * index-selection path this PR must not touch stays covered: base-index and rollup index ids are
     * offset from the range fixture's so the two never collide in the tablet inverted index.
     */
    private static LakeTable newHashDistributionLakeTableWithRollup() throws Exception {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Column c0 = new Column("c0", IntegerType.BIGINT, true, null, null, false, null, "cc0", 1);
        List<Column> columns = Lists.newArrayList(c0);

        long partitionId = BASE_PARTITION_ID + 600L;
        long physicalPartitionId = partitionId + 1L;

        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        HashDistributionInfo distributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c0));
        LakeTable lakeTable = new LakeTable(HASH_ROLLUP_TABLE_ID, HASH_ROLLUP_TABLE_NAME,
                columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);

        MaterializedIndex baseIndex = newIndexWithTablets(31000L, 3100L, 3,
                HASH_ROLLUP_TABLE_ID, physicalPartitionId);
        lakeTable.setBaseIndexMetaId(baseIndex.getMetaId());
        lakeTable.setIndexMeta(baseIndex.getMetaId(), HASH_ROLLUP_TABLE_NAME, columns, 0, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);

        FilePathInfo pathInfo = newTestPathInfo(HASH_ROLLUP_TABLE_NAME);
        lakeTable.setStorageInfo(pathInfo, new DataCacheInfo(false, false));
        mockLakeLookups(pathInfo);

        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", baseIndex,
                distributionInfo);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        physicalPartition.createRollupIndex(newIndexWithTablets(11000L, 1100L, 2,
                HASH_ROLLUP_TABLE_ID, physicalPartitionId));
        physicalPartition.createRollupIndex(newIndexWithTablets(21000L, 2100L, 4,
                HASH_ROLLUP_TABLE_ID, physicalPartitionId));
        partition.getDefaultPhysicalPartition().setVisibleVersion(testStartVersion, System.currentTimeMillis());
        partition.getDefaultPhysicalPartition().setNextVersion(testStartVersion + 1);
        lakeTable.addPartition(partition);

        return lakeTable;
    }

    private static FilePathInfo newTestPathInfo(String tableName) {
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
        return builder.build();
    }

    private static void mockLakeLookups(FilePathInfo pathInfo) throws Exception {
        new MockUp<LakeTablet>() {
            @Mock
            public Set<Long> getBackendIds() {
                return Sets.newHashSet(testBackendId1);
            }
        };
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getStarOSAgent().getShardInfo(anyLong, anyLong);
                minTimes = 0;
                result = ShardInfo.newBuilder().setFilePath(pathInfo).build();

                GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getComputeNodeId((ComputeResource) any, anyLong);
                minTimes = 0;
                result = testBackendId1;

                GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getAliveComputeNodeId((ComputeResource) any, anyLong);
                minTimes = 0;
                result = testBackendId1;
            }
        };
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
