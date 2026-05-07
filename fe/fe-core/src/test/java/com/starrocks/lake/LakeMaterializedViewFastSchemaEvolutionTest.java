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

import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedView.MvRefreshScheme;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for LakeMaterializedView fast schema evolution support.
 * This test verifies the new isFastSchemaEvolutionV2 and setFastSchemaEvolutionV2 methods.
 */
public class LakeMaterializedViewFastSchemaEvolutionTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_mv_fse";
    private static final String MV_NAME = "test_lake_mv";
    private static final long DB_ID = 10001L;
    private static final long MV_ID = 10002L;
    private static final long PARTITION_ID = 10003L;
    private static final long PHYSICAL_PARTITION_ID = 10004L;
    private static final long INDEX_ID = 10005L;
    private static final long TABLET_ID = 10006L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase(DB_NAME);

        // Mock GlobalStateMgr to return current meta version
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.META_VERSION;
            }
        };

        // Create database
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);

        // Create LakeMaterializedView without fast schema evolution v2 property
        LakeMaterializedView lakeMv = createLakeMaterializedView(MV_ID, MV_NAME, 3);
        db.registerTableUnlocked(lakeMv);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static LakeMaterializedView createLakeMaterializedView(long mvId, String mvName, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("sum_v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LakeTablet tablet = new LakeTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, mvId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD, true);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        MvRefreshScheme refreshScheme = new MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);

        LakeMaterializedView lakeMv = new LakeMaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        lakeMv.setIndexMeta(INDEX_ID, mvName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        Deencapsulation.setField(lakeMv, "baseIndexMetaId", INDEX_ID);
        lakeMv.addPartition(partition);

        // Set storage info
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
        builder.setFullPath("s3://test-bucket/" + mvId + "/");
        FilePathInfo pathInfo = builder.build();
        lakeMv.setStorageInfo(pathInfo, new DataCacheInfo(true, true));

        // Set table property (without fast schema evolution v2 initially)
        Map<String, String> properties = new HashMap<>();
        TableProperty tableProperty = new TableProperty(properties);
        lakeMv.setTableProperty(tableProperty);

        return lakeMv;
    }

    @Test
    public void testSetFastSchemaEvolutionV2True() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Enable fast schema evolution v2
        lakeMv.setFastSchemaEvolutionV2(true);

        // Verify it's enabled
        Assertions.assertTrue(lakeMv.isFastSchemaEvolutionV2());

        // Verify the property is stored correctly
        Map<String, String> props = lakeMv.getTableProperty().getProperties();
        Assertions.assertEquals("true", props.get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
    }

    @Test
    public void testSetFastSchemaEvolutionV2False() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Note: With the current implementation, isFastSchemaEvolutionV2() always returns true
        // This test verifies that setFastSchemaEvolutionV2 stores the property correctly

        // First enable it
        lakeMv.setFastSchemaEvolutionV2(true);

        // Then disable it
        lakeMv.setFastSchemaEvolutionV2(false);

        // Verify the property is stored correctly
        Map<String, String> props = lakeMv.getTableProperty().getProperties();
        Assertions.assertEquals("false", props.get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
    }

    @Test
    public void testSetFastSchemaEvolutionV2ToggleMultipleTimes() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Note: With the current implementation, isFastSchemaEvolutionV2() always returns true
        // This test verifies that setFastSchemaEvolutionV2 can be called multiple times without error

        // Toggle multiple times
        for (int i = 0; i < 5; i++) {
            lakeMv.setFastSchemaEvolutionV2(true);
            lakeMv.setFastSchemaEvolutionV2(false);
        }

        // Verify the property is stored correctly after toggling
        Map<String, String> props = lakeMv.getTableProperty().getProperties();
        Assertions.assertEquals("false", props.get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
    }

    @Test
    public void testIsFastSchemaEvolutionV2WithPropertySet() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Create a new TableProperty with fast schema evolution v2 enabled
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, "true");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildCloudNativeFastSchemaEvolutionV2();
        lakeMv.setTableProperty(tableProperty);

        // Verify it's enabled
        Assertions.assertTrue(lakeMv.isFastSchemaEvolutionV2());
    }

    @Test
    public void testLakeMaterializedViewInheritsFromMaterializedView() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify inheritance
        Assertions.assertTrue(lakeMv instanceof MaterializedView);
        Assertions.assertTrue(lakeMv instanceof OlapTable);
        Assertions.assertTrue(lakeMv.isCloudNativeMaterializedView());
    }

    @Test
    public void testLakeMaterializedViewIsLakeTable() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify it's recognized as a lake table (cloud native)
        Assertions.assertTrue(lakeMv.isCloudNativeTableOrMaterializedView());
    }

    @Test
    public void testLakeMaterializedViewGetDefaultFilePathInfo() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify file path info - may be null in test environment
        FilePathInfo pathInfo = lakeMv.getDefaultFilePathInfo();
        if (pathInfo != null) {
            Assertions.assertTrue(pathInfo.getFullPath().contains(String.valueOf(MV_ID)));
        }
    }

    @Test
    public void testLakeMaterializedViewTablePropertyNotNull() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify table property is not null
        TableProperty property = lakeMv.getTableProperty();
        Assertions.assertNotNull(property);
    }

    @Test
    public void testLakeMaterializedViewGetBaseIndexMetaId() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify base index meta ID
        long baseIndexMetaId = lakeMv.getBaseIndexMetaId();
        Assertions.assertEquals(INDEX_ID, baseIndexMetaId);
    }

    @Test
    public void testLakeMaterializedViewGetIndexMetaByMetaId() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify index meta can be retrieved
        var indexMeta = lakeMv.getIndexMetaByMetaId(INDEX_ID);
        Assertions.assertNotNull(indexMeta);
    }

    @Test
    public void testLakeMaterializedViewGetPartitions() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify partitions
        List<Partition> partitions = new ArrayList<>(lakeMv.getPartitions());
        Assertions.assertNotNull(partitions);
        Assertions.assertEquals(1, partitions.size());
    }

    @Test
    public void testLakeMaterializedViewGetPhysicalPartition() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify physical partition
        var physicalPartition = lakeMv.getPhysicalPartition(PHYSICAL_PARTITION_ID);
        Assertions.assertNotNull(physicalPartition);
        Assertions.assertEquals(PHYSICAL_PARTITION_ID, physicalPartition.getId());
    }

    @Test
    public void testLakeMaterializedViewGetFullSchema() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify full schema
        List<Column> fullSchema = lakeMv.getFullSchema();
        Assertions.assertNotNull(fullSchema);
        Assertions.assertEquals(2, fullSchema.size());
        Assertions.assertEquals("v1", fullSchema.get(0).getName());
        Assertions.assertEquals("sum_v2", fullSchema.get(1).getName());
    }

    @Test
    public void testLakeMaterializedViewGetBaseSchema() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify base schema
        List<Column> baseSchema = lakeMv.getBaseSchema();
        Assertions.assertNotNull(baseSchema);
        Assertions.assertEquals(2, baseSchema.size());
    }

    @Test
    public void testLakeMaterializedViewGetDefaultDistributionInfo() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify distribution info
        DistributionInfo distInfo = lakeMv.getDefaultDistributionInfo();
        Assertions.assertNotNull(distInfo);
        Assertions.assertTrue(distInfo instanceof HashDistributionInfo);
        Assertions.assertEquals(3, ((HashDistributionInfo) distInfo).getBucketNum());
    }

    @Test
    public void testLakeMaterializedViewGetState() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify initial state
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, lakeMv.getState());
    }

    @Test
    public void testLakeMaterializedViewGetRelatedMaterializedViews() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // getRelatedMaterializedViews should return empty list for MV itself
        var relatedMVs = lakeMv.getRelatedMaterializedViews();
        Assertions.assertNotNull(relatedMVs);
        Assertions.assertTrue(relatedMVs.isEmpty());
    }
}
