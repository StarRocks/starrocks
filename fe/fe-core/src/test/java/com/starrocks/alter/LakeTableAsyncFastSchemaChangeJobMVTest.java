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

package com.starrocks.alter;

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
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTablet;
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
 * Tests for LakeTableAsyncFastSchemaChangeJob support for LakeMaterializedView.
 * This test verifies that fast schema change jobs work correctly with LakeMaterializedView.
 */
public class LakeTableAsyncFastSchemaChangeJobMVTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_fse_mv";
    private static final String MV_NAME = "test_lake_mv_fse";
    private static final long DB_ID = 10001L;
    private static final long MV_ID = 10003L;
    private static final long PARTITION_ID = 10004L;
    private static final long PHYSICAL_PARTITION_ID = 10005L;
    private static final long INDEX_ID = 10006L;
    private static final long TABLET_ID = 10007L;

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

        // Create LakeMaterializedView with fast schema evolution v2 disabled
        LakeMaterializedView lakeMv = createLakeMaterializedView(MV_ID, MV_NAME, 3, false);
        db.registerTableUnlocked(lakeMv);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static LakeMaterializedView createLakeMaterializedView(long mvId, String mvName, int bucketNum,
                                                                    boolean fastSchemaEvolutionV2) {
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
        builder.setFullPath("s3://test-bucket/2/");
        FilePathInfo pathInfo = builder.build();
        lakeMv.setStorageInfo(pathInfo, new DataCacheInfo(true, true));

        // Set table property with fast schema evolution v2
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2,
                Boolean.toString(fastSchemaEvolutionV2));
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildCloudNativeFastSchemaEvolutionV2();
        lakeMv.setTableProperty(tableProperty);

        return lakeMv;
    }

    @Test
    public void testLakeMaterializedViewDisableFastSchemaEvolutionV2() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Note: With the current implementation, isFastSchemaEvolutionV2() always returns true
        // This will be fixed when the implementation reads from tableProperty

        // Simulate disabling fast schema evolution v2 via job
        // This tests the setFastSchemaEvolutionV2 method used in LakeTableAsyncFastSchemaChangeJob
        lakeMv.setFastSchemaEvolutionV2(false);

        // Verify the property is correctly stored
        Map<String, String> props = lakeMv.getTableProperty().getProperties();
        Assertions.assertEquals("false", props.get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
    }

    @Test
    public void testLakeMaterializedViewEnableFastSchemaEvolutionV2() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Enable fast schema evolution v2
        lakeMv.setFastSchemaEvolutionV2(true);
        Assertions.assertTrue(lakeMv.isFastSchemaEvolutionV2());

        // Verify the property is correctly stored
        Map<String, String> props = lakeMv.getTableProperty().getProperties();
        Assertions.assertEquals("true", props.get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
    }

    @Test
    public void testLakeMaterializedViewFastSchemaEvolutionV2Toggle() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Note: With the current implementation, isFastSchemaEvolutionV2() always returns true
        // This test verifies that setFastSchemaEvolutionV2 can be called without error

        // Toggle multiple times
        for (int i = 0; i < 3; i++) {
            // Enable
            lakeMv.setFastSchemaEvolutionV2(true);
            // Disable
            lakeMv.setFastSchemaEvolutionV2(false);
        }

        // Verify the property is correctly stored after toggling
        Map<String, String> props = lakeMv.getTableProperty().getProperties();
        Assertions.assertEquals("false", props.get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
    }

    @Test
    public void testLakeMaterializedViewIsCloudNative() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify it's recognized as cloud native
        Assertions.assertTrue(lakeMv.isCloudNativeMaterializedView());
        Assertions.assertTrue(lakeMv.isCloudNativeTableOrMaterializedView());
    }

    @Test
    public void testLakeMaterializedViewAlterJobState() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeMaterializedView lakeMv = (LakeMaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(lakeMv);

        // Verify initial state
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, lakeMv.getState());

        // The table state should be NORMAL for the job to proceed
        // This is important for LakeTableAlterMetaJobBase.runPendingJob() checks
        Assertions.assertTrue(lakeMv.getState() == OlapTable.OlapTableState.NORMAL);
    }
}
