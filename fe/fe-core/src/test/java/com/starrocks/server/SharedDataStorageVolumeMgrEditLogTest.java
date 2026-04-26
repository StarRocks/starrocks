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

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.TableStorageInfos;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SharedDataStorageVolumeMgrEditLogTest {
    @Mocked
    private StarOSAgent starOSAgent;

    private SharedDataStorageVolumeMgr masterStorageVolumeMgr;
    private Database testDatabase;
    private LakeTable testTable;
    private static final long TEST_DB_ID = 10001L;
    private static final long TEST_TABLE_ID = 10002L;
    private static final long TEST_PARTITION_ID = 10003L;
    private static final long TEST_INDEX_ID = 10004L;
    private static final long TEST_TABLET_ID = 10005L;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "access_key";
        Config.aws_s3_secret_key = "secret_key";
        Config.aws_s3_region = "region";
        Config.aws_s3_endpoint = "endpoint";
        Config.aws_s3_path = "default-bucket/1";
        Config.enable_load_volume_from_conf = true;

        // Mock StarOSAgent
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        // Mock StarOSAgent methods
        new MockUp<StarOSAgent>() {
            Map<String, FileStoreInfo> fileStores = new HashMap<>();
            private long id = 1;

            @Mock
            public String addFileStore(FileStoreInfo fsInfo) {
                if (fsInfo.getFsKey().isEmpty()) {
                    fsInfo = fsInfo.toBuilder().setFsKey(String.valueOf(id++)).build();
                }
                fileStores.put(fsInfo.getFsKey(), fsInfo);
                return fsInfo.getFsKey();
            }

            @Mock
            public FileStoreInfo getFileStoreByName(String fsName) {
                for (FileStoreInfo fsInfo : fileStores.values()) {
                    if (fsInfo.getFsName().equals(fsName)) {
                        return fsInfo;
                    }
                }
                return null;
            }

            @Mock
            public FileStoreInfo getFileStore(String fsKey) {
                return fileStores.get(fsKey);
            }

            @Mock
            public void updateFileStore(FileStoreInfo fsInfo) {
                fileStores.put(fsInfo.getFsKey(), fsInfo);
            }

            @Mock
            public void replaceFileStore(FileStoreInfo fsInfo) {
                fileStores.put(fsInfo.getFsKey(), fsInfo);
            }

            @Mock
            public void removeFileStoreByName(String fsName) throws DdlException {
                FileStoreInfo fsInfo = getFileStoreByName(fsName);
                if (fsInfo == null) {
                    throw new DdlException("Failed to remove file store");
                }
                fileStores.remove(fsInfo.getFsKey());
            }

            @Mock
            public FilePathInfo allocateFilePath(String storageVolumeId, long dbId, long tableId)
                    throws DdlException {
                // Create a mock FilePathInfo
                FilePathInfo.Builder builder = FilePathInfo.newBuilder();
                FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();
                S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
                s3FsBuilder.setBucket("test-bucket");
                s3FsBuilder.setRegion("test-region");
                S3FileStoreInfo s3FsInfo = s3FsBuilder.build();
                fsBuilder.setFsType(FileStoreType.S3);
                fsBuilder.setFsKey(storageVolumeId);
                fsBuilder.setS3FsInfo(s3FsInfo);
                FileStoreInfo fsInfo = fsBuilder.build();
                builder.setFsInfo(fsInfo);
                builder.setFullPath(String.format("s3://test-bucket/%d/%d", dbId, tableId));
                return builder.build();
            }
        };

        // Get StorageVolumeMgr instance
        // Note: This test requires SharedDataStorageVolumeMgr
        StorageVolumeMgr currentMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        if (currentMgr instanceof SharedDataStorageVolumeMgr) {
            masterStorageVolumeMgr = (SharedDataStorageVolumeMgr) currentMgr;
        } else {
            // If not SharedDataStorageVolumeMgr, create a new instance for testing
            // This may happen if RunMode is not SharedDataMode
            masterStorageVolumeMgr = new SharedDataStorageVolumeMgr();
            // Note: We cannot set it to GlobalStateMgr, so we'll use it directly
            // The test will use this instance instead of GlobalStateMgr's instance
        }

        // Create test database and table
        createTestDatabaseAndTable();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();

        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "";
        Config.aws_s3_secret_key = "";
        Config.aws_s3_region = "";
        Config.aws_s3_endpoint = "";
        Config.aws_s3_path = "";
        Config.enable_load_volume_from_conf = false;
    }

    private void createTestDatabaseAndTable() throws Exception {
        // Create database
        testDatabase = new Database(TEST_DB_ID, "test_db");
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(testDatabase);

        // Create table columns
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", IntegerType.INT);
        col1.setIsKey(true);
        columns.add(col1);
        Column col2 = new Column("v1", IntegerType.BIGINT);
        columns.add(col2);

        // Create partition info
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(TEST_PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(TEST_PARTITION_ID, (short) 1);

        // Create distribution info
        HashDistributionInfo distributionInfo = new HashDistributionInfo(3, Lists.newArrayList(col1));

        // Create MaterializedIndex
        MaterializedIndex materializedIndex = new MaterializedIndex(TEST_INDEX_ID, MaterializedIndex.IndexState.NORMAL);

        // Create LakeTablet
        LakeTablet tablet = new LakeTablet(TEST_TABLET_ID);
        com.starrocks.catalog.TabletMeta tabletMeta = new com.starrocks.catalog.TabletMeta(
                TEST_DB_ID, TEST_TABLE_ID, TEST_PARTITION_ID, TEST_INDEX_ID,
                com.starrocks.thrift.TStorageMedium.HDD, true);
        materializedIndex.addTablet(tablet, tabletMeta);

        // Create Partition
        Partition partition = new Partition(TEST_PARTITION_ID, TEST_PARTITION_ID + 100, "test_partition",
                materializedIndex, distributionInfo);

        // Create LakeTable
        testTable = new LakeTable(TEST_TABLE_ID, "test_table", columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        testTable.setIndexMeta(TEST_INDEX_ID, "test_table", columns, 0, 0, (short) 1,
                com.starrocks.thrift.TStorageType.COLUMN, KeysType.DUP_KEYS);
        testTable.setBaseIndexMetaId(TEST_INDEX_ID);
        testTable.addPartition(partition);

        // Create TableProperty with StorageInfo
        TableProperty tableProperty = new TableProperty(new HashMap<>());
        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();
        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("old-bucket");
        s3FsBuilder.setRegion("old-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();
        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("old-storage-volume-id");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();
        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://old-bucket/old/path");
        FilePathInfo oldFilePathInfo = builder.build();
        StorageInfo storageInfo = new StorageInfo(oldFilePathInfo, null);
        tableProperty.setStorageInfo(storageInfo);
        testTable.setTableProperty(tableProperty);

        // Register table to database
        testDatabase.registerTableUnlocked(testTable);
    }

    private Map<String, String> createTestParams() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "us-west-2");
        storageParams.put(AWS_S3_ENDPOINT, "https://s3.us-west-2.amazonaws.com");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        return storageParams;
    }

    @Test
    public void testUpdateTableStorageInfoNormalCase() throws Exception {
        // 1. Create a storage volume
        String svName = "test_update_table_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "test storage volume");

        // 2. Bind table to storage volume
        masterStorageVolumeMgr.bindTableToStorageVolume(svName, TEST_DB_ID, TEST_TABLE_ID);

        // 3. Get initial storage info
        TableProperty tableProperty = testTable.getTableProperty();
        StorageInfo initialStorageInfo = tableProperty.getStorageInfo();
        String initialPath = initialStorageInfo.getFilePathInfo().getFullPath();

        // 4. Execute updateTableStorageInfo operation (master side)
        masterStorageVolumeMgr.updateTableStorageInfo(storageVolumeId);

        // 5. Verify master state - storage info should be updated
        StorageInfo updatedStorageInfo = tableProperty.getStorageInfo();
        Assertions.assertNotNull(updatedStorageInfo);
        String newPath = updatedStorageInfo.getFilePathInfo().getFullPath();
        Assertions.assertNotEquals(initialPath, newPath);
        Assertions.assertTrue(newPath.contains(String.valueOf(TEST_DB_ID)));
        Assertions.assertTrue(newPath.contains(String.valueOf(TEST_TABLE_ID)));

        // 6. Test follower replay functionality
        SharedDataStorageVolumeMgr followerStorageVolumeMgr = new SharedDataStorageVolumeMgr();
        
        // Clean up if storage volume already exists
        if (followerStorageVolumeMgr.exists(svName)) {
            try {
                followerStorageVolumeMgr.removeStorageVolume(svName);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        
        followerStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "test storage volume");

        // Create same database and table in follower
        Database followerDb = new Database(TEST_DB_ID, "test_db");
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(followerDb);
        LakeTable followerTable = createFollowerTable();
        followerDb.registerTableUnlocked(followerTable);
        followerStorageVolumeMgr.bindTableToStorageVolume(svName, TEST_DB_ID, TEST_TABLE_ID);

        TableStorageInfos replayInfo = (TableStorageInfos) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_TABLE_STORAGE_INFOS);

        // Execute follower replay
        followerStorageVolumeMgr.replayUpdateTableStorageInfos(replayInfo);

        // 7. Verify follower state is consistent with master
        TableProperty followerTableProperty = followerTable.getTableProperty();
        StorageInfo followerStorageInfo = followerTableProperty.getStorageInfo();
        Assertions.assertNotNull(followerStorageInfo);
        String followerPath = followerStorageInfo.getFilePathInfo().getFullPath();
        Assertions.assertEquals(newPath, followerPath);
    }

    @Test
    public void testUpdateTableStorageInfoEditLogException() throws Exception {
        // 1. Create a storage volume
        String svName = "exception_update_table_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "test storage volume");

        // 2. Bind table to storage volume
        masterStorageVolumeMgr.bindTableToStorageVolume(svName, TEST_DB_ID, TEST_TABLE_ID);

        // 3. Get initial storage info
        TableProperty tableProperty = testTable.getTableProperty();
        StorageInfo initialStorageInfo = tableProperty.getStorageInfo();
        String initialPath = initialStorageInfo.getFilePathInfo().getFullPath();

        EditLog spyEditLog = spy(new EditLog(null));

        // 4. Mock EditLog.logUpdateTableStorageInfos to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateTableStorageInfos(any(TableStorageInfos.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 5. Execute updateTableStorageInfo operation and expect exception
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            masterStorageVolumeMgr.updateTableStorageInfo(storageVolumeId);
        });
        // When using reflection, exceptions are wrapped in InvocationTargetException
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 6. Verify leader memory state remains unchanged after exception
        StorageInfo currentStorageInfo = tableProperty.getStorageInfo();
        String currentPath = currentStorageInfo.getFilePathInfo().getFullPath();
        Assertions.assertEquals(initialPath, currentPath);
    }

    @Test
    public void testUpdateTableStorageInfoNoBindedTables() throws Exception {
        // 1. Create a storage volume
        String svName = "empty_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "test storage volume");

        // 2. Don't bind any table to storage volume

        // 3. Execute updateTableStorageInfo operation (should not fail, just do nothing)
        masterStorageVolumeMgr.updateTableStorageInfo(storageVolumeId);

        // 4. Verify no exception is thrown and state remains unchanged
        TableProperty tableProperty = testTable.getTableProperty();
        StorageInfo storageInfo = tableProperty.getStorageInfo();
        Assertions.assertNotNull(storageInfo);
        // Path should remain unchanged since no table is bound
        Assertions.assertTrue(storageInfo.getFilePathInfo().getFullPath().contains("old-bucket"));
    }

    @Test
    public void testUpdateTableStorageInfoMultipleTables() throws Exception {
        // 1. Create a storage volume
        String svName = "multi_table_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "test storage volume");

        // 2. Create another table
        long tableId2 = TEST_TABLE_ID + 1;
        LakeTable table2 = createTestTable(tableId2, "test_table2");
        testDatabase.registerTableUnlocked(table2);

        // 3. Bind both tables to storage volume
        masterStorageVolumeMgr.bindTableToStorageVolume(svName, TEST_DB_ID, TEST_TABLE_ID);
        masterStorageVolumeMgr.bindTableToStorageVolume(svName, TEST_DB_ID, tableId2);

        // 4. Execute updateTableStorageInfo operation
        masterStorageVolumeMgr.updateTableStorageInfo(storageVolumeId);

        // 5. Verify both tables' storage info are updated
        TableProperty tableProperty1 = testTable.getTableProperty();
        StorageInfo storageInfo1 = tableProperty1.getStorageInfo();
        Assertions.assertNotNull(storageInfo1);
        String path1 = storageInfo1.getFilePathInfo().getFullPath();
        Assertions.assertTrue(path1.contains(String.valueOf(TEST_TABLE_ID)));

        TableProperty tableProperty2 = table2.getTableProperty();
        StorageInfo storageInfo2 = tableProperty2.getStorageInfo();
        Assertions.assertNotNull(storageInfo2);
        String path2 = storageInfo2.getFilePathInfo().getFullPath();
        Assertions.assertTrue(path2.contains(String.valueOf(tableId2)));
        Assertions.assertNotEquals(path1, path2);
    }

    private LakeTable createFollowerTable() {
        // Create a similar table for follower
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", IntegerType.INT);
        col1.setIsKey(true);
        columns.add(col1);
        Column col2 = new Column("v1", IntegerType.BIGINT);
        columns.add(col2);

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(TEST_PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(TEST_PARTITION_ID, (short) 1);

        HashDistributionInfo distributionInfo = new HashDistributionInfo(3, Lists.newArrayList(col1));

        MaterializedIndex materializedIndex = new MaterializedIndex(TEST_INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LakeTablet tablet = new LakeTablet(TEST_TABLET_ID);
        com.starrocks.catalog.TabletMeta tabletMeta = new com.starrocks.catalog.TabletMeta(
                TEST_DB_ID, TEST_TABLE_ID, TEST_PARTITION_ID, TEST_INDEX_ID,
                com.starrocks.thrift.TStorageMedium.HDD, true);
        materializedIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(TEST_PARTITION_ID, TEST_PARTITION_ID + 100, "test_partition",
                materializedIndex, distributionInfo);

        LakeTable table = new LakeTable(TEST_TABLE_ID, "test_table", columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        table.setIndexMeta(TEST_INDEX_ID, "test_table", columns, 0, 0, (short) 1,
                com.starrocks.thrift.TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexMetaId(TEST_INDEX_ID);
        table.addPartition(partition);

        TableProperty tableProperty = new TableProperty(new HashMap<>());
        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();
        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("old-bucket");
        s3FsBuilder.setRegion("old-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();
        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("old-storage-volume-id");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();
        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://old-bucket/old/path");
        FilePathInfo oldFilePathInfo = builder.build();
        StorageInfo storageInfo = new StorageInfo(oldFilePathInfo, null);
        tableProperty.setStorageInfo(storageInfo);
        table.setTableProperty(tableProperty);

        return table;
    }

    private LakeTable createTestTable(long tableId, String tableName) {
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", IntegerType.INT);
        col1.setIsKey(true);
        columns.add(col1);
        Column col2 = new Column("v1", IntegerType.BIGINT);
        columns.add(col2);

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(TEST_PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(TEST_PARTITION_ID, (short) 1);

        HashDistributionInfo distributionInfo = new HashDistributionInfo(3, Lists.newArrayList(col1));

        MaterializedIndex materializedIndex = new MaterializedIndex(TEST_INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LakeTablet tablet = new LakeTablet(TEST_TABLET_ID);
        com.starrocks.catalog.TabletMeta tabletMeta = new com.starrocks.catalog.TabletMeta(
                TEST_DB_ID, tableId, TEST_PARTITION_ID, TEST_INDEX_ID,
                com.starrocks.thrift.TStorageMedium.HDD, true);
        materializedIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(TEST_PARTITION_ID, TEST_PARTITION_ID + 100, "test_partition",
                materializedIndex, distributionInfo);

        LakeTable table = new LakeTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        table.setIndexMeta(TEST_INDEX_ID, tableName, columns, 0, 0, (short) 1,
                com.starrocks.thrift.TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexMetaId(TEST_INDEX_ID);
        table.addPartition(partition);

        TableProperty tableProperty = new TableProperty(new HashMap<>());
        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();
        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("old-bucket");
        s3FsBuilder.setRegion("old-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();
        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("old-storage-volume-id");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();
        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://old-bucket/old/path");
        FilePathInfo oldFilePathInfo = builder.build();
        StorageInfo storageInfo = new StorageInfo(oldFilePathInfo, null);
        tableProperty.setStorageInfo(storageInfo);
        table.setTableProperty(tableProperty);

        return table;
    }
}

