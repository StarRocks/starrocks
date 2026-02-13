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

import com.google.common.collect.Lists;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.BatchModifyPartitionsInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.NextIdLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class LakeTableAlterDataCacheEnableTest {
    private static final int NUM_BUCKETS = 4;
    private ConnectContext connectContext;
    private Database db;
    private LakeTable table;
    private long partitionId;
    private final List<Long> shadowTabletIds = new ArrayList<>();
    private final AtomicBoolean updateShardGroupCalled = new AtomicBoolean(false);
    private final AtomicBoolean updateShardGroupEnableCache = new AtomicBoolean(false);
    private final List<BatchModifyPartitionsInfo> loggedBatchModifyPartitions = new ArrayList<>();

    public LakeTableAlterDataCacheEnableTest() {
        connectContext = new ConnectContext(null);
        connectContext.setStartTime();
        connectContext.setThreadLocalInfo();
    }

    @BeforeEach
    public void before() throws Exception {
        FeConstants.runningUnitTest = true;
        updateShardGroupCalled.set(false);
        updateShardGroupEnableCache.set(false);
        loggedBatchModifyPartitions.clear();

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(int shardCount, FilePathInfo path, FileCacheInfo cache, long groupId,
                                           List<Long> matchShardIds, Map<String, String> properties)
                    throws DdlException {
                for (int i = 0; i < shardCount; i++) {
                    shadowTabletIds.add(GlobalStateMgr.getCurrentState().getNextId());
                }
                return shadowTabletIds;
            }

            @Mock
            public void updateShardGroup(List<Partition> partitionsList, boolean enableCache) throws DdlException {
                updateShardGroupCalled.set(true);
                updateShardGroupEnableCache.set(enableCache);
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logAlterTableProperties(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
                walApplier.apply(info);
            }

            @Mock
            public void logSaveNextId(long nextId, WALApplier applier) {
                applier.apply(new NextIdLog(nextId));
            }

            @Mock
            public void logBatchModifyPartition(BatchModifyPartitionsInfo info) {
                loggedBatchModifyPartitions.add(info);
            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        final long dbId = GlobalStateMgr.getCurrentState().getNextId();
        partitionId = GlobalStateMgr.getCurrentState().getNextId();
        final long tableId = GlobalStateMgr.getCurrentState().getNextId();
        final long indexId = GlobalStateMgr.getCurrentState().getNextId();

        GlobalStateMgr.getCurrentState().setStarOSAgent(new StarOSAgent());

        KeysType keysType = KeysType.DUP_KEYS;
        db = new Database(dbId, "db_datacache_enable");

        Database oldDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().putIfAbsent(db.getId(), db);
        Assertions.assertNull(oldDb);

        Column c0 = new Column("c0", IntegerType.INT, true);
        DistributionInfo dist = new HashDistributionInfo(NUM_BUCKETS, Collections.singletonList(c0));
        PartitionInfo partitionInfo = new RangePartitionInfo(Collections.singletonList(c0));
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);

        table = new LakeTable(tableId, "t0", Collections.singletonList(c0), keysType, partitionInfo, dist);
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, partitionId + 100, "t0", index, dist);
        TStorageMedium storage = TStorageMedium.HDD;
        TabletMeta tabletMeta = new TabletMeta(db.getId(), table.getId(), partition.getId(), index.getId(), storage, true);
        for (int i = 0; i < NUM_BUCKETS; i++) {
            Tablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            index.addTablet(tablet, tabletMeta);
        }
        table.addPartition(partition);

        table.setIndexMeta(index.getMetaId(), "t0", Collections.singletonList(c0), 0, 0, (short) 1, TStorageType.COLUMN,
                keysType);
        table.setBaseIndexMetaId(index.getMetaId());

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
        builder.setFullPath("s3://test-bucket/datacache-enable-test");
        FilePathInfo pathInfo = builder.build();

        // Set table property first, then storage info (setStorageInfo writes to the table property)
        table.setTableProperty(new TableProperty(new HashMap<>()));

        // Initialize with datacache disabled
        table.setStorageInfo(pathInfo, new DataCacheInfo(false, false));
        DataCacheInfo dataCacheInfo = new DataCacheInfo(false, false);
        partitionInfo.setDataCacheInfo(partitionId, dataCacheInfo);

        db.registerTableUnlocked(table);
    }

    @AfterEach
    public void after() throws Exception {
        db.dropTable(table.getName());
    }

    private void executeAlterTable(Map<String, String> properties) throws Exception {
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return table;
            }
        };

        connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(), table.getName());
        new AlterJobExecutor().process(new AlterTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList(
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                        null, NodePosition.ZERO),
                Lists.newArrayList(modify)
        ), connectContext);
    }

    private void executeModifyPartition(String partitionName, Map<String, String> properties) throws Exception {
        ModifyPartitionClause modify = new ModifyPartitionClause(
                Collections.singletonList(partitionName), properties, NodePosition.ZERO);

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return table;
            }
        };

        connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(), table.getName());
        new AlterJobExecutor().process(new AlterTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList(
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                        null, NodePosition.ZERO),
                Lists.newArrayList(modify)
        ), connectContext);
    }

    // ==================== Table-level ALTER TABLE SET tests ====================

    @Test
    public void testEnableDataCacheOnTable() throws Exception {
        // Initially disabled
        PartitionInfo partitionInfo = table.getPartitionInfo();
        DataCacheInfo initialInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(initialInfo);
        Assertions.assertFalse(initialInfo.isEnabled());

        // Enable datacache
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        executeAlterTable(properties);

        // Verify table property
        Assertions.assertTrue(table.getTableProperty().getStorageInfo().getDataCacheInfo().isEnabled());

        // Verify partition DataCacheInfo updated
        DataCacheInfo updatedInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedInfo);
        Assertions.assertTrue(updatedInfo.isEnabled());

        // Verify StarOS updateShardGroup was called
        Assertions.assertTrue(updateShardGroupCalled.get());
        Assertions.assertTrue(updateShardGroupEnableCache.get());

        // Table-level ALTER TABLE SET does NOT write a separate OP_BATCH_MODIFY_PARTITION log.
        // The partition DataCacheInfo is persisted atomically as part of OP_ALTER_TABLE_PROPERTIES
        // and recovered during replay via replayModifyTableProperty.
        Assertions.assertEquals(0, loggedBatchModifyPartitions.size());
    }

    @Test
    public void testDisableDataCacheOnTable() throws Exception {
        // First enable
        PartitionInfo partitionInfo = table.getPartitionInfo();
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        // Disable datacache
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "false");
        executeAlterTable(properties);

        // Verify partition DataCacheInfo updated
        DataCacheInfo updatedInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedInfo);
        Assertions.assertFalse(updatedInfo.isEnabled());

        // Verify StarOS updateShardGroup was called with false
        Assertions.assertTrue(updateShardGroupCalled.get());
        Assertions.assertFalse(updateShardGroupEnableCache.get());

        // Table-level: no separate batch modify partition log
        Assertions.assertEquals(0, loggedBatchModifyPartitions.size());
    }

    @Test
    public void testEnableDataCacheWhenAlreadyEnabled() throws Exception {
        // Set datacache already enabled
        PartitionInfo partitionInfo = table.getPartitionInfo();
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        // Try to enable again
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        executeAlterTable(properties);

        // updateShardGroup should NOT be called (no partition needs update)
        Assertions.assertFalse(updateShardGroupCalled.get());

        // Table-level: no separate batch modify partition log
        Assertions.assertEquals(0, loggedBatchModifyPartitions.size());
    }

    @Test
    public void testDataCacheEnablePreservesAsyncWriteBack() throws Exception {
        // Set datacache with asyncWriteBack=true (though it's disabled by default in newer versions)
        PartitionInfo partitionInfo = table.getPartitionInfo();
        DataCacheInfo originalInfo = new DataCacheInfo(false, false);
        partitionInfo.setDataCacheInfo(partitionId, originalInfo);

        // Enable datacache
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        executeAlterTable(properties);

        // asyncWriteBack should be preserved (false)
        DataCacheInfo updatedInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertFalse(updatedInfo.isAsyncWriteBack());
    }

    @Test
    public void testDataCacheEnableOnNonCloudNativeTableFails() throws Exception {
        // Create a non-cloud-native OlapTable
        final long dbId2 = GlobalStateMgr.getCurrentState().getNextId();
        final long tableId2 = GlobalStateMgr.getCurrentState().getNextId();
        final long indexId2 = GlobalStateMgr.getCurrentState().getNextId();
        final long partitionId2 = GlobalStateMgr.getCurrentState().getNextId();

        Database db2 = new Database(dbId2, "db_non_cloud");
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().putIfAbsent(db2.getId(), db2);

        Column c0 = new Column("c0", IntegerType.INT, true);
        DistributionInfo dist = new HashDistributionInfo(NUM_BUCKETS, Collections.singletonList(c0));
        PartitionInfo partInfo = new RangePartitionInfo(Collections.singletonList(c0));
        partInfo.setDataProperty(partitionId2, DataProperty.DEFAULT_DATA_PROPERTY);
        partInfo.setReplicationNum(partitionId2, (short) 1);

        OlapTable olapTable = new OlapTable(tableId2, "t_olap", Collections.singletonList(c0),
                KeysType.DUP_KEYS, partInfo, dist);
        MaterializedIndex idx = new MaterializedIndex(indexId2, MaterializedIndex.IndexState.NORMAL);
        Partition p = new Partition(partitionId2, partitionId2 + 100, "t_olap", idx, dist);
        olapTable.addPartition(p);
        olapTable.setIndexMeta(indexId2, "t_olap", Collections.singletonList(c0), 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(indexId2);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        db2.registerTableUnlocked(olapTable);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db2;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return olapTable;
            }
        };

        connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                db2.getFullName(), olapTable.getName());
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);

        Assertions.assertThrows(AlterJobException.class, () -> {
            new AlterJobExecutor().process(new AlterTableStmt(
                    new TableRef(QualifiedName.of(Lists.newArrayList(
                            tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                            null, NodePosition.ZERO),
                    Lists.newArrayList(modify)
            ), connectContext);
        });
    }

    // ==================== Partition-level MODIFY PARTITION tests ====================

    @Test
    public void testEnableDataCacheOnPartition() throws Exception {
        PartitionInfo partitionInfo = table.getPartitionInfo();

        // Initially disabled
        Assertions.assertFalse(partitionInfo.getDataCacheInfo(partitionId).isEnabled());

        // Modify partition to enable datacache
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        executeModifyPartition("t0", properties);

        // Verify partition DataCacheInfo updated
        DataCacheInfo updatedInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedInfo);
        Assertions.assertTrue(updatedInfo.isEnabled());

        // Verify StarOS updateShardGroup was called
        Assertions.assertTrue(updateShardGroupCalled.get());
        Assertions.assertTrue(updateShardGroupEnableCache.get());

        // Verify batch modify partition was logged
        Assertions.assertEquals(1, loggedBatchModifyPartitions.size());
    }

    @Test
    public void testDisableDataCacheOnPartition() throws Exception {
        PartitionInfo partitionInfo = table.getPartitionInfo();

        // First enable
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        // Modify partition to disable datacache
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "false");
        executeModifyPartition("t0", properties);

        // Verify partition DataCacheInfo updated
        DataCacheInfo updatedInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedInfo);
        Assertions.assertFalse(updatedInfo.isEnabled());

        // Verify StarOS updateShardGroup was called with false
        Assertions.assertTrue(updateShardGroupCalled.get());
        Assertions.assertFalse(updateShardGroupEnableCache.get());
    }

    @Test
    public void testModifyPartitionDataCacheOnNonCloudNativeTableFails() throws Exception {
        // Create non-cloud-native table
        final long dbId2 = GlobalStateMgr.getCurrentState().getNextId();
        final long tableId2 = GlobalStateMgr.getCurrentState().getNextId();
        final long indexId2 = GlobalStateMgr.getCurrentState().getNextId();
        final long partitionId2 = GlobalStateMgr.getCurrentState().getNextId();

        Database db2 = new Database(dbId2, "db_non_cloud_part");
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().putIfAbsent(db2.getId(), db2);

        Column c0 = new Column("c0", IntegerType.INT, true);
        DistributionInfo dist = new HashDistributionInfo(NUM_BUCKETS, Collections.singletonList(c0));
        PartitionInfo partInfo = new RangePartitionInfo(Collections.singletonList(c0));
        partInfo.setDataProperty(partitionId2, DataProperty.DEFAULT_DATA_PROPERTY);
        partInfo.setReplicationNum(partitionId2, (short) 1);

        OlapTable olapTable = new OlapTable(tableId2, "t_olap2", Collections.singletonList(c0),
                KeysType.DUP_KEYS, partInfo, dist);
        MaterializedIndex idx = new MaterializedIndex(indexId2, MaterializedIndex.IndexState.NORMAL);
        Partition p = new Partition(partitionId2, partitionId2 + 100, "t_olap2", idx, dist);
        olapTable.addPartition(p);
        olapTable.setIndexMeta(indexId2, "t_olap2", Collections.singletonList(c0), 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(indexId2);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        db2.registerTableUnlocked(olapTable);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db2;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return olapTable;
            }
        };

        connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                db2.getFullName(), olapTable.getName());
        ModifyPartitionClause modify = new ModifyPartitionClause(
                Collections.singletonList("t_olap2"), properties, NodePosition.ZERO);

        Assertions.assertThrows(AlterJobException.class, () -> {
            new AlterJobExecutor().process(new AlterTableStmt(
                    new TableRef(QualifiedName.of(Lists.newArrayList(
                            tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                            null, NodePosition.ZERO),
                    Lists.newArrayList(modify)
            ), connectContext);
        });
    }

    @Test
    public void testUpdateShardGroupFailureThrowsException() throws Exception {
        // Mock StarOSAgent to throw exception on updateShardGroup
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(int shardCount, FilePathInfo path, FileCacheInfo cache, long groupId,
                                           List<Long> matchShardIds, Map<String, String> properties)
                    throws DdlException {
                return shadowTabletIds;
            }

            @Mock
            public void updateShardGroup(List<Partition> partitionsList, boolean enableCache) throws DdlException {
                throw new DdlException("StarOS update failed");
            }
        };

        PartitionInfo partitionInfo = table.getPartitionInfo();
        Assertions.assertFalse(partitionInfo.getDataCacheInfo(partitionId).isEnabled());

        // Try to enable — partition-level should throw exception (consistent with table-level)
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");

        Assertions.assertThrows(AlterJobException.class, () -> {
            executeModifyPartition("t0", properties);
        });

        // Verify in-memory datacache state was reverted to false
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertFalse(info.isEnabled());

        // Verify no batch modify partition was logged (exception prevented persistence)
        Assertions.assertEquals(0, loggedBatchModifyPartitions.size());
    }

    @Test
    public void testUpdateShardGroupFailureOnTableLevel() throws Exception {
        // Mock StarOSAgent to throw exception on updateShardGroup
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(int shardCount, FilePathInfo path, FileCacheInfo cache, long groupId,
                                           List<Long> matchShardIds, Map<String, String> properties)
                    throws DdlException {
                return shadowTabletIds;
            }

            @Mock
            public void updateShardGroup(List<Partition> partitionsList, boolean enableCache) throws DdlException {
                throw new DdlException("StarOS update failed");
            }
        };

        // Table-level: the error propagates
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");

        Assertions.assertThrows(AlterJobException.class, () -> {
            executeAlterTable(properties);
        });

        // Partition should remain unchanged (not enabled)
        PartitionInfo partitionInfo = table.getPartitionInfo();
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertFalse(info.isEnabled());
    }

    @Test
    public void testEnableDataCacheWhenDataCacheInfoIsNull() throws Exception {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        // Set DataCacheInfo to null to test the null check path
        partitionInfo.setDataCacheInfo(partitionId, null);

        // Enable datacache
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        executeAlterTable(properties);

        // Verify partition DataCacheInfo is now set and enabled
        DataCacheInfo updatedInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedInfo);
        Assertions.assertTrue(updatedInfo.isEnabled());

        // updateShardGroup should be called since DataCacheInfo was null
        Assertions.assertTrue(updateShardGroupCalled.get());
    }

    @Test
    public void testModifyPartitionOtherPropertyDoesNotChangeDatacache() throws Exception {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        // Set datacache to enabled with asyncWriteBack=true
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, true));

        // Mock analyzeReplicationNum to avoid backend availability check in unit test
        new MockUp<PropertyAnalyzer>() {
            @Mock
            public Short analyzeReplicationNum(Map<String, String> properties, short oldReplicationNum) {
                short replicationNum = oldReplicationNum;
                if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                    replicationNum = Short.parseShort(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
                    properties.remove(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
                }
                return replicationNum;
            }
        };

        // Modify only replication_num, NOT datacache.enable
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
        executeModifyPartition("t0", properties);

        // Verify datacache state is unchanged (still enabled with asyncWriteBack)
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertTrue(info.isEnabled());
        Assertions.assertTrue(info.isAsyncWriteBack());

        // Verify updateShardGroup was NOT called
        Assertions.assertFalse(updateShardGroupCalled.get());

        // Verify the logged ModifyPartitionInfo has null dataCacheEnable
        Assertions.assertEquals(1, loggedBatchModifyPartitions.size());
        ModifyPartitionInfo modInfo = loggedBatchModifyPartitions.get(0).getModifyPartitionInfos().get(0);
        Assertions.assertNull(modInfo.getDataCacheEnable());
    }

    @Test
    public void testEnableDataCacheOnPartitionWhenAlreadyEnabled() throws Exception {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        // Set datacache already enabled
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        // Try to enable again
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        executeModifyPartition("t0", properties);

        // Datacache should still be enabled
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertTrue(info.isEnabled());

        // updateShardGroup should NOT be called (no change needed)
        Assertions.assertFalse(updateShardGroupCalled.get());
    }

    @Test
    public void testModifyPartitionDataCachePreservesAsyncWriteBack() throws Exception {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        // Set asyncWriteBack=true (represents a legacy configuration)
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, true));

        // Disable datacache
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "false");
        executeModifyPartition("t0", properties);

        // Verify asyncWriteBack is preserved even after datacache state change
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertFalse(info.isEnabled());
        Assertions.assertTrue(info.isAsyncWriteBack());
    }

    @Test
    public void testReplayModifyPartitionWithDataCacheEnable() {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        // Initially datacache disabled, asyncWriteBack=true
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(false, true));

        // Create a ModifyPartitionInfo with dataCacheEnable=true
        ModifyPartitionInfo modInfo = new ModifyPartitionInfo(
                db.getId(), table.getId(), partitionId,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) -1, true);

        // Replay
        GlobalStateMgr.getCurrentState().getAlterJobMgr().replayModifyPartition(modInfo);

        // Verify datacache is now enabled
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertTrue(info.isEnabled());
        // asyncWriteBack should be preserved from the original DataCacheInfo
        Assertions.assertTrue(info.isAsyncWriteBack());
    }

    @Test
    public void testReplayModifyPartitionWithoutDataCacheField() {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        // Set datacache to enabled
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        // Create a ModifyPartitionInfo WITHOUT dataCacheEnable (old format, backward compat)
        ModifyPartitionInfo modInfo = new ModifyPartitionInfo(
                db.getId(), table.getId(), partitionId,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) -1);

        // Replay
        GlobalStateMgr.getCurrentState().getAlterJobMgr().replayModifyPartition(modInfo);

        // Verify datacache state is UNCHANGED (still enabled) — backward compatibility
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertTrue(info.isEnabled());
    }

    @Test
    public void testReplayModifyTablePropertyWithDataCacheEnable() {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        // Initially datacache disabled, asyncWriteBack=true
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(false, true));

        // Create ModifyTablePropertyOperationLog with datacache.enable=true
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(
                db.getId(), table.getId(), properties);

        // Replay via replayModifyTableProperty with OP_ALTER_TABLE_PROPERTIES
        GlobalStateMgr.getCurrentState().getLocalMetastore().replayModifyTableProperty(
                OperationType.OP_ALTER_TABLE_PROPERTIES, log);

        // Verify table-level property updated
        Assertions.assertTrue(table.getTableProperty().getStorageInfo().isEnableDataCache());

        // Verify partition-level DataCacheInfo updated
        DataCacheInfo info = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertTrue(info.isEnabled());
        // asyncWriteBack should be preserved
        Assertions.assertTrue(info.isAsyncWriteBack());
    }
}
