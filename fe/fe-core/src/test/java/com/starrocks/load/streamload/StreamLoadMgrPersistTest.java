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

package com.starrocks.load.streamload;

import com.google.common.collect.Lists;
import com.google.gson.stream.JsonReader;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.load.batchwrite.MergeCommitTask;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TStorageType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamLoadMgrPersistTest {
    private StreamLoadMgr masterStreamLoadMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create StreamLoadMgr instance
        masterStreamLoadMgr = new StreamLoadMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // Helper method to create a database with table
    private Database createDatabaseWithTable(long dbId, String dbName, long tableId, String tableName,
                                             long partitionId, long indexId) throws Exception {
        // Create database
        Database database = new Database(dbId, dbName);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(database);
        
        // Create table columns
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", Type.INT);
        col1.setIsKey(true);
        columns.add(col1);
        Column col2 = new Column("v1", Type.BIGINT);
        columns.add(col2);
        
        // Create partition info
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);
        
        // Create distribution info
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(3);
        
        // Create MaterializedIndex
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        
        // Create Partition
        Partition partition = new Partition(partitionId, partitionId + 100, tableName, 
                materializedIndex, distributionInfo);
        
        // Create OlapTable
        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(indexId, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexId(indexId);
        olapTable.addPartition(partition);
        
        // Register table to database
        database.registerTableUnlocked(olapTable);
        
        return database;
    }

    @Test
    public void testImageWithMixedTaskTypes() throws Exception {
        // StreamLoadMgr should have StreamLoadTask, StreamLoadMultiStmtTask, and MergeCommitTask
        // Only StreamLoadTask and StreamLoadMultiStmtTask should be persisted (MergeCommitTask is not in PERSISTENT_TASK_TYPES)

        // 1. Prepare test data
        String dbName = "test_db_save_load";
        String tableName = "test_table_save_load";
        long dbId = 10005L;
        long tableId = 20005L;
        long partitionId = 30005L;
        long indexId = 40005L;

        // Create database with table
        Database database = createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        OlapTable olapTable = (OlapTable) database.getTable(tableName);

        String user = "test_user";
        String clientIp = "127.0.0.1";
        long timeoutMillis = 60000L;

        // 2. Create multiple StreamLoadTasks (3 tasks)
        List<StreamLoadTask> streamLoadTasks = Lists.newArrayList();
        for (int i = 1; i <= 3; i++) {
            String streamLoadLabel = "stream_load_label_" + i;
            StreamLoadTask streamLoadTask = masterStreamLoadMgr.createLoadTaskWithoutLock(
                    database, olapTable, streamLoadLabel, user, clientIp, timeoutMillis, false,
                    WarehouseManager.DEFAULT_RESOURCE);
            masterStreamLoadMgr.addLoadTask(streamLoadTask, false);
            streamLoadTasks.add(streamLoadTask);
        }

        // 3. Create multiple StreamLoadMultiStmtTasks (2 tasks)
        List<StreamLoadMultiStmtTask> multiStmtTasks = Lists.newArrayList();
        for (int i = 1; i <= 2; i++) {
            String multiStmtLabel = "multi_stmt_label_" + i;
            StreamLoadMultiStmtTask multiStmtTask = masterStreamLoadMgr.createMultiStatementLoadTask(
                    database, multiStmtLabel, user, clientIp, timeoutMillis, WarehouseManager.DEFAULT_RESOURCE);
            masterStreamLoadMgr.addLoadTask(multiStmtTask, false);
            multiStmtTasks.add(multiStmtTask);
        }

        // 4. Create MergeCommitTask using mock
        String mergeCommitLabel = "merge_commit_label";
        MergeCommitTask mergeCommitTask = mock(MergeCommitTask.class);
        when(mergeCommitTask.getId()).thenReturn(GlobalStateMgr.getCurrentState().getNextId());
        when(mergeCommitTask.getLabel()).thenReturn(mergeCommitLabel);
        when(mergeCommitTask.getDBId()).thenReturn(dbId);
        when(mergeCommitTask.getDBName()).thenReturn(dbName);
        when(mergeCommitTask.checkNeedRemove(anyLong(), anyBoolean())).thenReturn(false);
        doNothing().when(mergeCommitTask).init();
        masterStreamLoadMgr.addLoadTask(mergeCommitTask, false);

        // 5. Verify all tasks are in the manager
        // 3 StreamLoadTasks + 2 StreamLoadMultiStmtTasks + 1 MergeCommitTask = 6 tasks
        Assertions.assertEquals(6, masterStreamLoadMgr.getStreamLoadTaskCount());
        for (int i = 1; i <= 3; i++) {
            String streamLoadLabel = "stream_load_label_" + i;
            AbstractStreamLoadTask task = masterStreamLoadMgr.getTaskByLabel(streamLoadLabel);
            Assertions.assertNotNull(task);
            Assertions.assertInstanceOf(StreamLoadTask.class, task);
        }
        for (int i = 1; i <= 2; i++) {
            String multiStmtLabel = "multi_stmt_label_" + i;
            AbstractStreamLoadTask task = masterStreamLoadMgr.getTaskByLabel(multiStmtLabel);
            Assertions.assertNotNull(task);
            Assertions.assertInstanceOf(StreamLoadMultiStmtTask.class, task);
        }
        Assertions.assertNotNull(masterStreamLoadMgr.getTaskByLabel(mergeCommitLabel));
        Assertions.assertInstanceOf(MergeCommitTask.class, masterStreamLoadMgr.getTaskByLabel(mergeCommitLabel));

        // 6. Save to image
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        masterStreamLoadMgr.save(image.getImageWriter());

        // 7. Create new manager and load from image
        StreamLoadMgr loadedStreamLoadMgr = new StreamLoadMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        loadedStreamLoadMgr.load(reader);
        reader.close();

        // 8. Verify loaded state
        // Only StreamLoadTask and StreamLoadMultiStmtTask should be loaded (MergeCommitTask is not in PERSISTENT_TASK_TYPES)
        // 3 StreamLoadTasks + 2 StreamLoadMultiStmtTasks = 5 tasks
        Assertions.assertEquals(5, loadedStreamLoadMgr.getStreamLoadTaskCount());
        
        // Verify all StreamLoadTasks are loaded
        for (int i = 0; i < streamLoadTasks.size(); i++) {
            StreamLoadTask originalTask = streamLoadTasks.get(i);
            String streamLoadLabel = "stream_load_label_" + (i + 1);
            AbstractStreamLoadTask loadedTask = loadedStreamLoadMgr.getTaskByLabel(streamLoadLabel);
            Assertions.assertNotNull(loadedTask, "StreamLoadTask " + streamLoadLabel + " should be loaded");
            Assertions.assertInstanceOf(StreamLoadTask.class, loadedTask);
            Assertions.assertEquals(streamLoadLabel, loadedTask.getLabel());
            Assertions.assertEquals(dbId, loadedTask.getDBId());
            Assertions.assertEquals(originalTask.getId(), loadedTask.getId());
        }

        // Verify all StreamLoadMultiStmtTasks are loaded
        for (int i = 0; i < multiStmtTasks.size(); i++) {
            StreamLoadMultiStmtTask originalTask = multiStmtTasks.get(i);
            String multiStmtLabel = "multi_stmt_label_" + (i + 1);
            AbstractStreamLoadTask loadedTask = loadedStreamLoadMgr.getTaskByLabel(multiStmtLabel);
            Assertions.assertNotNull(loadedTask, "StreamLoadMultiStmtTask " + multiStmtLabel + " should be loaded");
            Assertions.assertInstanceOf(StreamLoadMultiStmtTask.class, loadedTask);
            Assertions.assertEquals(multiStmtLabel, loadedTask.getLabel());
            Assertions.assertEquals(dbId, loadedTask.getDBId());
            Assertions.assertEquals(originalTask.getId(), loadedTask.getId());
        }

        // Verify MergeCommitTask is NOT loaded (it's not in PERSISTENT_TASK_TYPES)
        AbstractStreamLoadTask loadedMergeCommitTask = loadedStreamLoadMgr.getTaskByLabel(mergeCommitLabel);
        Assertions.assertNull(loadedMergeCommitTask, "MergeCommitTask should not be loaded");
    }

    @Test
    public void testImageBackwardCompatibilityAfterSupportMergeCommitTask() throws Exception {
        String path = Objects.requireNonNull(getClass().getClassLoader().getResource(
                "persist/StreamLoadMgrPersistTest_testImageBackwardCompatibilityAfterSupportMergeCommitTask.image")).getPath();
        StreamLoadMgr loadedStreamLoadMgr = new StreamLoadMgr();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(path))) {
            SRMetaBlockReader reader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(dis)));
            loadedStreamLoadMgr.load(reader);
            reader.close();
        }

        // StreamLoadTasks + 2 StreamLoadMultiStmtTasks = 5 tasks
        Assertions.assertEquals(5, loadedStreamLoadMgr.getStreamLoadTaskCount());
        final long expectedDbId = 10005L;
        final String expectedDbName = "test_db_save_load";
        final String expectedTableName = "test_table_save_load";

        // Verify all StreamLoadTasks are loaded
        for (int i = 1; i <= 3; i++) {
            String streamLoadLabel = "stream_load_label_" + i;
            AbstractStreamLoadTask loadedTask = loadedStreamLoadMgr.getTaskByLabel(streamLoadLabel);
            Assertions.assertNotNull(loadedTask);
            Assertions.assertInstanceOf(StreamLoadTask.class, loadedTask);
            Assertions.assertEquals(streamLoadLabel, loadedTask.getLabel());
            Assertions.assertEquals(expectedDbId, loadedTask.getDBId());
            Assertions.assertEquals(10000 + i, loadedTask.getId());
            Assertions.assertEquals(expectedDbName, loadedTask.getDBName());
            Assertions.assertEquals(expectedTableName, loadedTask.getTableName());
            Assertions.assertNotNull(((StreamLoadTask) loadedTask).getTUniqueId());
        }

        // Verify all StreamLoadMultiStmtTasks are loaded
        for (int i = 1; i <= 2; i++) {
            String multiStmtLabel = "multi_stmt_label_" + i;
            AbstractStreamLoadTask loadedTask = loadedStreamLoadMgr.getTaskByLabel(multiStmtLabel);
            Assertions.assertNotNull(loadedTask);
            Assertions.assertInstanceOf(StreamLoadMultiStmtTask.class, loadedTask);
            Assertions.assertEquals(multiStmtLabel, loadedTask.getLabel());
            Assertions.assertEquals(expectedDbId, loadedTask.getDBId());
            Assertions.assertEquals(10003 + i, loadedTask.getId());
            Assertions.assertEquals(expectedDbName, loadedTask.getDBName());
        }
    }
}

