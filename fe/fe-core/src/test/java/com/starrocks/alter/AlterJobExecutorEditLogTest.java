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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AlterTableModifyDefaultBucketsClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AlterJobExecutorEditLogTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_alter_job_executor_editlog";
    private static final String TABLE_NAME = "test_table";
    private static final long DB_ID = 10001L;
    private static final long TABLE_ID = 10002L;
    private static final long PARTITION_ID = 10003L;
    private static final long PHYSICAL_PARTITION_ID = 10004L;
    private static final long INDEX_ID = 10005L;
    private static final long TABLET_ID = 10006L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase(DB_NAME);

        // Create database and table directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        db.registerTableUnlocked(table);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testVisitAlterTableModifyDefaultBucketsClauseNormalCase() throws Exception {
        // 1. Get table and verify initial state
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);
        Assertions.assertInstanceOf(HashDistributionInfo.class, table.getDefaultDistributionInfo());
        HashDistributionInfo distInfo = (HashDistributionInfo) table.getDefaultDistributionInfo();
        int originalBucketNum = distInfo.getBucketNum();
        Assertions.assertEquals(3, originalBucketNum);

        // 2. Create AlterTableModifyDefaultBucketsClause
        int newBucketNum = 5;
        // distributionColumns should match existing distribution columns (v1)
        AlterTableModifyDefaultBucketsClause clause = new AlterTableModifyDefaultBucketsClause(
                List.of("v1"), newBucketNum, NodePosition.ZERO);
        
        // 3. Execute visitAlterTableModifyDefaultBucketsClause directly
        AlterJobExecutor executor = new AlterJobExecutor();
        executor.db = db;
        executor.table = table;
        executor.visitAlterTableModifyDefaultBucketsClause(clause, connectContext);

        // 5. Verify master state
        table = (OlapTable) db.getTable(TABLE_NAME);
        distInfo = (HashDistributionInfo) table.getDefaultDistributionInfo();
        Assertions.assertEquals(newBucketNum, distInfo.getBucketNum());

        // 6. Test follower replay functionality
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_DEFAULT_BUCKET_NUM);
        
        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(table.getId(), replayInfo.getTableId());

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_DEFAULT_BUCKET_NUM, replayInfo);

        // 7. Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID);
        Assertions.assertNotNull(replayed);
        HashDistributionInfo replayedDist = (HashDistributionInfo) replayed.getDefaultDistributionInfo();
        Assertions.assertEquals(newBucketNum, replayedDist.getBucketNum());
    }

    @Test
    public void testVisitAlterTableModifyDefaultBucketsClauseEditLogException() throws Exception {
        // 1. Get table and verify initial state
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        final OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.getDefaultDistributionInfo() instanceof HashDistributionInfo);
        HashDistributionInfo distInfo = (HashDistributionInfo) table.getDefaultDistributionInfo();
        int originalBucketNum = distInfo.getBucketNum();
        Assertions.assertEquals(3, originalBucketNum);

        // 2. Mock EditLog.logModifyDefaultBucketNum to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyDefaultBucketNum(any(ModifyTablePropertyOperationLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Create AlterTableModifyDefaultBucketsClause
        int newBucketNum = 5;
        AlterTableModifyDefaultBucketsClause clause = new AlterTableModifyDefaultBucketsClause(
                List.of("v1"), newBucketNum, NodePosition.ZERO);

        // 4. Execute visitAlterTableModifyDefaultBucketsClause and expect exception
        AlterJobExecutor executor = new AlterJobExecutor();
        final Database finalDb = db;
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            executor.db = finalDb;
            executor.table = table;
            executor.visitAlterTableModifyDefaultBucketsClause(clause, connectContext);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 6. Verify leader memory state remains unchanged after exception
        OlapTable tableAfterException = (OlapTable) db.getTable(TABLE_NAME);
        distInfo = (HashDistributionInfo) tableAfterException.getDefaultDistributionInfo();
        Assertions.assertEquals(originalBucketNum, distInfo.getBucketNum());
    }

    @Test
    public void testVisitAlterTableModifyDefaultBucketsClauseNonHashDistribution() throws Exception {
        // 1. Create a table with non-hash distribution (random distribution)
        String tableName2 = "test_table_random";
        long tableId2 = TABLE_ID + 10;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable randomTable = createRandomOlapTable(tableId2, tableName2, 3);
        db.registerTableUnlocked(randomTable);

        // 2. Get table
        OlapTable table = (OlapTable) db.getTable(tableName2);
        Assertions.assertNotNull(table);
        Assertions.assertFalse(table.getDefaultDistributionInfo() instanceof HashDistributionInfo);

        // 3. Create AlterTableModifyDefaultBucketsClause
        int newBucketNum = 5;
        // For non-hash distribution, distributionColumns can be empty
        AlterTableModifyDefaultBucketsClause clause = new AlterTableModifyDefaultBucketsClause(
                new ArrayList<>(), newBucketNum, NodePosition.ZERO);

        // 4. Execute visitAlterTableModifyDefaultBucketsClause (should not modify for non-hash)
        AlterJobExecutor executor = new AlterJobExecutor();
        executor.db = db;
        executor.table = table;
        executor.visitAlterTableModifyDefaultBucketsClause(clause, connectContext);

        // 5. Verify state remains unchanged (no exception, but no modification)
        table = (OlapTable) db.getTable(tableId2);
        Assertions.assertFalse(table.getDefaultDistributionInfo() instanceof HashDistributionInfo);
    }

    private static OlapTable createHashOlapTable(long tableId, String tableName, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static OlapTable createRandomOlapTable(long tableId, String tableName, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID + 10, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID + 10, (short) 1);

        DistributionInfo distributionInfo = new RandomDistributionInfo(bucketNum);
        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID + 10, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID + 10);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID + 10, INDEX_ID + 10, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);
        Partition partition = new Partition(PARTITION_ID + 10, PHYSICAL_PARTITION_ID + 10, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID + 10, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID + 10);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }
}

