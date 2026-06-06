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
import com.starrocks.catalog.MockedLocalMetaStore;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.View;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.MockedMetadataMgr;
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

public class AlterJobMgrEditLogTest {
    private static final long SWAP_TABLE_ID_1 = 91001L;
    private static final long SWAP_TABLE_ID_2 = 91002L;
    private static final long SWAP_PARTITION_ID_1 = 92001L;
    private static final long SWAP_PARTITION_ID_2 = 92002L;
    private static final long SWAP_PHYSICAL_PARTITION_ID_1 = 93001L;
    private static final long SWAP_PHYSICAL_PARTITION_ID_2 = 93002L;
    private static final long SWAP_INDEX_ID_1 = 94001L;
    private static final long SWAP_INDEX_ID_2 = 94002L;
    private static final long SWAP_TABLET_ID_1 = 95001L;
    private static final long SWAP_TABLET_ID_2 = 95002L;

    private AlterJobMgr alterJobMgr;
    private MockedLocalMetaStore localMetastore;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        globalStateMgr.setLocalMetastore(localMetastore);

        MockedMetadataMgr mockedMetadataMgr = new MockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(mockedMetadataMgr);

        alterJobMgr = globalStateMgr.getAlterJobMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSetViewSecurityNormalCase() throws Exception {
        // 1. Prepare test data - create a database and view
        localMetastore.createDb("test_db");
        Database db = localMetastore.getDb("test_db");
        Assertions.assertNotNull(db);

        // Create a simple view
        List<Column> columns = new ArrayList<>();
        View view = new View(1L, "test_view", columns);
        db.registerTableUnlocked(view);
        
        Table table = localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.isView());
        View testView = (View) table;
        Assertions.assertFalse(testView.isSecurity());

        // 2. Prepare AlterViewInfo
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), true);

        // 3. Execute setViewSecurity operation (master side)
        alterJobMgr.setViewSecurity(alterViewInfo);

        // 4. Verify master state
        View updatedView = (View) localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(updatedView);
        Assertions.assertTrue(updatedView.isSecurity());

        // 5. Test follower replay functionality
        // Use the same alterJobMgr for replay since we're testing the replay method
        AlterJobMgr followerAlterJobMgr = alterJobMgr;
        
        // Create follower view with initial state
        List<Column> followerColumns = new ArrayList<>();
        View followerView = new View(view.getId(), "test_view", followerColumns);
        followerView.setSecurity(false);
        db.registerTableUnlocked(followerView);
        
        Assertions.assertFalse(followerView.isSecurity());

        // Replay the operation
        AlterViewInfo replayInfo = (AlterViewInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SET_VIEW_SECURITY_LOG);
        followerAlterJobMgr.updateViewSecurity(replayInfo);

        // 6. Verify follower state is consistent with master
        View followerUpdatedView = (View) localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(followerUpdatedView);
        Assertions.assertTrue(followerUpdatedView.isSecurity());
    }

    @Test
    public void testSetViewSecurityEditLogException() throws Exception {
        // 1. Prepare test data - create a database and view
        localMetastore.createDb("test_db");
        Database db = localMetastore.getDb("test_db");
        Assertions.assertNotNull(db);

        // Create a simple view
        List<Column> columns = new ArrayList<>();
        View view = new View(1L, "test_view", columns);
        db.registerTableUnlocked(view);
        
        Table table = localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.isView());
        View testView = (View) table;
        Assertions.assertFalse(testView.isSecurity());

        // 2. Prepare AlterViewInfo
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), true);

        // 3. Mock EditLog.logJsonObject to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logJsonObject(any(Short.class), any(), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute setViewSecurity operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            alterJobMgr.setViewSecurity(alterViewInfo);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify view security remains unchanged after exception
        Assertions.assertNotNull(exception);
        Assertions.assertFalse(testView.isSecurity());
    }

    @Test
    public void testAlterViewNormalCase() throws Exception {
        // 1. Prepare test data - create a database and view
        localMetastore.createDb("test_db");
        Database db = localMetastore.getDb("test_db");
        Assertions.assertNotNull(db);

        // Create a simple view
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("c1", com.starrocks.type.IntegerType.INT);
        columns.add(col1);
        View view = new View(1L, "test_view", columns);
        view.setInlineViewDefWithSqlMode("SELECT 1 as c1", 0L);
        db.registerTableUnlocked(view);

        Table table = localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.isView());
        View testView = (View) table;
        String originalViewDef = testView.getInlineViewDef();
        Assertions.assertEquals("SELECT 1 as c1", originalViewDef);

        // 2. Prepare AlterViewInfo with new view definition
        String newViewDef = "SELECT 2 as c1";
        List<Column> newColumns = new ArrayList<>();
        Column newCol1 = new Column("c1", com.starrocks.type.IntegerType.INT);
        newColumns.add(newCol1);
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), newViewDef, newColumns, 0L, "test comment",
                newViewDef);

        // 3. Execute alterView operation (master side)
        alterJobMgr.alterView(alterViewInfo);

        // 4. Verify master state
        View updatedView = (View) localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(updatedView);
        Assertions.assertEquals(newViewDef, updatedView.getInlineViewDef());
        Assertions.assertEquals("test comment", updatedView.getComment());
        Assertions.assertEquals(newColumns.size(), updatedView.getFullSchema().size());
        Assertions.assertEquals(newColumns.get(0).getName(), updatedView.getFullSchema().get(0).getName());

        // 5. Test follower replay functionality
        AlterViewInfo replayInfo = (AlterViewInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_VIEW_DEF);

        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(view.getId(), replayInfo.getTableId());
        Assertions.assertEquals(newViewDef, replayInfo.getInlineViewDef());
        Assertions.assertEquals("test comment", replayInfo.getComment());
        Assertions.assertEquals(newColumns.size(), replayInfo.getNewFullSchema().size());

        // Create follower metastore and the same id objects, then replay into it
        MockedLocalMetaStore followerMetastore = new MockedLocalMetaStore(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getRecycleBin(), null);
        Database followerDb = new Database(db.getId(), "test_db");
        followerMetastore.unprotectCreateDb(followerDb);

        // Create view with same ID
        List<Column> followerColumns = new ArrayList<>();
        Column followerCol1 = new Column("c1", com.starrocks.type.IntegerType.INT);
        followerColumns.add(followerCol1);
        View followerView = new View(view.getId(), "test_view", followerColumns);
        followerView.setInlineViewDefWithSqlMode(originalViewDef, 0L);
        followerDb.registerTableUnlocked(followerView);

        // Create follower AlterJobMgr
        AlterJobMgr followerAlterJobMgr = new AlterJobMgr(
                GlobalStateMgr.getCurrentState().getSchemaChangeHandler(),
                GlobalStateMgr.getCurrentState().getAlterJobMgr().getMaterializedViewHandler(),
                GlobalStateMgr.getCurrentState().getAlterJobMgr().getClusterHandler());
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);

        followerAlterJobMgr.replayAlterView(replayInfo);

        // 6. Verify follower state
        View replayedView = (View) followerDb.getTable(view.getId());
        Assertions.assertNotNull(replayedView);
        Assertions.assertEquals(newViewDef, replayedView.getInlineViewDef());
        Assertions.assertEquals("test comment", replayedView.getComment());
        Assertions.assertEquals(newColumns.size(), replayedView.getFullSchema().size());
        Assertions.assertEquals(newColumns.get(0).getName(), replayedView.getFullSchema().get(0).getName());
    }

    @Test
    public void testAlterViewEditLogException() throws Exception {
        // 1. Prepare test data - create a database and view
        localMetastore.createDb("test_db");
        Database db = localMetastore.getDb("test_db");
        Assertions.assertNotNull(db);

        // Create a simple view
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("c1", com.starrocks.type.IntegerType.INT);
        columns.add(col1);
        View view = new View(1L, "test_view", columns);
        view.setInlineViewDefWithSqlMode("SELECT 1 as c1", 0L);
        db.registerTableUnlocked(view);

        Table table = localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(table);
        View testView = (View) table;
        String originalViewDef = testView.getInlineViewDef();

        // 2. Prepare AlterViewInfo
        String newViewDef = "SELECT 2 as c1";
        List<Column> newColumns = new ArrayList<>();
        Column newCol1 = new Column("c1", com.starrocks.type.IntegerType.INT);
        newColumns.add(newCol1);
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), newViewDef, newColumns, 0L, "test comment",
                newViewDef);

        // 3. Mock EditLog.logModifyViewDef to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyViewDef(any(AlterViewInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterView operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            alterJobMgr.alterView(alterViewInfo);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify view definition remains unchanged after exception
        View unchangedView = (View) localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(unchangedView);
        Assertions.assertEquals(originalViewDef, unchangedView.getInlineViewDef());
    }

    @Test
    public void testSwapTableNormalCase() throws Exception {
        localMetastore.createDb("swap_db");
        Database db = localMetastore.getDb("swap_db");
        Assertions.assertNotNull(db);

        OlapTable originTable = createHashOlapTable(SWAP_TABLE_ID_1, "swap_table_a",
                SWAP_PARTITION_ID_1, SWAP_PHYSICAL_PARTITION_ID_1, SWAP_INDEX_ID_1, SWAP_TABLET_ID_1);
        OlapTable newTable = createHashOlapTable(SWAP_TABLE_ID_2, "swap_table_b",
                SWAP_PARTITION_ID_2, SWAP_PHYSICAL_PARTITION_ID_2, SWAP_INDEX_ID_2, SWAP_TABLET_ID_2);
        db.registerTableUnlocked(originTable);
        db.registerTableUnlocked(newTable);

        SwapTableOperationLog log = new SwapTableOperationLog(db.getId(), originTable.getId(), newTable.getId());
        GlobalStateMgr.getCurrentState().getEditLog().logSwapTable(log, wal -> alterJobMgr.swapTableInternal(log));

        Assertions.assertEquals(newTable.getId(), db.getTable("swap_table_a").getId());
        Assertions.assertEquals(originTable.getId(), db.getTable("swap_table_b").getId());

        SwapTableOperationLog replayInfo = (SwapTableOperationLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SWAP_TABLE);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(originTable.getId(), replayInfo.getOrigTblId());
        Assertions.assertEquals(newTable.getId(), replayInfo.getNewTblId());

        MockedLocalMetaStore followerMetastore =
                new MockedLocalMetaStore(
                        GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getRecycleBin(), null);
        Database followerDb = new Database(db.getId(), "swap_db");
        followerMetastore.unprotectCreateDb(followerDb);
        followerDb.registerTableUnlocked(createHashOlapTable(SWAP_TABLE_ID_1, "swap_table_a",
                SWAP_PARTITION_ID_1, SWAP_PHYSICAL_PARTITION_ID_1, SWAP_INDEX_ID_1, SWAP_TABLET_ID_1));
        followerDb.registerTableUnlocked(createHashOlapTable(SWAP_TABLE_ID_2, "swap_table_b",
                SWAP_PARTITION_ID_2, SWAP_PHYSICAL_PARTITION_ID_2, SWAP_INDEX_ID_2, SWAP_TABLET_ID_2));

        MockedLocalMetaStore originalMetastore = localMetastore;
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            alterJobMgr.replaySwapTable(replayInfo);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(originalMetastore);
        }

        Assertions.assertEquals(SWAP_TABLE_ID_2, followerDb.getTable("swap_table_a").getId());
        Assertions.assertEquals(SWAP_TABLE_ID_1, followerDb.getTable("swap_table_b").getId());
    }

    @Test
    public void testSwapTableEditLogException() throws Exception {
        localMetastore.createDb("swap_db_error");
        Database db = localMetastore.getDb("swap_db_error");
        Assertions.assertNotNull(db);

        OlapTable originTable = createHashOlapTable(SWAP_TABLE_ID_1 + 100, "swap_table_x",
                SWAP_PARTITION_ID_1 + 100, SWAP_PHYSICAL_PARTITION_ID_1 + 100, SWAP_INDEX_ID_1 + 100,
                SWAP_TABLET_ID_1 + 100);
        OlapTable newTable = createHashOlapTable(SWAP_TABLE_ID_2 + 100, "swap_table_y",
                SWAP_PARTITION_ID_2 + 100, SWAP_PHYSICAL_PARTITION_ID_2 + 100, SWAP_INDEX_ID_2 + 100,
                SWAP_TABLET_ID_2 + 100);
        db.registerTableUnlocked(originTable);
        db.registerTableUnlocked(newTable);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logSwapTable(any(SwapTableOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            SwapTableOperationLog log = new SwapTableOperationLog(db.getId(), originTable.getId(), newTable.getId());
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () ->
                    GlobalStateMgr.getCurrentState().getEditLog().logSwapTable(log, wal -> alterJobMgr.swapTableInternal(log)));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(originTable.getId(), db.getTable("swap_table_x").getId());
            Assertions.assertEquals(newTable.getId(), db.getTable("swap_table_y").getId());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    private static OlapTable createHashOlapTable(long tableId, String tableName, long partitionId,
                                                 long physicalPartitionId, long indexId, long tabletId) {
        List<Column> columns = new ArrayList<>();
        Column keyColumn = new Column("k1", IntegerType.BIGINT);
        keyColumn.setIsKey(true);
        columns.add(keyColumn);
        columns.add(new Column("v1", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(1, List.of(keyColumn));
        MaterializedIndex baseIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        TabletMeta tabletMeta = new TabletMeta(1L, tableId, partitionId, indexId, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);
        Partition partition = new Partition(partitionId, physicalPartitionId, tableName, baseIndex, distributionInfo);

        OlapTable table = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        table.setIndexMeta(indexId, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexMetaId(indexId);
        table.addPartition(partition);
        table.setTableProperty(new TableProperty(new HashMap<>()));
        return table;
    }
}
