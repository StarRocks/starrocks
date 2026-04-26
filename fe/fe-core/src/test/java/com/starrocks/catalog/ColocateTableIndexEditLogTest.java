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

package com.starrocks.catalog;

import com.starrocks.http.ActionController;
import com.starrocks.http.meta.ColocateMetaService;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;
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

public class ColocateTableIndexEditLogTest {
    private static final String DB_NAME = "test_colocate_index_editlog";
    private static final long DB_ID = 65001L;
    private static final long TABLE_ID = 65002L;
    private static final long PARTITION_ID = 65003L;
    private static final long PHYSICAL_PARTITION_ID = 65004L;
    private static final long INDEX_ID = 65005L;
    private static final long TABLET_ID = 65006L;
    private static final long BACKEND_ID = 65007L;
    private static final long BACKEND_ID_2 = 65008L;
    private static final long REPLICA_ID = 65009L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();

        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);

        Backend backend1 = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend1.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend1);
        Backend backend2 = new Backend(BACKEND_ID_2, "127.0.0.2", 9050);
        backend2.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testUpdateBackendPerBucketSeqAndMarkUnstableNormalCase() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        OlapTable table = createHashOlapTable(GlobalStateMgr.getCurrentState().getNextId(), "tbl_colocate");
        db.registerTableUnlocked(table);

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId =
                colocateIndex.addTableToGroup(db.getId(), table, "g1_normal", null, false);
        colocateIndex.addBackendsPerBucketSeq(groupId, List.of(List.of(BACKEND_ID)));
        Assertions.assertFalse(colocateIndex.isGroupUnstable(groupId));

        ColocateMetaService.BucketSeqAction action = new ColocateMetaService.BucketSeqAction(new ActionController());
        List<List<Long>> newBackendsPerBucketSeq = List.of(List.of(BACKEND_ID_2));
        action.updateBackendPerBucketSeq(groupId, newBackendsPerBucketSeq);

        Assertions.assertEquals(newBackendsPerBucketSeq, colocateIndex.getBackendsPerBucketSeq(groupId));
        Assertions.assertTrue(colocateIndex.isGroupUnstable(groupId));

        ColocatePersistInfo replayBackends = (ColocatePersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ_V2);
        ColocatePersistInfo replayUnstable = (ColocatePersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_COLOCATE_MARK_UNSTABLE_V2);
        Assertions.assertNotNull(replayBackends);
        Assertions.assertNotNull(replayUnstable);

        ColocateTableIndex followerIndex = new ColocateTableIndex();
        followerIndex.addTableToGroup(db.getId(), table, "g1_normal", groupId, true);
        followerIndex.addBackendsPerBucketSeq(groupId, List.of(List.of(BACKEND_ID)));
        followerIndex.replayAddBackendsPerBucketSeq(replayBackends);
        followerIndex.replayMarkGroupUnstable(replayUnstable);

        Assertions.assertEquals(newBackendsPerBucketSeq, followerIndex.getBackendsPerBucketSeq(groupId));
        Assertions.assertTrue(followerIndex.isGroupUnstable(groupId));
    }

    @Test
    public void testUpdateBackendPerBucketSeqEditLogException() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        OlapTable table = createHashOlapTable(GlobalStateMgr.getCurrentState().getNextId(), "tbl_colocate_error");
        db.registerTableUnlocked(table);

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId =
                colocateIndex.addTableToGroup(db.getId(), table, "g1_error", null, false);
        List<List<Long>> originalBackendsPerBucketSeq = List.of(List.of(BACKEND_ID));
        colocateIndex.addBackendsPerBucketSeq(groupId, originalBackendsPerBucketSeq);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logColocateBackendsPerBucketSeq(any(ColocatePersistInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            ColocateMetaService.BucketSeqAction action = new ColocateMetaService.BucketSeqAction(new ActionController());
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> action.updateBackendPerBucketSeq(groupId, List.of(List.of(BACKEND_ID_2))));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(originalBackendsPerBucketSeq, colocateIndex.getBackendsPerBucketSeq(groupId));
            Assertions.assertFalse(colocateIndex.isGroupUnstable(groupId));
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testMarkGroupUnstableEditLogException() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        OlapTable table =
                createHashOlapTable(GlobalStateMgr.getCurrentState().getNextId(), "tbl_colocate_unstable_error");
        db.registerTableUnlocked(table);

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId =
                colocateIndex.addTableToGroup(db.getId(), table, "g1_unstable_error", null, false);
        colocateIndex.markGroupStable(groupId, false);
        Assertions.assertFalse(colocateIndex.isGroupUnstable(groupId));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logColocateMarkUnstable(any(ColocatePersistInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> colocateIndex.markGroupUnstable(groupId, true));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertFalse(colocateIndex.isGroupUnstable(groupId));
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testMarkGroupStableNormalAndEditLogException() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        OlapTable table = createHashOlapTable(GlobalStateMgr.getCurrentState().getNextId(), "tbl_colocate_stable");
        db.registerTableUnlocked(table);

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId =
                colocateIndex.addTableToGroup(db.getId(), table, "g1_stable", null, false);
        colocateIndex.markGroupUnstable(groupId, false);
        Assertions.assertTrue(colocateIndex.isGroupUnstable(groupId));

        colocateIndex.markGroupStable(groupId, true);
        Assertions.assertFalse(colocateIndex.isGroupUnstable(groupId));

        ColocatePersistInfo replayInfo = (ColocatePersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_COLOCATE_MARK_STABLE_V2);
        Assertions.assertNotNull(replayInfo);

        ColocateTableIndex followerIndex = new ColocateTableIndex();
        followerIndex.addTableToGroup(db.getId(), table, "g1_stable", groupId, true);
        followerIndex.markGroupUnstable(groupId, false);
        Assertions.assertTrue(followerIndex.isGroupUnstable(groupId));
        followerIndex.replayMarkGroupStable(replayInfo);
        Assertions.assertFalse(followerIndex.isGroupUnstable(groupId));

        colocateIndex.markGroupUnstable(groupId, false);
        Assertions.assertTrue(colocateIndex.isGroupUnstable(groupId));
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logColocateMarkStable(any(ColocatePersistInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> colocateIndex.markGroupStable(groupId, true));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertTrue(colocateIndex.isGroupUnstable(groupId));
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    private static OlapTable createHashOlapTable(long tableId, String tableName) {
        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", IntegerType.BIGINT);
        k1.setIsKey(true);
        columns.add(k1);
        columns.add(new Column("v1", IntegerType.BIGINT));

        DistributionInfo distributionInfo = new HashDistributionInfo(1, List.of(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);
        tablet.addReplica(new Replica(REPLICA_ID, BACKEND_ID, Replica.ReplicaState.NORMAL, 1, 0), false);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);
        OlapTable table = new OlapTable(tableId, tableName, columns, com.starrocks.sql.ast.KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        table.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN,
                com.starrocks.sql.ast.KeysType.DUP_KEYS);
        table.setBaseIndexMetaId(INDEX_ID);
        table.addPartition(partition);
        table.setTableProperty(new TableProperty(new HashMap<>()));
        return table;
    }
}
