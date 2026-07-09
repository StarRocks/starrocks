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

package com.starrocks.leader;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;
import com.starrocks.task.AgentTask;
import com.starrocks.task.StorageMediaMigrationTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class LeaderImplStorageMigrationTest {
    private static final String DB_NAME = "test_finish_storage_migration";
    private static final long DB_ID = 80001L;
    private static final long TABLE_ID = 80002L;
    private static final long PARTITION_ID = 80004L;
    private static final long PHYSICAL_PARTITION_ID = 80005L;
    private static final long INDEX_ID = 80006L;
    private static final long TABLET_ID = 80007L;
    private static final long BACKEND_ID = 80008L;
    private static final long REPLICA_ID = 80009L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);

        Backend backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static OlapTable createOlapTable(long tableId, String tableName) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns,
                com.starrocks.sql.ast.KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1,
                com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new java.util.HashMap<>()));
        return olapTable;
    }

    @Test
    public void testFinishStorageMigrationSetsPathHash() throws Exception {
        // 1. Register table and add tablet/replica to the inverted index so finishStorageMigration
        //    can resolve the db/table and locate the replica.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);

        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(2, 2, 2);
        tablet.addReplica(replica);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        long newPathHash = 123456789L;
        Assertions.assertNotEquals(newPathHash, replica.getPathHash());

        // 2. Build a successful finish request carrying the new path hash.
        TFinishTaskRequest request = new TFinishTaskRequest();
        request.setTask_status(new TStatus(TStatusCode.OK));
        TTabletInfo tabletInfo = new TTabletInfo();
        tabletInfo.setTablet_id(TABLET_ID);
        tabletInfo.setPath_hash(newPathHash);
        request.setFinish_tablet_infos(Lists.newArrayList(tabletInfo));

        StorageMediaMigrationTask task =
                new StorageMediaMigrationTask(BACKEND_ID, TABLET_ID, 0, TStorageMedium.HDD, false);

        // 3. Invoke the private finishStorageMigration: it takes the table-scoped WRITE lock and
        //    sets the replica path hash.
        LeaderImpl leader = new LeaderImpl();
        Method method = LeaderImpl.class.getDeclaredMethod("finishStorageMigration",
                AgentTask.class, TFinishTaskRequest.class);
        method.setAccessible(true);
        method.invoke(leader, task, request);

        Assertions.assertEquals(newPathHash, replica.getPathHash());
    }
}
