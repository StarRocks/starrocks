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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/CatalogTestUtil.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.common.exception.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class GlobalStateMgrTestUtil {

    public static String testDb1 = "testDb1";
    public static long testDbId1 = 1;
    public static String testTable1 = "testTable1";
    public static long testTableId1 = 2;
    public static String testPartition1 = "testPartition1";
    public static long testPartitionId1 = 3;
    public static String testIndex1 = "testIndex1";
    public static long testIndexId1 = 2; // the base indexid == tableid
    public static int testSchemaHash1 = 93423942;
    public static long testBackendId1 = 5;
    public static long testBackendId2 = 6;
    public static long testBackendId3 = 7;
    public static long testReplicaId1 = 8;
    public static long testReplicaId2 = 9;
    public static long testReplicaId3 = 10;
    public static long testTabletId1 = 11;
    public static long testStartVersion = 12;
    public static String testRollupIndex2 = "newRollupIndex";
    public static String testRollupIndex3 = "newRollupIndex2";
    public static String testTxnLable1 = "testTxnLable1";
    public static String testTxnLable2 = "testTxnLable2";
    public static String testTxnLable3 = "testTxnLable3";
    public static String testTxnLable4 = "testTxnLable4";
    public static String testTxnLable5 = "testTxnLable5";
    public static String testTxnLable6 = "testTxnLable6";
    public static String testTxnLable7 = "testTxnLable7";
    public static String testTxnLable8 = "testTxnLable8";
    public static String testTxnLable9 = "testTxnLable9";
    public static String testEsTable1 = "partitionedEsTable1";
    public static long testEsTableId1 = 14;

    public static GlobalStateMgr createTestState() throws InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        Constructor<GlobalStateMgr> constructor = GlobalStateMgr.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        GlobalStateMgr globalStateMgr = constructor.newInstance();
        globalStateMgr.setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        GlobalStateMgr.getCurrentSystemInfo().clear();
        globalStateMgr.initDefaultCluster();

        Backend backend1 = createBackend(testBackendId1, "host1", 123, 124, 125);
        Backend backend2 = createBackend(testBackendId2, "host2", 123, 124, 125);
        Backend backend3 = createBackend(testBackendId3, "host3", 123, 124, 125);
        GlobalStateMgr.getCurrentSystemInfo().addBackend(backend1);
        GlobalStateMgr.getCurrentSystemInfo().addBackend(backend2);
        GlobalStateMgr.getCurrentSystemInfo().addBackend(backend3);
        Database db = createSimpleDb(testDbId1, testTableId1, testPartitionId1, testIndexId1, testTabletId1,
                testStartVersion);
        LocalMetastore metastore = (LocalMetastore) globalStateMgr.getMetadata();
        metastore.unprotectCreateDb(db);
        return globalStateMgr;
    }

    public static boolean compareState(GlobalStateMgr masterGlobalStateMgr, GlobalStateMgr slaveGlobalStateMgr) {
        Database masterDb = masterGlobalStateMgr.getDb(testDb1);
        Database slaveDb = slaveGlobalStateMgr.getDb(testDb1);
        List<Table> tables = masterDb.getTables();
        for (Table table : tables) {
            Table slaveTable = slaveDb.getTable(table.getId());
            if (slaveTable == null) {
                return false;
            }
            Partition masterPartition = table.getPartition(testPartition1);
            Partition slavePartition = slaveTable.getPartition(testPartition1);
            if (masterPartition == null && slavePartition == null) {
                return true;
            }
            if (masterPartition.getId() != slavePartition.getId()) {
                return false;
            }
            if (masterPartition.getVisibleVersion() != slavePartition.getVisibleVersion()
                    || masterPartition.getNextVersion() != slavePartition.getNextVersion()) {
                return false;
            }
            List<MaterializedIndex> allMaterializedIndices = masterPartition.getMaterializedIndices(IndexExtState.ALL);
            for (MaterializedIndex masterIndex : allMaterializedIndices) {
                MaterializedIndex slaveIndex = slavePartition.getIndex(masterIndex.getId());
                if (slaveIndex == null) {
                    return false;
                }
                List<Tablet> allTablets = masterIndex.getTablets();
                for (Tablet masterTablet : allTablets) {
                    Tablet slaveTablet = slaveIndex.getTablet(masterTablet.getId());
                    if (slaveTablet == null) {
                        return false;
                    }
                    List<Replica> allReplicas = ((LocalTablet) masterTablet).getImmutableReplicas();
                    for (Replica masterReplica : allReplicas) {
                        Replica slaveReplica = ((LocalTablet) slaveTablet).getReplicaById(masterReplica.getId());
                        if (slaveReplica.getBackendId() != masterReplica.getBackendId()
                                || slaveReplica.getVersion() != masterReplica.getVersion()
                                || slaveReplica.getLastFailedVersion() != masterReplica.getLastFailedVersion()
                                || slaveReplica.getLastSuccessVersion() != slaveReplica.getLastSuccessVersion()) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    public static Database createSimpleDb(long dbId, long tableId, long partitionId, long indexId, long tabletId,
                                          long version) {
        GlobalStateMgr.getCurrentInvertedIndex().clear();

        // replica
        long replicaId = 0;
        Replica replica1 = new Replica(testReplicaId1, testBackendId1, version, 0, 0L, 0L,
                ReplicaState.NORMAL, -1, 0);
        Replica replica2 = new Replica(testReplicaId2, testBackendId2, version, 0, 0L, 0L,
                ReplicaState.NORMAL, -1, 0);
        Replica replica3 = new Replica(testReplicaId3, testBackendId3, version, 0, 0L, 0L,
                ReplicaState.NORMAL, -1, 0);

        // tablet
        LocalTablet tablet = new LocalTablet(tabletId);

        // index
        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // partition
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(partitionId, testPartition1, index, distributionInfo);
        partition.updateVisibleVersion(testStartVersion);
        partition.setNextVersion(testStartVersion + 1);

        // columns
        List<Column> columns = new ArrayList<Column>();
        Column temp = new Column("k1", Type.INT);
        temp.setIsKey(true);
        columns.add(temp);
        temp = new Column("k2", Type.INT);
        temp.setIsKey(true);
        columns.add(temp);
        columns.add(new Column("v", Type.DOUBLE, false, AggregateType.SUM, "0", ""));

        List<Column> keysColumn = new ArrayList<Column>();
        temp = new Column("k1", Type.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);
        temp = new Column("k2", Type.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        OlapTable table = new OlapTable(tableId, testTable1, columns, KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(indexId, testIndex1, columns, 0, testSchemaHash1, (short) 1, TStorageType.COLUMN,
                KeysType.AGG_KEYS);
        table.setBaseIndexId(indexId);
        // db
        Database db = new Database(dbId, testDb1);
        db.registerTableUnlocked(table);

        // add a es table to globalStateMgr
        try {
            createEsTable(db);
        } catch (DdlException ignored) {
        }
        return db;
    }

    public static void createEsTable(Database db) throws DdlException {
        // columns
        List<Column> columns = new ArrayList<>();
        Column userId = new Column("userId", Type.VARCHAR);
        columns.add(userId);
        columns.add(new Column("time", Type.BIGINT));
        columns.add(new Column("type", Type.VARCHAR));

        // table
        List<Column> partitionColumns = Lists.newArrayList();
        List<SingleRangePartitionDesc> singleRangePartitionDescs = Lists.newArrayList();
        partitionColumns.add(userId);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                new PartitionKeyDesc(Lists
                        .newArrayList(new PartitionValue("100"))),
                null));

        RangePartitionInfo partitionInfo = new RangePartitionInfo(partitionColumns);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(EsTable.KEY_HOSTS, "xxx");
        properties.put(EsTable.KEY_INDEX, "doe");
        properties.put(EsTable.KEY_TYPE, "doc");
        properties.put(EsTable.KEY_PASSWORD, "");
        properties.put(EsTable.KEY_USER, "root");
        properties.put(EsTable.KEY_DOC_VALUE_SCAN, "true");
        properties.put(EsTable.KEY_KEYWORD_SNIFF, "true");
        EsTable esTable = new EsTable(testEsTableId1, testEsTable1,
                columns, properties, partitionInfo);
        db.registerTableUnlocked(esTable);
    }

    public static Backend createBackend(long id, String host, int heartPort, int bePort, int httpPort) {
        Backend backend = new Backend(id, host, heartPort);
        backend.setAlive(true);
        return backend;
    }
}
