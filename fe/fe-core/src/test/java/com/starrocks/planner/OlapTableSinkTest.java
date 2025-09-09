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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.system.Backend;
import com.starrocks.system.BackendHbResponse;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TOlapTableLocationParam;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class OlapTableSinkTest {
    private static final Logger LOG = LogManager.getLogger(OlapTableSinkTest.class);

    @Injectable
    public OlapTable dstTable;

    private TupleDescriptor getTuple() {
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tuple = descTable.createTupleDescriptor("DstTable");
        // k1
        SlotDescriptor k1 = descTable.addSlotDescriptor(tuple);
        k1.setColumn(new Column("k1", Type.BIGINT));
        k1.setIsMaterialized(true);

        // k2
        SlotDescriptor k2 = descTable.addSlotDescriptor(tuple);
        k2.setColumn(new Column("k2", ScalarType.createVarchar(25)));
        k2.setIsMaterialized(true);
        // v1
        SlotDescriptor v1 = descTable.addSlotDescriptor(tuple);
        v1.setColumn(new Column("v1", ScalarType.createVarchar(25)));
        v1.setIsMaterialized(true);
        // v2
        SlotDescriptor v2 = descTable.addSlotDescriptor(tuple);
        v2.setColumn(new Column("v2", Type.BIGINT));
        v2.setIsMaterialized(true);

        return tuple;
    }

    @BeforeEach
    public void before() {
        UtFrameUtils.mockInitWarehouseEnv();
    }

    @Test
    public void testSinglePartition() throws StarRocksException {
        TupleDescriptor tuple = getTuple();
        SinglePartitionInfo partInfo = new SinglePartitionInfo();
        partInfo.setReplicationNum(2, (short) 3);
        MaterializedIndex index = new MaterializedIndex(2, MaterializedIndex.IndexState.NORMAL);
        HashDistributionInfo distInfo = new HashDistributionInfo(
                2, Lists.newArrayList(new Column("k1", Type.BIGINT)));
        Partition partition = new Partition(2, 22, "p1", index, distInfo);

        new Expectations() {
            {
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                dstTable.getPartitions();
                result = Lists.newArrayList(partition);
                dstTable.getPartition(2L);
                result = partition;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(2L),
                TWriteQuorumType.MAJORITY, false, false, false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000);
        sink.complete();
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }

    @Test
    public void testRangePartition(
            @Injectable RangePartitionInfo partInfo,
            @Injectable MaterializedIndex index) throws StarRocksException {
        TupleDescriptor tuple = getTuple();

        HashDistributionInfo distInfo = new HashDistributionInfo(
                2, Lists.newArrayList(new Column("k1", Type.BIGINT)));

        Column partKey = new Column("k2", Type.VARCHAR);
        PartitionKey key = PartitionKey
                .createPartitionKey(Lists.newArrayList(new PartitionValue("123")), Lists.newArrayList(partKey));
        Partition p1 = new Partition(1, 21, "p1", index, distInfo);
        Partition p2 = new Partition(2, 22, "p2", index, distInfo);

        new Expectations() {
            {
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                partInfo.getType();
                result = PartitionType.RANGE;
                partInfo.getPartitionColumns((Map<ColumnId, Column>) any);
                result = Lists.newArrayList(partKey);
                dstTable.getPartitions();
                result = Lists.newArrayList(p1, p2);
                dstTable.getPartition(p1.getId());
                result = p1;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(p1.getId()),
                TWriteQuorumType.MAJORITY, false, false, false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000);
        try {
            sink.complete();
        } catch (StarRocksException e) {

        }
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }

    @Test
    public void testRangeUnknownPartition(
            @Injectable RangePartitionInfo partInfo,
            @Injectable MaterializedIndex index) {
        assertThrows(StarRocksException.class, () -> {
            TupleDescriptor tuple = getTuple();

            long unknownPartId = 12345L;
            new Expectations() {
                {
                    dstTable.getPartition(unknownPartId);
                    result = null;
                }
            };

            OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(unknownPartId),
                    TWriteQuorumType.MAJORITY, false, false, false);
            sink.init(new TUniqueId(1, 2), 3, 4, 1000);
            sink.complete();
            LOG.info("sink is {}", sink.toThrift());
            LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
        });
    }

    @Test
    public void testCreateLocationWithLocalTablet(@Mocked GlobalStateMgr globalStateMgr,
                                                  @Mocked SystemInfoService systemInfoService) throws Exception {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long physicalPartitionId = 6L;
        long replicaId = 10L;
        long backendId = 20L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Replica
        Replica replica1 = new Replica(replicaId, backendId, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica3 = new Replica(replicaId + 2, backendId + 2, Replica.ReplicaState.NORMAL, 1, 0);

        // Tablet
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.SSD));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, indexId, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                result = Status.OK;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.checkBackendAlive(anyLong);
                result = true;
            }
        };

        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        TOlapTablePartition tPartition = new TOlapTablePartition();
        tPartition.setId(physicalPartitionId);
        partitionParam.addToPartitions(tPartition);
        TOlapTableLocationParam param = OlapTableSink.createLocation(
                table, partitionParam, false);
        System.out.println(param);

        // Check
        List<TTabletLocation> locations = param.getTablets();
        Assertions.assertEquals(1, locations.size());
        TTabletLocation location = locations.get(0);
        List<Long> nodes = location.getNode_ids();
        Assertions.assertEquals(3, nodes.size());
        Collections.sort(nodes);
        Assertions.assertEquals(Lists.newArrayList(backendId, backendId + 1, backendId + 2), nodes);
    }

    @Test
    public void testReplicatedStorageWithLocalTablet(@Mocked GlobalStateMgr globalStateMgr,
                                                     @Mocked SystemInfoService systemInfoService) throws Exception {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long physicalPartitionId = 6L;
        long replicaId = 10L;
        long backendId = 20L;
        
        //init be node
        Backend be1 = new Backend(1001L, "127.0.0.1", 9050);
        Backend be2 = new Backend(1002L, "127.0.0.2", 9050);
        Backend be3 = new Backend(1003L, "127.0.0.3", 9050);
        be1.setAlive(true);
        be2.setAlive(true);
        be3.setAlive(true);
        
        Map<Long, Backend> idToBackendRef = new HashMap<>();
        idToBackendRef.put(be1.getId(), be1);
        idToBackendRef.put(be2.getId(), be2);
        idToBackendRef.put(be3.getId(), be3);
        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackend(long backendId) {
                return idToBackendRef.get(backendId);
            }
        };

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);

        for (int i = 0; i < 9; i++) {
            // Replica
            Replica replica1 = new Replica(replicaId, be1.getId(), Replica.ReplicaState.NORMAL, 1, 0);
            Replica replica2 = new Replica(replicaId + 1, be2.getId(), Replica.ReplicaState.NORMAL, 1, 0);
            Replica replica3 = new Replica(replicaId + 2, be3.getId(), Replica.ReplicaState.NORMAL, 1, 0);

            // Tablet
            LocalTablet tablet = new LocalTablet(tabletId);
            tablet.addReplica(replica1);
            tablet.addReplica(replica2);
            tablet.addReplica(replica3);

            // Index
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, indexId, TStorageMedium.SSD);
            index.addTablet(tablet, tabletMeta);
        }

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.SSD));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                result = Status.OK;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.checkBackendAlive(anyLong);
                result = true;
            }
        };

        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        TOlapTablePartition tPartition = new TOlapTablePartition();
        tPartition.setId(physicalPartitionId);
        partitionParam.addToPartitions(tPartition);
        TOlapTableLocationParam param = OlapTableSink.createLocation(
                table, partitionParam, true);
        System.out.println(param);

        // Check
        List<TTabletLocation> locations = param.getTablets();
        Assertions.assertEquals(9, locations.size());

        HashMap<Long, Integer> beCount = new HashMap<>();
        for (TTabletLocation location : locations) {
            List<Long> nodes = location.getNode_ids();
            Assertions.assertEquals(3, nodes.size());

            beCount.put(nodes.get(0), beCount.getOrDefault(nodes.get(0), 0) + 1);
        }

        for (Integer v : beCount.values()) {
            Assertions.assertEquals(3, v.longValue());
        }
    }

    @Test
    public void testSingleListPartition() throws StarRocksException {
        TupleDescriptor tuple = getTuple();
        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST,
                Lists.newArrayList(new Column("province", Type.STRING)));
        listPartitionInfo.setValues(1, Lists.newArrayList("beijing", "shanghai"));
        listPartitionInfo.setReplicationNum(1, (short) 3);
        MaterializedIndex index = new MaterializedIndex(1, MaterializedIndex.IndexState.NORMAL);
        HashDistributionInfo distInfo = new HashDistributionInfo(
                3, Lists.newArrayList(new Column("id", Type.BIGINT)));
        Partition partition = new Partition(1, 11, "p1", index, distInfo);

        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        idToColumn.put(ColumnId.create("province"), new Column("province", Type.STRING));

        new Expectations() {
            {
                dstTable.getId();
                result = 1;
                dstTable.getPartitions();
                result = Lists.newArrayList(partition);
                dstTable.getPartition(1L);
                result = partition;
                dstTable.getPartitionInfo();
                result = listPartitionInfo;
                dstTable.getIdToColumn();
                result = idToColumn;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(1L),
                TWriteQuorumType.MAJORITY, false, false, false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000);
        sink.complete();

        Assertions.assertTrue(sink.toThrift() instanceof TDataSink);
    }

    @Test
    public void testImmutablePartition() throws StarRocksException {
        TupleDescriptor tuple = getTuple();
        SinglePartitionInfo partInfo = new SinglePartitionInfo();
        partInfo.setReplicationNum(2, (short) 3);
        MaterializedIndex index = new MaterializedIndex(2, MaterializedIndex.IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(3);
        Partition partition = new Partition(2, 22, "p1", index, distInfo);

        PhysicalPartition physicalPartition = new PhysicalPartition(3, "", 2, index);
        partition.addSubPartition(physicalPartition);

        physicalPartition = new PhysicalPartition(4, "", 2, index);
        physicalPartition.setImmutable(true);
        partition.addSubPartition(physicalPartition);

        LOG.info("partition is {}", partition);

        new Expectations() {
            {
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                dstTable.getPartitions();
                result = Lists.newArrayList(partition);
                dstTable.getPartition(2L);
                result = partition;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(2L),
                TWriteQuorumType.MAJORITY, false, false, false);
        sink.setAutomaticBucketSize(1);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000);
        sink.complete();
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }

    @Test
    public void testInitialOpenPartition() throws StarRocksException {
        TupleDescriptor tuple = getTuple();
        SinglePartitionInfo partInfo = new SinglePartitionInfo();
        partInfo.setReplicationNum(2, (short) 3);
        MaterializedIndex index = new MaterializedIndex(2, MaterializedIndex.IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(3);
        Partition partition = new Partition(2, 22, "p1", index, distInfo);

        PhysicalPartition physicalPartition = new PhysicalPartition(3, "", 2, index);
        partition.addSubPartition(physicalPartition);

        physicalPartition = new PhysicalPartition(4, "", 2, index);
        physicalPartition.setImmutable(true);
        partition.addSubPartition(physicalPartition);

        LOG.info("partition is {}", partition);

        new Expectations() {
            {
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                dstTable.getPartitions();
                result = Lists.newArrayList(partition);
                dstTable.getPartition(2L);
                result = partition;
            }
        };

        Config.max_load_initial_open_partition_number = 1;

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(2L),
                TWriteQuorumType.MAJORITY, false, false, true);
        sink.setAutomaticBucketSize(1);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000);
        sink.complete();
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));

        Config.max_load_initial_open_partition_number = 32;
    }

    @Test
    public void testSchemaChangeOpenPartition() throws StarRocksException {
        TupleDescriptor tuple = getTuple();
        SinglePartitionInfo partInfo = new SinglePartitionInfo();
        partInfo.setReplicationNum(2, (short) 3);
        MaterializedIndex index = new MaterializedIndex(2, MaterializedIndex.IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(3);
        Partition partition = new Partition(2, 22, "p1", index, distInfo);

        PhysicalPartition physicalPartition = new PhysicalPartition(3, "", 2, index);
        partition.addSubPartition(physicalPartition);

        physicalPartition = new PhysicalPartition(4, "", 2, index);
        physicalPartition.setImmutable(true);
        partition.addSubPartition(physicalPartition);

        new Expectations() {
            {
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                dstTable.getPartitions();
                result = Lists.newArrayList(partition);
                dstTable.getPartition(2L);
                result = partition;
                dstTable.getState();
                result = OlapTable.OlapTableState.SCHEMA_CHANGE;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(2L),
                TWriteQuorumType.MAJORITY, false, false, true);
        sink.setAutomaticBucketSize(1);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000);
        sink.complete();
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }

    @Test
    public void testFindPrimaryReplica() throws StarRocksException {

        //init be node
        Backend be1 = new Backend(1001L, "127.0.0.1", 9050);
        Backend be2 = new Backend(1002L, "127.0.0.2", 9050);
        Backend be3 = new Backend(1003L, "127.0.0.3", 9050);
        be1.setAlive(true);
        be2.setAlive(false);
        be3.setAlive(true);

        Map<Long, Backend> idToBackendRef = new HashMap<>();
        idToBackendRef.put(be1.getId(), be1);
        idToBackendRef.put(be2.getId(), be2);
        idToBackendRef.put(be3.getId(), be3);
        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackend(long backendId) {
                return idToBackendRef.get(backendId);
            }
        };
        //init primary replica num for be node
        Map<Long, Long> bePrimaryMap = new HashMap<>();
        bePrimaryMap.put(be1.getId(), 2L);
        bePrimaryMap.put(be2.getId(), 0L);
        bePrimaryMap.put(be3.getId(), 1L);

        OlapTable olapTable = new OlapTable();
        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        MaterializedIndex index = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        List<Long> selectedBackedIds = Lists.newArrayList();

        //1.check primary replica selection in multiple replica
        Replica replica1 = new Replica(11L, be1.getId(), Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica2 = new Replica(22L, be2.getId(), Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica3 = new Replica(33L, be3.getId(), Replica.ReplicaState.NORMAL, 1, 0);
        replica1.setLastWriteFail(false);
        replica2.setLastWriteFail(false);
        replica3.setLastWriteFail(false);
        List<Replica> multipleReplicaList = new ArrayList<>();
        multipleReplicaList.add(replica1);
        multipleReplicaList.add(replica2);
        multipleReplicaList.add(replica3);

        int lowUsageIndex1 = OlapTableSink.findPrimaryReplica(olapTable, bePrimaryMap, infoService,
                index, selectedBackedIds, multipleReplicaList);
        //note: even though in bePrimaryMap, primary replica num in be2 < primary replica num in be3,
        //      but be2 is in shutting down, so choose replica3 as primary replica.
        Assertions.assertEquals(multipleReplicaList.get(lowUsageIndex1).getId(), replica3.getId());
        Assertions.assertEquals(multipleReplicaList.get(lowUsageIndex1).getBackendId(), be3.getId());

        //2.check primary replica selection in single replica
        Replica replica4 = new Replica(44L, be2.getId(), Replica.ReplicaState.NORMAL, 1, 0);
        replica4.setLastWriteFail(false);
        List<Replica> singleReplicaList = new ArrayList<>();
        singleReplicaList.add(replica4);

        int lowUsageIndex2 = OlapTableSink.findPrimaryReplica(olapTable, bePrimaryMap, infoService,
                index, selectedBackedIds, singleReplicaList);
        //note: even though be2 is in shutting down, to ensure the load job can be loaded normally,
        //      be2 SHUTDOWN status could not be checked, so choose replica4 as primary replica. 
        Assertions.assertEquals(singleReplicaList.get(lowUsageIndex2).getId(), replica4.getId());
        Assertions.assertEquals(singleReplicaList.get(lowUsageIndex2).getBackendId(), be2.getId());
    }

    @Test
    public void testCreateLocationWithSharedDataMode(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        SystemInfoService sysInfoService = new SystemInfoService();
        MockedWarehouseManager warehouseManager = new MockedWarehouseManager();

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = sysInfoService;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = sysInfoService;
                GlobalStateMgr.getCurrentState().getWarehouseMgr();
                result = warehouseManager;
            }
        };

        // create two ComputeNodes
        ComputeNode node1 = new ComputeNode(10001L, "127.0.0.1", 9071);
        node1.updateOnce(1, 2, 3);
        BackendHbResponse shutdownResponse =
                new BackendHbResponse(node1.getId(), TStatusCode.SHUTDOWN, "BE is in shutting down");
        // Set node1 to status:SHUTDOWN
        Assertions.assertTrue(node1.handleHbResponse(shutdownResponse, false));
        Assertions.assertEquals(node1.getStatus(), ComputeNode.Status.SHUTDOWN);
        Assertions.assertFalse(node1.isAlive());

        ComputeNode node2 = new ComputeNode(10002L, "127.0.0.1", 9072);
        node2.updateOnce(1, 2, 3);

        warehouseManager.setAllComputeNodeIds(Lists.newArrayList(node1.getId(), node2.getId()));
        warehouseManager.setAliveComputeNodes(Lists.newArrayList(node2));
        warehouseManager.setComputeNodesAssignedToTablet(Sets.newHashSet(node1));

        sysInfoService.addComputeNode(node1);
        sysInfoService.addComputeNode(node2);

        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long physicalPartitionId = 6L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);

        LakeTablet tablet = new LakeTablet(tabletId);
        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.SSD));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, indexId, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);
        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);
        // Table
        OlapTable table = new LakeTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        TOlapTablePartition tPartition = new TOlapTablePartition();
        tPartition.setId(physicalPartitionId);
        partitionParam.addToPartitions(tPartition);
        TOlapTableLocationParam param = OlapTableSink.createLocation(table, partitionParam, false);
        LOG.warn("TableLocationParam: {}", param);
        // Check
        List<TTabletLocation> locations = param.getTablets();
        Assertions.assertEquals(1, locations.size());
        TTabletLocation location = locations.get(0);
        List<Long> nodes = location.getNode_ids();
        Assertions.assertEquals(1, nodes.size());
        Assertions.assertEquals((Long) node2.getId(), nodes.get(0));
    }
}
