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

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.load.PartitionUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class LakeColocateShardMetaGroupTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_colo_mg").useDatabase("test_colo_mg");
    }

    @Test
    public void testNewPartitionShardsJoinMetaGroupAtCreation() throws Exception {
        List<Long> capturedMetaGroupIds = new ArrayList<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, long metaGroupId,
                                           ComputeResource computeResource) throws DdlException {
                capturedMetaGroupIds.add(metaGroupId);
                return invocation.proceed(numShards, pathInfo, cacheInfo, groupIds, matchShardIds,
                        properties, metaGroupId, computeResource);
            }
        };

        starRocksAssert.withTable("CREATE TABLE test_colo_mg.cm1 (k1 int, k2 int)"
                + " PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3"
                + " PROPERTIES('colocate_with' = 'cg_mg_ut')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test_colo_mg", "cm1");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Assertions.assertTrue(colocateTableIndex.isMetaGroupColocateTable(table.getId()));
        long grpId = colocateTableIndex.getGroup(table.getId()).grpId;

        // A new partition of the colocate table: its shards must be created with the meta group,
        // so the very first placement already honors the colocation constraint.
        capturedMetaGroupIds.clear();
        starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm1 ADD PARTITION p2 VALUES LESS THAN ('20')");
        Assertions.assertEquals(List.of(grpId), capturedMetaGroupIds);

        // Control: a non-colocate table's new partition carries no meta group.
        starRocksAssert.withTable("CREATE TABLE test_colo_mg.cm2 (k1 int, k2 int)"
                + " PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3");
        capturedMetaGroupIds.clear();
        starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm2 ADD PARTITION p2 VALUES LESS THAN ('20')");
        Assertions.assertEquals(List.of(0L), capturedMetaGroupIds);
    }

    @Test
    public void testAddPartitionRetriesWhenColocationChangesDuringShardCreation() throws Exception {
        List<Long> capturedMetaGroupIds = new ArrayList<>();
        AtomicBoolean decolocated = new AtomicBoolean(false);
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, long metaGroupId,
                                           ComputeResource computeResource) throws DdlException {
                capturedMetaGroupIds.add(metaGroupId);
                List<Long> shardIds = invocation.proceed(numShards, pathInfo, cacheInfo, groupIds, matchShardIds,
                        properties, metaGroupId, computeResource);
                // Race a de-colocation into the lock-free window of ADD PARTITION: the shards above are
                // already pinned to the colocate meta group when the table leaves the group.
                if (metaGroupId != 0 && decolocated.compareAndSet(false, true)) {
                    try {
                        starRocksAssert.alterTableProperties(
                                "ALTER TABLE test_colo_mg.cm3 SET ('colocate_with' = '')");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return shardIds;
            }
        };

        starRocksAssert.withTable("CREATE TABLE test_colo_mg.cm3 (k1 int, k2 int)"
                + " PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3"
                + " PROPERTIES('colocate_with' = 'cg_mg_race')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test_colo_mg", "cm3");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateTableIndex.getGroup(table.getId()).grpId;

        // The commit re-validates the colocation snapshot taken before shard creation and rejects the
        // DDL instead of committing shards pinned to a meta group the table no longer belongs to.
        capturedMetaGroupIds.clear();
        Exception e = Assertions.assertThrows(Exception.class,
                () -> starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm3 ADD PARTITION p2 VALUES LESS THAN ('20')"));
        Assertions.assertTrue(e.getMessage().contains("colocation has been changed"), e.getMessage());
        Assertions.assertEquals(List.of(grpId), capturedMetaGroupIds);
        Assertions.assertTrue(decolocated.get());
        Assertions.assertNull(table.getPartition("p2"));
        Assertions.assertFalse(colocateTableIndex.isColocateTable(table.getId()));

        // The retry sees the table's current, non-colocate state and creates the shards without a meta group.
        capturedMetaGroupIds.clear();
        starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm3 ADD PARTITION p2 VALUES LESS THAN ('20')");
        Assertions.assertEquals(List.of(0L), capturedMetaGroupIds);
        Assertions.assertNotNull(table.getPartition("p2"));
    }

    @Test
    public void testFirstPartitionOfColocateTableCreatedWithoutPartitions() throws Exception {
        List<Long> capturedMetaGroupIds = new ArrayList<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, long metaGroupId,
                                           ComputeResource computeResource) throws DdlException {
                capturedMetaGroupIds.add(metaGroupId);
                return invocation.proceed(numShards, pathInfo, cacheInfo, groupIds, matchShardIds,
                        properties, metaGroupId, computeResource);
            }
        };

        // A hash colocate lake table created without any partition (the shape of dynamic partitioning
        // or a partitioned colocate MV): it joins the colocate group with no shard group, so its meta
        // group has no bucket yet and StarMgr rejects a create-time join.
        starRocksAssert.withTable("CREATE TABLE test_colo_mg.cm4 (k1 int, k2 int)"
                + " PARTITION BY RANGE(k1) ()"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3"
                + " PROPERTIES('colocate_with' = 'cg_mg_empty')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test_colo_mg", "cm4");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateTableIndex.getGroup(table.getId()).grpId;
        Assertions.assertTrue(colocateTableIndex.isMetaGroupColocateTable(table.getId()));
        Assertions.assertTrue(table.getShardGroupIds().isEmpty());

        // The very first partition falls back to a plain creation; its post-commit join defines the buckets.
        capturedMetaGroupIds.clear();
        starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm4 ADD PARTITION p1 VALUES LESS THAN ('10')");
        Assertions.assertEquals(List.of(grpId), capturedMetaGroupIds);
        Assertions.assertNotNull(table.getPartition("p1"));

        // From then on the create-time join is honored.
        capturedMetaGroupIds.clear();
        starRocksAssert.ddl("ALTER TABLE test_colo_mg.cm4 ADD PARTITION p2 VALUES LESS THAN ('20')");
        Assertions.assertEquals(List.of(grpId), capturedMetaGroupIds);
        Assertions.assertNotNull(table.getPartition("p2"));
    }

    @Test
    public void testTempPartitionCreationRejectedWhenColocationChangesDuringShardCreation() throws Exception {
        List<Long> capturedMetaGroupIds = new ArrayList<>();
        AtomicBoolean decolocated = new AtomicBoolean(false);
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, long metaGroupId,
                                           ComputeResource computeResource) throws DdlException {
                capturedMetaGroupIds.add(metaGroupId);
                List<Long> shardIds = invocation.proceed(numShards, pathInfo, cacheInfo, groupIds, matchShardIds,
                        properties, metaGroupId, computeResource);
                if (metaGroupId != 0 && decolocated.compareAndSet(false, true)) {
                    try {
                        starRocksAssert.alterTableProperties(
                                "ALTER TABLE test_colo_mg.cm5 SET ('colocate_with' = '')");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return shardIds;
            }
        };

        starRocksAssert.withTable("CREATE TABLE test_colo_mg.cm5 (k1 int, k2 int)"
                + " PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'))"
                + " DISTRIBUTED BY HASH(k2) BUCKETS 3"
                + " PROPERTIES('colocate_with' = 'cg_mg_tmp')");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_colo_mg");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test_colo_mg", "cm5");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateTableIndex.getGroup(table.getId()).grpId;
        List<Long> sourcePartitionIds = List.of(table.getPartition("p1").getId());
        connectContext.setThreadLocalInfo();

        // The temp partitions of INSERT OVERWRITE / OPTIMIZE take the same lock-free path: the shards are
        // pinned to the meta group, then committed under the WRITE lock, where the colocation is re-validated.
        capturedMetaGroupIds.clear();
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> PartitionUtils.createAndAddTempPartitionsForTable(db, table, "_tmp", sourcePartitionIds,
                        List.of(GlobalStateMgr.getCurrentState().getNextId()), null,
                        WarehouseManager.DEFAULT_RESOURCE));
        Assertions.assertTrue(e.getMessage().contains("colocation has been changed"), e.getMessage());
        Assertions.assertEquals(List.of(grpId), capturedMetaGroupIds);
        Assertions.assertTrue(decolocated.get());
        Assertions.assertTrue(table.getTempPartitions().isEmpty());
        Assertions.assertFalse(colocateTableIndex.isColocateTable(table.getId()));

        // The retry sees the table's current, non-colocate state and creates the shards without a meta group.
        capturedMetaGroupIds.clear();
        PartitionUtils.createAndAddTempPartitionsForTable(db, table, "_tmp", sourcePartitionIds,
                List.of(GlobalStateMgr.getCurrentState().getNextId()), null, WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(List.of(0L), capturedMetaGroupIds);
        Assertions.assertEquals(1, table.getTempPartitions().size());
    }
}
