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

import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PhysicalPartitionTest {
    private FakeGlobalStateMgr fakeGlobalStateMgr;

    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testPhysicalPartition() throws Exception {
        PhysicalPartition p = new PhysicalPartition(1, 1, new MaterializedIndex());
        Assertions.assertEquals(1, p.getId());
        Assertions.assertEquals(1, p.getParentId());
        Assertions.assertFalse(p.isImmutable());
        p.setImmutable(true);
        Assertions.assertTrue(p.isImmutable());
        Assertions.assertEquals(1, p.getVisibleVersion());
        Assertions.assertFalse(p.isFirstLoad());

        p.hashCode();

        p.updateVersionForRestore(2);
        Assertions.assertEquals(2, p.getVisibleVersion());
        Assertions.assertTrue(p.isFirstLoad());

        p.updateVisibleVersion(3);
        Assertions.assertEquals(3, p.getVisibleVersion());

        p.updateVisibleVersion(4, System.currentTimeMillis(), 1);
        Assertions.assertEquals(4, p.getVisibleVersion());
        Assertions.assertEquals(1, p.getVisibleTxnId());

        Assertions.assertTrue(p.getVisibleVersionTime() <= System.currentTimeMillis());

        p.setNextVersion(6);
        Assertions.assertEquals(6, p.getNextVersion());
        Assertions.assertEquals(5, p.getCommittedVersion());

        p.setDataVersion(5);
        Assertions.assertEquals(5, p.getDataVersion());

        p.setNextDataVersion(6);
        Assertions.assertEquals(6, p.getNextDataVersion());
        Assertions.assertEquals(5, p.getCommittedDataVersion());

        p.createRollupIndex(new MaterializedIndex(1));
        p.createRollupIndex(new MaterializedIndex(2, IndexState.SHADOW));

        Assertions.assertEquals(0, p.getLatestBaseIndex().getId());
        Assertions.assertNotNull(p.getIndex(0));
        Assertions.assertNotNull(p.getIndex(1));
        Assertions.assertNotNull(p.getIndex(2));
        Assertions.assertNull(p.getIndex(3));

        Assertions.assertEquals(3, p.getLatestMaterializedIndices(IndexExtState.ALL).size());
        Assertions.assertEquals(2, p.getLatestMaterializedIndices(IndexExtState.VISIBLE).size());
        Assertions.assertEquals(1, p.getLatestMaterializedIndices(IndexExtState.SHADOW).size());

        Assertions.assertTrue(p.hasMaterializedView());
        Assertions.assertTrue(p.hasStorageData());
        Assertions.assertEquals(0, p.storageDataSize());
        Assertions.assertEquals(0, p.storageRowCount());
        Assertions.assertEquals(0, p.storageReplicaCount());
        Assertions.assertEquals(0, p.getTabletMaxDataSize());

        p.toString();

        Assertions.assertFalse(p.visualiseShadowIndex(1, false));
        Assertions.assertTrue(p.visualiseShadowIndex(2, false));

        p.createRollupIndex(new MaterializedIndex(3, IndexState.SHADOW));
        Assertions.assertTrue(p.visualiseShadowIndex(3, true));

        Assertions.assertTrue(p.equals(p));
        Assertions.assertFalse(p.equals(new Partition(0, 11, "", new MaterializedIndex(), null)));

        p.createRollupIndex(new MaterializedIndex(4, IndexState.SHADOW));
        p.deleteMaterializedIndexByMetaId(0);
        p.deleteMaterializedIndexByMetaId(1);
        p.deleteMaterializedIndexByMetaId(2);
        p.deleteMaterializedIndexByMetaId(4);

        p.setIdForRestore(3);
        Assertions.assertEquals(3, p.getId());
        Assertions.assertEquals(1, p.getBeforeRestoreId());

        p.setParentId(3);
        Assertions.assertEquals(3, p.getParentId());

        p.setMinRetainVersion(1);
        Assertions.assertEquals(1, p.getMinRetainVersion());
        p.setLastVacuumTime(1);
        Assertions.assertEquals(1, p.getLastVacuumTime());

        p.setMinRetainVersion(3);
        p.setMetadataSwitchVersion(1);
        Assertions.assertEquals(1, p.getMinRetainVersion());
        p.setMetadataSwitchVersion(0);
        Assertions.assertEquals(3, p.getMinRetainVersion());

        p.setDataVersion(0);
        p.setNextDataVersion(0);
        p.setVersionEpoch(0);
        p.setVersionTxnType(null);
        p.gsonPostProcess();
        Assertions.assertEquals(p.getDataVersion(), p.getVisibleVersion());
        Assertions.assertEquals(p.getNextDataVersion(), p.getNextVersion());
        Assertions.assertTrue(p.getVersionEpoch() > 0);
        Assertions.assertEquals(p.getVersionTxnType(), TransactionType.TXN_NORMAL);
    }

    @Test
    public void testPhysicalPartitionEqual() throws Exception {
        PhysicalPartition p1 = new PhysicalPartition(1, 1, new MaterializedIndex(2));
        PhysicalPartition p2 = new PhysicalPartition(1, 1, new MaterializedIndex(2));
        Assertions.assertTrue(p1.equals(p2));

        p1.createRollupIndex(new MaterializedIndex(3));
        p2.createRollupIndex(new MaterializedIndex(3));
        Assertions.assertTrue(p1.equals(p2));
    }

    @Test
    public void testPhysicalPartitionUpgrade() throws IOException {
        long partitionId = 10001L;
        long parentId = 10002L;

        MaterializedIndex baseIndex = new MaterializedIndex(10003L, IndexState.NORMAL);

        Map<Long, MaterializedIndex> idToVisibleRollupIndex = Maps.newHashMap();
        MaterializedIndex rollupIndex = new MaterializedIndex(10004L, IndexState.NORMAL);
        idToVisibleRollupIndex.put(rollupIndex.getId(), rollupIndex);

        Map<Long, MaterializedIndex> idToShadowIndex = Maps.newHashMap();
        MaterializedIndex shadowBaseIndex = new MaterializedIndex(10005L, IndexState.SHADOW);
        idToShadowIndex.put(shadowBaseIndex.getId(), shadowBaseIndex);
        MaterializedIndex shadowRollupIndex = new MaterializedIndex(10006L, IndexState.SHADOW);
        idToShadowIndex.put(shadowRollupIndex.getId(), shadowRollupIndex);

        // Simulate the state deserialized from old version
        PhysicalPartition physicalPartition = new PhysicalPartition(partitionId, parentId, baseIndex);
        Deencapsulation.setField(physicalPartition, "baseIndex", baseIndex);
        Deencapsulation.setField(physicalPartition, "baseIndexMetaId", -1L);
        Deencapsulation.setField(physicalPartition, "indexMetaIdToIndexIds", Maps.newHashMap());
        Deencapsulation.setField(physicalPartition, "idToVisibleIndex", idToVisibleRollupIndex);
        Deencapsulation.setField(physicalPartition, "idToShadowIndex", idToShadowIndex);

        // Serialization/Deserialization
        String json = GsonUtils.GSON.toJson(physicalPartition);
        PhysicalPartition newPhysicalPartition = GsonUtils.GSON.fromJson(json, PhysicalPartition.class);

        // Check the state after post process
        // baseIndexMetaId should be set to baseIndex.getMataId()
        Assertions.assertEquals(baseIndex.getMetaId(), (long) Deencapsulation.getField(newPhysicalPartition, "baseIndexMetaId"));

        // indexMetaIdToIndexIds should be set correctly
        Map<Long, List<Long>> indexMetaIdToIndexIds = Deencapsulation.getField(newPhysicalPartition, "indexMetaIdToIndexIds");
        Assertions.assertEquals(4, indexMetaIdToIndexIds.size());

        Assertions.assertTrue(indexMetaIdToIndexIds.containsKey(baseIndex.getMetaId()));
        List<Long> baseIndexIds = indexMetaIdToIndexIds.get(baseIndex.getMetaId());
        Assertions.assertEquals(1, baseIndexIds.size());
        Assertions.assertEquals(baseIndex.getId(), baseIndexIds.get(0));

        Assertions.assertTrue(indexMetaIdToIndexIds.containsKey(rollupIndex.getMetaId()));
        List<Long> rollupIndexIds = indexMetaIdToIndexIds.get(rollupIndex.getMetaId());
        Assertions.assertEquals(1, rollupIndexIds.size());
        Assertions.assertEquals(rollupIndex.getId(), rollupIndexIds.get(0));

        Assertions.assertTrue(indexMetaIdToIndexIds.containsKey(shadowBaseIndex.getMetaId()));
        List<Long> shadowBaseIndexIds = indexMetaIdToIndexIds.get(shadowBaseIndex.getMetaId());
        Assertions.assertEquals(1, shadowBaseIndexIds.size());
        Assertions.assertEquals(shadowBaseIndex.getId(), shadowBaseIndexIds.get(0));

        Assertions.assertTrue(indexMetaIdToIndexIds.containsKey(shadowRollupIndex.getMetaId()));
        List<Long> shadowRollupIndexIds = indexMetaIdToIndexIds.get(shadowRollupIndex.getMetaId());
        Assertions.assertEquals(1, shadowRollupIndexIds.size());
        Assertions.assertEquals(shadowRollupIndex.getId(), shadowRollupIndexIds.get(0));

        // idToVisibleIndex should contain both baseIndex and rollupIndex
        Map<Long, MaterializedIndex> idToVisibleIndexNew = Deencapsulation.getField(newPhysicalPartition, "idToVisibleIndex");
        Assertions.assertEquals(2, idToVisibleIndexNew.size());
        Assertions.assertTrue(idToVisibleIndexNew.containsKey(baseIndex.getId()));
        Assertions.assertEquals(baseIndex, idToVisibleIndexNew.get(baseIndex.getId()));
        Assertions.assertTrue(idToVisibleIndexNew.containsKey(rollupIndex.getId()));
        Assertions.assertEquals(rollupIndex, idToVisibleIndexNew.get(rollupIndex.getId()));

        // idToShadowIndex is not changed
        Map<Long, MaterializedIndex> idToShadowIndexNew = Deencapsulation.getField(newPhysicalPartition, "idToShadowIndex");
        Assertions.assertEquals(2, idToShadowIndexNew.size());
        Assertions.assertTrue(idToShadowIndexNew.containsKey(shadowRollupIndex.getId()));
        Assertions.assertEquals(shadowRollupIndex, idToShadowIndexNew.get(shadowRollupIndex.getId()));

        // baseIndex is null
        Assertions.assertNull(Deencapsulation.getField(newPhysicalPartition, "baseIndex"));
    }

    @Test
    public void testCreateRollupMaterializedIndexOperations() {
        long partitionId = 1000L;
        long parentId = 2000L;
        long baseIndexId = 3001L;
        long baseMetaId = 3100L;
        long baseShardGroupId = 100L;
        long shadowIndexId = 5001L;
        long shadowMetaId = 5100L;
        long shadowShardGroupId = 300L;

        MaterializedIndex baseIndex = new MaterializedIndex(baseIndexId, baseMetaId, IndexState.NORMAL, baseShardGroupId);
        PhysicalPartition partition = new PhysicalPartition(partitionId, parentId, baseIndex);

        // 1. Base index
        // initial base index
        Assertions.assertEquals(baseIndex, partition.getLatestBaseIndex());
        Assertions.assertEquals(baseIndex, partition.getIndex(baseIndexId));
        Assertions.assertEquals(baseIndex, partition.getLatestIndex(baseMetaId));

        List<MaterializedIndex> baseIndices = partition.getBaseIndices();
        Assertions.assertEquals(1, baseIndices.size());
        Assertions.assertTrue(baseIndices.contains(baseIndex));

        List<Long> shardGroupIds = partition.getShardGroupIds();
        Assertions.assertEquals(1, shardGroupIds.size());
        Assertions.assertTrue(shardGroupIds.contains(baseShardGroupId));

        // 2. Create rollup shadow index
        // create shadow index
        MaterializedIndex shadowRollup =
                new MaterializedIndex(shadowIndexId, shadowMetaId, IndexState.SHADOW, shadowShardGroupId);
        partition.createRollupIndex(shadowRollup);

        Assertions.assertEquals(shadowRollup, partition.getIndex(shadowIndexId));
        Assertions.assertEquals(shadowRollup, partition.getLatestIndex(shadowMetaId));

        shardGroupIds = partition.getShardGroupIds();
        Assertions.assertEquals(2, shardGroupIds.size());
        Assertions.assertTrue(shardGroupIds.contains(baseShardGroupId));
        Assertions.assertTrue(shardGroupIds.contains(shadowShardGroupId));

        List<MaterializedIndex> indices = partition.getAllMaterializedIndices(IndexExtState.VISIBLE);
        Assertions.assertEquals(1, indices.size());
        Assertions.assertEquals(baseIndex, indices.get(0));
        indices = partition.getAllMaterializedIndices(IndexExtState.SHADOW);
        Assertions.assertEquals(1, indices.size());
        Assertions.assertEquals(shadowRollup, indices.get(0));

        // 3. Finish rollup
        partition.visualiseShadowIndex(shadowIndexId, false);
        Assertions.assertEquals(IndexState.NORMAL, shadowRollup.getState());

        indices = partition.getAllMaterializedIndices(IndexExtState.VISIBLE);
        Assertions.assertEquals(2, indices.size());
        Assertions.assertEquals(baseIndex, indices.get(0));
        Assertions.assertEquals(shadowRollup, indices.get(1));
        indices = partition.getAllMaterializedIndices(IndexExtState.SHADOW);
        Assertions.assertEquals(0, indices.size());
    }

    @Test
    public void testSchemaChangeMaterializedIndexOperations() {
        long partitionId = 1000L;
        long parentId = 2000L;
        long baseIndexId = 3001L;
        long baseMetaId = 3100L;
        long baseIndexIdNew = 3002L;
        long baseMetaIdNew = 3101L;
        long baseShardGroupId = 100L;
        long rollupIndexId = 4001L;
        long rollupMetaId = 4100L;
        long rollupIndexIdNew = 4002L;
        long rollupMetaIdNew = 4101L;
        long rollupShardGroupId = 200L;

        MaterializedIndex baseIndex = new MaterializedIndex(baseIndexId, baseMetaId, IndexState.NORMAL, baseShardGroupId);
        PhysicalPartition partition = new PhysicalPartition(partitionId, parentId, baseIndex);

        // 1. Base index
        // initial base index
        Assertions.assertEquals(baseIndex, partition.getLatestBaseIndex());
        Assertions.assertEquals(baseIndex, partition.getIndex(baseIndexId));
        Assertions.assertEquals(baseIndex, partition.getLatestIndex(baseMetaId));

        List<MaterializedIndex> baseIndices = partition.getBaseIndices();
        Assertions.assertEquals(1, baseIndices.size());
        Assertions.assertTrue(baseIndices.contains(baseIndex));

        List<Long> shardGroupIds = partition.getShardGroupIds();
        Assertions.assertEquals(1, shardGroupIds.size());
        Assertions.assertTrue(shardGroupIds.contains(baseShardGroupId));

        // 2. Rollup index
        // create rollup index
        MaterializedIndex rollupIndex =
                new MaterializedIndex(rollupIndexId, rollupMetaId, IndexState.NORMAL, rollupShardGroupId);
        partition.createRollupIndex(rollupIndex);

        Assertions.assertEquals(rollupIndex, partition.getIndex(rollupIndexId));
        Assertions.assertEquals(rollupIndex, partition.getLatestIndex(rollupMetaId));

        shardGroupIds = partition.getShardGroupIds();
        Assertions.assertEquals(2, shardGroupIds.size());
        Assertions.assertTrue(shardGroupIds.contains(baseShardGroupId));
        Assertions.assertTrue(shardGroupIds.contains(rollupShardGroupId));

        // 3. Create schema change shadow index
        MaterializedIndex baseIndexNew =
                new MaterializedIndex(baseIndexIdNew, baseMetaIdNew, IndexState.SHADOW, baseShardGroupId);
        partition.createRollupIndex(baseIndexNew);
        MaterializedIndex rollupIndexNew =
                new MaterializedIndex(rollupIndexIdNew, rollupMetaIdNew, IndexState.SHADOW, rollupShardGroupId);
        partition.createRollupIndex(rollupIndexNew);

        Assertions.assertEquals(baseIndexNew, partition.getIndex(baseIndexIdNew));
        Assertions.assertEquals(baseIndexNew, partition.getLatestIndex(baseMetaIdNew));
        Assertions.assertEquals(rollupIndexNew, partition.getIndex(rollupIndexIdNew));
        Assertions.assertEquals(rollupIndexNew, partition.getLatestIndex(rollupMetaIdNew));

        shardGroupIds = partition.getShardGroupIds();
        Assertions.assertEquals(2, shardGroupIds.size());
        Assertions.assertTrue(shardGroupIds.contains(baseShardGroupId));
        Assertions.assertTrue(shardGroupIds.contains(rollupShardGroupId));

        List<MaterializedIndex> indices = partition.getAllMaterializedIndices(IndexExtState.VISIBLE);
        Assertions.assertEquals(2, indices.size());
        Assertions.assertTrue(indices.contains(baseIndex));
        Assertions.assertTrue(indices.contains(rollupIndex));
        indices = partition.getAllMaterializedIndices(IndexExtState.SHADOW);
        Assertions.assertEquals(2, indices.size());
        Assertions.assertTrue(indices.contains(baseIndexNew));
        Assertions.assertTrue(indices.contains(rollupIndexNew));

        // 4. Finish schema change
        partition.deleteMaterializedIndexByMetaId(baseMetaId);
        partition.deleteMaterializedIndexByMetaId(rollupMetaId);
        partition.visualiseShadowIndex(baseIndexIdNew, true);
        partition.visualiseShadowIndex(rollupIndexIdNew, false);

        indices = partition.getAllMaterializedIndices(IndexExtState.VISIBLE);
        Assertions.assertEquals(2, indices.size());
        Assertions.assertTrue(indices.contains(baseIndexNew));
        Assertions.assertTrue(indices.contains(rollupIndexNew));
        indices = partition.getAllMaterializedIndices(IndexExtState.SHADOW);
        Assertions.assertEquals(0, indices.size());
    }

    @Test
    public void testMultiVersionMaterializedIndexOperations() {
        long partitionId = 1000L;
        long parentId = 2000L;
        long baseIndexId1 = 3001L;
        long baseIndexId2 = 3002L;
        long baseMetaId = 3100L;
        long baseShardGroupId = 100L;
        long rollupIndexId1 = 4001L;
        long rollupIndexId2 = 4002L;
        long rollupMetaId = 4100L;
        long rollupShardGroupId = 200L;

        MaterializedIndex baseIndex1 = new MaterializedIndex(baseIndexId1, baseMetaId, IndexState.NORMAL, baseShardGroupId);
        PhysicalPartition partition = new PhysicalPartition(partitionId, parentId, baseIndex1);

        // 1. Base index
        // initial base index
        Assertions.assertEquals(baseIndex1, partition.getLatestBaseIndex());
        Assertions.assertEquals(baseIndex1, partition.getIndex(baseIndexId1));
        Assertions.assertEquals(baseIndex1, partition.getLatestIndex(baseMetaId));

        List<MaterializedIndex> baseIndices = partition.getBaseIndices();
        Assertions.assertEquals(1, baseIndices.size());
        Assertions.assertTrue(baseIndices.contains(baseIndex1));

        // addBaseIndex (requires same metaId)
        MaterializedIndex baseIndex2 = new MaterializedIndex(baseIndexId2, baseMetaId, IndexState.NORMAL, baseShardGroupId);
        partition.addMaterializedIndex(baseIndex2, true);

        Assertions.assertEquals(baseIndex2, partition.getLatestBaseIndex());
        Assertions.assertEquals(baseIndex2, partition.getIndex(baseIndexId2));
        Assertions.assertEquals(baseIndex2, partition.getLatestIndex(baseMetaId));

        baseIndices = partition.getBaseIndices();
        Assertions.assertEquals(2, baseIndices.size());
        Assertions.assertTrue(baseIndices.contains(baseIndex1));
        Assertions.assertTrue(baseIndices.contains(baseIndex2));

        List<Long> shardGroupIds = partition.getShardGroupIds();
        Assertions.assertEquals(1, shardGroupIds.size());
        Assertions.assertTrue(shardGroupIds.contains(baseShardGroupId));

        // 2. Rollup index
        // create rollup index
        MaterializedIndex rollup1 = new MaterializedIndex(rollupIndexId1, rollupMetaId, IndexState.NORMAL, rollupShardGroupId);
        partition.createRollupIndex(rollup1);
        Assertions.assertEquals(rollup1, partition.getIndex(rollupIndexId1));

        // addRollupIndex
        MaterializedIndex rollup2 = new MaterializedIndex(rollupIndexId2, rollupMetaId, IndexState.NORMAL, rollupShardGroupId);
        partition.addMaterializedIndex(rollup2, false);

        Assertions.assertEquals(rollup1, partition.getIndex(rollupIndexId2));
        Assertions.assertEquals(rollup2, partition.getLatestIndex(rollupMetaId));

        shardGroupIds = partition.getShardGroupIds();
        Assertions.assertEquals(2, shardGroupIds.size());
        Assertions.assertTrue(shardGroupIds.contains(baseShardGroupId));
        Assertions.assertTrue(shardGroupIds.contains(rollupShardGroupId));

        List<MaterializedIndex> allIndices = partition.getAllMaterializedIndices(IndexExtState.VISIBLE);
        Assertions.assertEquals(4, allIndices.size());

        List<MaterializedIndex> latestIndices = partition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
        Assertions.assertEquals(2, latestIndices.size());
        Assertions.assertTrue(latestIndices.contains(baseIndex2));
        Assertions.assertTrue(latestIndices.contains(rollup2));
    }

    @Test
    public void testDeleteMaterializedIndexOperations() {
        long partitionId = 1000L;
        long parentId = 2000L;
        long baseIndexId1 = 3001L;
        long baseIndexId2 = 3002L;
        long baseMetaId = 3100L;
        long baseShardGroupId = 100L;

        MaterializedIndex baseIndex1 = new MaterializedIndex(baseIndexId1, baseMetaId, IndexState.NORMAL, baseShardGroupId);
        PhysicalPartition partition = new PhysicalPartition(partitionId, parentId, baseIndex1);

        MaterializedIndex baseIndex2 = new MaterializedIndex(baseIndexId2, baseMetaId, IndexState.NORMAL, baseShardGroupId);
        partition.addMaterializedIndex(baseIndex2, true);

        Map<Long, List<Long>> indexMetaIdToIndexIds = Deencapsulation.getField(partition, "indexMetaIdToIndexIds");
        Assertions.assertTrue(indexMetaIdToIndexIds.containsKey(baseMetaId));
        Assertions.assertEquals(2, indexMetaIdToIndexIds.get(baseMetaId).size());

        partition.deleteMaterializedIndexByIndexId(baseIndexId1);
        Assertions.assertTrue(indexMetaIdToIndexIds.containsKey(baseMetaId));
        Assertions.assertEquals(1, indexMetaIdToIndexIds.get(baseMetaId).size());
        Assertions.assertEquals(baseIndexId2, (long) indexMetaIdToIndexIds.get(baseMetaId).get(0));

        partition.deleteMaterializedIndexByIndexId(baseIndexId2);
        Assertions.assertFalse(indexMetaIdToIndexIds.containsKey(baseMetaId));
    }
}
