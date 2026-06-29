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

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.NextIdLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
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

/**
 * Tests for {@link LakeTableAlterJobV2Builder#buildRangeShadowIndex}.
 */
public class LakeTableAlterJobV2BuilderTest {

    private static final int K = 3;

    private LakeTable table;
    private PhysicalPartition physicalPartition;
    private long partitionId;
    private long physicalPartitionId;

    // Tablet ids that the mocked createShards will return.
    private final List<Long> allocatedShardIds = new ArrayList<>();

    @BeforeEach
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        allocatedShardIds.clear();

        // Prevent getNextId from trying to persist to an absent EditLog.
        new MockUp<EditLog>() {
            @Mock
            public void logSaveNextId(long nextId, WALApplier applier) {
                applier.apply(new NextIdLog(nextId));
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        GlobalStateMgr.getCurrentState().setStarOSAgent(new StarOSAgent());

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(int shardCount, FilePathInfo path, FileCacheInfo cache, long groupId,
                                           List<Long> matchShardIds, Map<String, String> properties,
                                           ComputeResource computeResource)
                    throws DdlException {
                List<Long> ids = new ArrayList<>();
                for (int i = 0; i < shardCount; i++) {
                    ids.add(GlobalStateMgr.getCurrentState().getNextId());
                }
                allocatedShardIds.addAll(ids);
                return ids;
            }
        };

        long dbId = GlobalStateMgr.getCurrentState().getNextId();
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        partitionId = GlobalStateMgr.getCurrentState().getNextId();
        physicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        long indexId = GlobalStateMgr.getCurrentState().getNextId();

        Column c0 = new Column("c0", IntegerType.INT, true);
        DistributionInfo dist = new RangeDistributionInfo();
        PartitionInfo partitionInfo = new RangePartitionInfo(Collections.singletonList(c0));
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);

        table = new LakeTable(tableId, "t0", Collections.singletonList(c0), KeysType.DUP_KEYS,
                partitionInfo, dist);
        table.setTableProperty(new TableProperty(new HashMap<>()));

        // Provide a minimal FilePathInfo so getPartitionFilePathInfo returns non-null.
        FilePathInfo.Builder fpBuilder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = fpBuilder.getFsInfoBuilder();
        S3FileStoreInfo.Builder s3Builder = fsBuilder.getS3FsInfoBuilder();
        s3Builder.setBucket("test-bucket");
        s3Builder.setRegion("us-east-1");
        fsBuilder.setS3FsInfo(s3Builder.build());
        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fpBuilder.setFsInfo(fsBuilder.build());
        fpBuilder.setFullPath("s3://test-bucket/test-table");
        table.setStorageInfo(fpBuilder.build(), new DataCacheInfo(false, false));

        table.setIndexMeta(indexId, "t0", Collections.singletonList(c0), 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexMetaId(indexId);

        MaterializedIndex baseIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, physicalPartitionId, "p0", baseIndex, dist);
        table.addPartition(partition);
        physicalPartition = partition.getDefaultPhysicalPartition();
    }

    /**
     * Builds a K-tablet shadow index and checks that:
     * 1. The index state is SHADOW.
     * 2. The index contains exactly K tablets.
     * 3. Each tablet's range equals the corresponding input range.
     * 4. createShards was called with matchShardIds == null (verified via the mock capturing the call).
     */
    @Test
    public void testBuildRangeShadowIndex() throws DdlException {
        long shadowIndexId = 9001L;
        long shardGroupId = 500L;

        List<TabletRange> ranges = new ArrayList<>();
        for (int i = 0; i < K; i++) {
            ranges.add(new TabletRange());
        }

        AtomicBoolean matchShardIdsWasNull = new AtomicBoolean(false);
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(int shardCount, FilePathInfo path, FileCacheInfo cache, long groupId,
                                           List<Long> matchShardIds, Map<String, String> properties,
                                           ComputeResource computeResource)
                    throws DdlException {
                matchShardIdsWasNull.set(matchShardIds == null);
                List<Long> ids = new ArrayList<>();
                for (int i = 0; i < shardCount; i++) {
                    ids.add(GlobalStateMgr.getCurrentState().getNextId());
                }
                allocatedShardIds.addAll(ids);
                return ids;
            }
        };
        allocatedShardIds.clear();

        MaterializedIndex shadowIndex = LakeTableAlterJobV2Builder.buildRangeShadowIndex(
                1L /* dbId */,
                table,
                physicalPartition,
                shadowIndexId,
                K,
                ranges,
                shardGroupId,
                WarehouseComputeResource.of(0L));

        // 1. State must be SHADOW.
        Assertions.assertEquals(MaterializedIndex.IndexState.SHADOW, shadowIndex.getState());

        // 2. Exactly K tablets.
        List<Tablet> tablets = shadowIndex.getTablets();
        Assertions.assertEquals(K, tablets.size());

        // 3. Each tablet's range is the exact same object as the corresponding input range.
        //    TabletRange has no equals(), so identity (assertSame) is the real invariant:
        //    the method must store the exact range instance passed for index i.
        for (int i = 0; i < K; i++) {
            TabletRange got = tablets.get(i).getRange();
            Assertions.assertNotNull(got, "range at index " + i + " should not be null");
            Assertions.assertSame(ranges.get(i), got, "range identity mismatch at index " + i);
        }

        // 4. matchShardIds was null (no origin-shard preference).
        Assertions.assertTrue(matchShardIdsWasNull.get(), "createShards should receive null matchShardIds");
    }

    /**
     * Verifies the precondition: tabletCount != ranges.size() throws.
     */
    @Test
    public void testSizeMismatchThrows() {
        List<TabletRange> ranges = List.of(new TabletRange()); // size 1 != tabletCount 2

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                LakeTableAlterJobV2Builder.buildRangeShadowIndex(
                        1L, table, physicalPartition, 9001L,
                        2, ranges, 500L, WarehouseComputeResource.of(0L)));
    }
}
