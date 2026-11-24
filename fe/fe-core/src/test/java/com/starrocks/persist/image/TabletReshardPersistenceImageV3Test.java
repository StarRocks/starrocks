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

package com.starrocks.persist.image;

import com.starrocks.alter.reshard.IdenticalTablet;
import com.starrocks.alter.reshard.MergeTabletJob;
import com.starrocks.alter.reshard.MergingTablet;
import com.starrocks.alter.reshard.ReshardingMaterializedIndex;
import com.starrocks.alter.reshard.ReshardingPhysicalPartition;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.SplitTabletJob;
import com.starrocks.alter.reshard.SplittingTablet;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.TabletRangeProtoConverter;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.ReshardingTabletPB;
import com.starrocks.persist.proto.TabletReshardJobPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ReshardImageTestSupport.index;
import static com.starrocks.persist.image.ReshardImageTestSupport.job;
import static com.starrocks.persist.image.ReshardImageTestSupport.partition;
import static com.starrocks.persist.image.ReshardImageTestSupport.partitions;
import static com.starrocks.persist.image.ReshardImageTestSupport.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TabletReshardPersistenceImageV3Test {
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    @Test
    public void testBothJobKindsAllStatesAndAllPersistedFields() throws Exception {
        for (boolean split : List.of(false, true)) {
            for (TabletReshardJob.JobState state : TabletReshardJob.JobState.values()) {
                TabletReshardJob source = job(7, split, state);
                FinalManagersImageTestSupport.assertAllFieldsPresent(source);
                FinalManagersImageTestSupport.assertAllFieldsPresent(partition(source));
                FinalManagersImageTestSupport.assertAllFieldsPresent(index(source));
                for (ReshardingTablet tablet : index(source).getReshardingTablets()) {
                    FinalManagersImageTestSupport.assertAllFieldsPresent(tablet);
                }
                TabletReshardJobPB pb = TabletReshardJobPB.parseFrom(source.toProto().toByteArray());
                assertEquals(split ? TabletReshardJobPB.Kind.SPLIT_TABLET : TabletReshardJobPB.Kind.MERGE_TABLET, pb.getKind());
                assertEquals(split ? TabletReshardJobPB.InstanceCase.SPLIT_TABLET : TabletReshardJobPB.InstanceCase.MERGE_TABLET,
                        pb.getInstanceCase());
                TabletReshardJob restored = TabletReshardJob.fromProto(pb);
                GsonEquivalence.assertEquivalent(FinalManagersImageTestSupport.gson(source, TabletReshardJob.class), restored);
                assertEquals(source.getClass(), restored.getClass());
                assertEquals(source.getJobType(), restored.getJobType());
                assertEquals(7, restored.getJobId());
                assertEquals(state, getField(restored, "jobState"));
                assertEquals(123463L, restored.getCreatedTimeMs());
                assertEquals(234574L, restored.getFinishedTimeMs());
                assertEquals(345685L, (long) getField(restored, "stateStartedTimeMs"));
                assertEquals(456796L, (long) getField(restored, "transactionId"));
                assertEquals(567897L, (long) getField(restored, "gtid"));
                assertEquals(678908L, (long) getField(restored, "endTransactionId"));
                assertEquals("persisted error 7", getField(restored, "errorMessage"));
                assertEquals(99L, restored.getWarehouseId());
                assertEquals(source.isDone(), restored.isDone());
                assertEquals(source.getParallelTablets(), restored.getParallelTablets());
            }
        }
        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testOriginalConstructorsStillInitializeCreationAndStateTimes() {
        long start = System.currentTimeMillis();
        List<TabletReshardJob> jobs = List.of(new SplitTabletJob(1, 2, 3, new HashMap<>()),
                new MergeTabletJob(4, 5, 6, new HashMap<>()));
        long end = System.currentTimeMillis();
        for (TabletReshardJob job : jobs) {
            assertTrue(job.getCreatedTimeMs() >= start && job.getCreatedTimeMs() <= end);
            assertEquals(job.getCreatedTimeMs(), (long) getField(job, "stateStartedTimeMs"));
            assertEquals(TabletReshardJob.JobState.PENDING, getField(job, "jobState"));
            assertEquals(0, job.getFinishedTimeMs());
            assertNull(job.getWarehouseId());
        }
    }

    @Test
    public void testPersistedTimesOverrideFieldInitializersIncludingZero() throws Exception {
        when(global.getLocalMetastore()).thenReturn(mock(LocalMetastore.class));
        for (boolean split : List.of(false, true)) {
            for (long createdTime : List.of(0L, 123456L, Long.MAX_VALUE)) {
                TabletReshardJob source = job(1, split, TabletReshardJob.JobState.RUNNING);
                long stateStartedTime = createdTime == 0 ? 0 : createdTime - 1;
                setField(source, "createdTimeMs", createdTime);
                setField(source, "stateStartedTimeMs", stateStartedTime);
                TabletReshardJobPB pb = TabletReshardJobPB.parseFrom(source.toProto().toByteArray());
                TabletReshardJob restored = TabletReshardJob.fromProto(pb);
                GsonEquivalence.assertEquivalent(
                        FinalManagersImageTestSupport.gson(source, TabletReshardJob.class), restored);
                assertEquals(createdTime, restored.getCreatedTimeMs());
                assertEquals(stateStartedTime, (long) getField(restored, "stateStartedTimeMs"));
                assertEquals(createdTime / 1000, restored.getInfo().getCreated_time());
            }
        }
        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testWarehouseMissingZeroAndNonzeroStayDistinct() {
        for (boolean split : List.of(false, true)) {
            TabletReshardJob source = job(1, split, TabletReshardJob.JobState.PENDING);
            setField(source, "warehouseId", null);
            TabletReshardJobPB pb = source.toProto();
            assertFalse(pb.getCore().hasWarehouseId());
            assertNull(TabletReshardJob.fromProto(pb).getWarehouseId());
            source.setWarehouseId(0);
            pb = source.toProto();
            assertTrue(pb.getCore().hasWarehouseId());
            assertEquals(0L, TabletReshardJob.fromProto(pb).getWarehouseId());
            source.setWarehouseId(999);
            assertEquals(999L, TabletReshardJob.fromProto(source.toProto()).getWarehouseId());
        }
    }

    @Test
    public void testThreeTabletKindsPreserveConcreteClassAndIdLists() throws Exception {
        List<ReshardingTablet> tablets = List.of(
                new SplittingTablet(1, List.of(2L, 3L), List.of(range(null, 10L), range(10L, null))),
                new MergingTablet(new ArrayList<>(List.of(8L, 6L, 7L)), 9), new IdenticalTablet(10, 11));
        List<ReshardingTabletPB.Kind> kinds = List.of(ReshardingTabletPB.Kind.SPLITTING,
                ReshardingTabletPB.Kind.MERGING, ReshardingTabletPB.Kind.IDENTICAL);
        List<ReshardingTabletPB.InstanceCase> instances = List.of(ReshardingTabletPB.InstanceCase.SPLITTING,
                ReshardingTabletPB.InstanceCase.MERGING, ReshardingTabletPB.InstanceCase.IDENTICAL);
        for (int i = 0; i < tablets.size(); i++) {
            ReshardingTablet source = tablets.get(i);
            ReshardingTabletPB pb = ReshardingTabletPB.parseFrom(source.toImageProto().toByteArray());
            assertEquals(kinds.get(i), pb.getKind());
            assertEquals(instances.get(i), pb.getInstanceCase());
            ReshardingTablet restored = ReshardingTablet.fromImageProto(pb);
            GsonEquivalence.assertEquivalent(
                    FinalManagersImageTestSupport.gson(source, ReshardingTablet.class), restored);
            assertEquals(source.getClass(), restored.getClass());
            assertEquals(source.getOldTabletIds(), restored.getOldTabletIds());
            assertEquals(source.getNewTabletIds(), restored.getNewTabletIds());
            assertEquals(source.getFirstOldTabletId(), restored.getFirstOldTabletId());
            assertEquals(source.getFirstNewTabletId(), restored.getFirstNewTabletId());
            assertEquals(source.getParallelTablets(), restored.getParallelTablets());
        }
    }

    @Test
    public void testIdenticalFallbackKeepsSplittingClassInImageAndUsesIdenticalRpc() {
        SplittingTablet source = new SplittingTablet(1, List.of(2L, 3L), List.of(range(null, 5L), range(5L, null)));
        source.fallbackToIdenticalTablet();
        ReshardingTabletPB pb = source.toImageProto();
        assertEquals(ReshardingTabletPB.Kind.SPLITTING, pb.getKind());
        SplittingTablet restored = assertInstanceOf(SplittingTablet.class, ReshardingTablet.fromImageProto(pb));
        assertTrue(restored.isIdenticalTablet());
        assertEquals(List.of(2L), restored.getNewTabletIds());
        assertTrue(restored.getNewTabletRanges().isEmpty());
        assertNull(restored.getSplittingTablet());
        assertNotNull(restored.getIdenticalTablet());
        assertNotNull(restored.toProto().identicalTabletInfo);
        assertNull(restored.toProto().splittingTabletInfo);
        GsonEquivalence.assertEquivalent(FinalManagersImageTestSupport.gson(source, ReshardingTablet.class), restored);
    }

    @Test
    public void testRestoredSplitCanFallbackAndDiscardItsExplicitRanges() {
        SplittingTablet source = new SplittingTablet(1, List.of(2L, 3L), List.of(range(null, 5L), range(5L, null)));
        SplittingTablet restored = (SplittingTablet) ReshardingTablet.fromImageProto(source.toImageProto());
        assertEquals(2, restored.toProto().splittingTabletInfo.newTabletRanges.size());
        restored.fallbackToIdenticalTablet();
        assertEquals(List.of(2L), restored.getNewTabletIds());
        assertTrue(restored.getNewTabletRanges().isEmpty());
        assertEquals(List.of(2L, 3L), source.getNewTabletIds());
        assertEquals(2, source.getNewTabletRanges().size());
    }

    @Test
    public void testDataDrivenSplitKeepsEmptyRangesAndOriginalRpcShape() {
        SplittingTablet source = new SplittingTablet(1, List.of(2L, 3L, 4L));
        SplittingTablet restored = (SplittingTablet) ReshardingTablet.fromImageProto(source.toImageProto());
        assertTrue(restored.getNewTabletRanges().isEmpty());
        assertNull(restored.toProto().splittingTabletInfo.newTabletRanges);
        assertEquals(source.toProto().splittingTabletInfo.newTabletIds, restored.toProto().splittingTabletInfo.newTabletIds);
        assertEquals(3, restored.getParallelTablets());
    }

    @Test
    public void testIndexMetadataTabletsAndLookupAreRestoredTogether() {
        TabletReshardJob source = job(1, true, TabletReshardJob.JobState.RUNNING);
        TabletReshardJob restored = TabletReshardJob.fromProto(source.toProto());
        MaterializedIndex index = index(restored).getMaterializedIndex();
        assertEquals(1002, index.getId());
        assertEquals(1003, index.getMetaId());
        assertEquals(2, index.getTablets().size());
        assertInstanceOf(LakeTablet.class, index.getTablet(1011));
        assertSame(index.getTablets().get(0), index.getTablet(1011));
        assertSame(index.getTablets().get(1), index.getTablet(1012));
        assertEquals(TabletRangeProtoConverter.toProto(range(null, 50L)),
                TabletRangeProtoConverter.toProto(index.getTablet(1011).getRange()));
        assertNotSame(index(source).getMaterializedIndex(), index);
        assertNotSame(index(source).getMaterializedIndex().getTablet(1011), index.getTablet(1011));
    }

    @Test
    public void testNestedMapKeysArePreservedIndependentlyOfIds() {
        TabletReshardJob source = job(1, true, TabletReshardJob.JobState.PENDING);
        ReshardingPhysicalPartition partition = partitions(source).remove(1001L);
        ReshardingMaterializedIndex index = partition.getReshardingIndexes().remove(1002L);
        partition.getReshardingIndexes().put(2222L, index);
        partitions(source).put(1111L, partition);
        TabletReshardJob restored = TabletReshardJob.fromProto(source.toProto());
        assertEquals(Set.of(1111L), partitions(restored).keySet());
        assertEquals(1001, partition(restored).getPhysicalPartitionId());
        assertEquals(Set.of(2222L), partition(restored).getReshardingIndexes().keySet());
        assertEquals(1002, index(restored).getMaterializedIndexId());
        GsonEquivalence.assertEquivalent(
                FinalManagersImageTestSupport.gson(source, TabletReshardJob.class), restored);
    }

    @Test
    public void testRestoredNestedContainersAreIndependentAndMutable() {
        TabletReshardJob source = job(1, false, TabletReshardJob.JobState.PENDING);
        TabletReshardJob restored = TabletReshardJob.fromProto(source.toProto());
        index(restored).getReshardingTablets().add(new IdenticalTablet(30, 31));
        partition(restored).getReshardingIndexes().put(99L, index(restored));
        partitions(restored).put(98L, partition(restored));
        assertEquals(2, partitions(restored).size());
        assertEquals(2, partition(restored).getReshardingIndexes().size());
        assertEquals(3, index(restored).getReshardingTablets().size());
        assertEquals(1, partitions(source).size());
        assertEquals(1, partition(source).getReshardingIndexes().size());
        assertEquals(2, index(source).getReshardingTablets().size());
        MergingTablet merge = (MergingTablet) index(restored).getReshardingTablets().get(0);
        merge.getOldTabletIds().add(99L);
        assertEquals(List.of(1010L, 1012L), index(source).getReshardingTablets().get(0).getOldTabletIds());
    }

    @Test
    public void testPublishFutureFailureAndBackoffAreRuntimeOnly() {
        TabletReshardJob source = job(1, true, TabletReshardJob.JobState.RUNNING);
        ReshardingPhysicalPartition partition = partition(source);
        partition.setPublishFuture(CompletableFuture.completedFuture(Map.of()));
        partition.setPublishFailureReason("old leader publish failure");
        setField(partition, "consecutivePublishFailures", 5);
        setField(partition, "nextPublishRetryTimeMs", Long.MAX_VALUE);
        setField(partition, "publishFailureAccounted", true);
        setField(source, "cleanupExcludedTransactionIds", Set.of(1L, 2L));
        TabletReshardJob restored = TabletReshardJob.fromProto(source.toProto());
        ReshardingPhysicalPartition actual = partition(restored);
        assertEquals(ReshardingPhysicalPartition.PublishState.NOT_STARTED, actual.getPublishResult().publishState());
        assertNull(actual.getPublishFailureReason());
        assertEquals(0, (int) getField(actual, "consecutivePublishFailures"));
        assertEquals(0L, (long) getField(actual, "nextPublishRetryTimeMs"));
        assertFalse((boolean) getField(actual, "publishFailureAccounted"));
        assertTrue(actual.isPublishRetryDue());
        assertNull(getField(restored, "cleanupExcludedTransactionIds"));
        assertEquals(101, actual.getCommitVersion());
    }

    @Test
    public void testNullableNestedObjectsAndEmptyLists() {
        ReshardingMaterializedIndex absent = new ReshardingMaterializedIndex(1, null, null);
        ReshardingMaterializedIndex restored = ReshardingMaterializedIndex.fromProto(absent.toProto());
        assertNull(restored.getMaterializedIndex());
        assertNull(restored.getReshardingTablets());
        ReshardingMaterializedIndex empty = new ReshardingMaterializedIndex(2, new MaterializedIndex(3), new ArrayList<>());
        restored = ReshardingMaterializedIndex.fromProto(empty.toProto());
        assertTrue(restored.getReshardingTablets().isEmpty());
        assertTrue(restored.getMaterializedIndex().getTablets().isEmpty());
        restored.getReshardingTablets().add(new IdenticalTablet(1, 2));
        ReshardingPhysicalPartition noIndexes = new ReshardingPhysicalPartition(4, null);
        assertNull(ReshardingPhysicalPartition.fromProto(noIndexes.toProto()).getReshardingIndexes());
        ReshardingPhysicalPartition emptyIndexes = new ReshardingPhysicalPartition(5, new HashMap<>());
        assertTrue(ReshardingPhysicalPartition.fromProto(emptyIndexes.toProto()).getReshardingIndexes().isEmpty());
    }

    @Test
    public void testUnknownJobKindCheckedBeforeCommonState() {
        TabletReshardJobPB good = job(1, true, TabletReshardJob.JobState.PENDING).toProto();
        TabletReshardJobPB unknown = good.toBuilder().setKindValue(900)
                .setCore(good.getCore().toBuilder().setJobStateValue(999)).build();
        assertThrows(SubtypeNotFoundException.class, () -> TabletReshardJob.fromProto(unknown));
        assertThrows(IllegalArgumentException.class,
                () -> TabletReshardJob.fromProto(good.toBuilder().clearKind().build()));
        assertThrows(IllegalArgumentException.class, () -> TabletReshardJob.fromProto(good.toBuilder()
                .setCore(good.getCore().toBuilder().setJobStateValue(999)).build()));
    }

    @Test
    public void testUnknownNestedTabletFailsWholeConversion() {
        ReshardingTabletPB unknown = ReshardingTabletPB.newBuilder().setKindValue(901).build();
        assertThrows(SubtypeNotFoundException.class, () -> ReshardingTablet.fromImageProto(unknown));
        assertThrows(IllegalArgumentException.class,
                () -> ReshardingTablet.fromImageProto(ReshardingTabletPB.getDefaultInstance()));
        TabletReshardJobPB good = job(1, true, TabletReshardJob.JobState.RUNNING).toProto();
        TabletReshardJobPB bad = ReshardImageTestSupport.withTablet(good, 1, unknown);
        assertThrows(SubtypeNotFoundException.class, () -> TabletReshardJob.fromProto(bad));
    }
}
