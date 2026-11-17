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
import com.starrocks.alter.reshard.ReshardingMaterializedIndex;
import com.starrocks.alter.reshard.ReshardingPhysicalPartition;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.ReshardingTabletInfo;
import com.starrocks.alter.reshard.SplittingTablet;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.common.Config;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.ReshardingTabletPB;
import com.starrocks.persist.proto.TabletReshardJobPB;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ReshardImageTestSupport.index;
import static com.starrocks.persist.image.ReshardImageTestSupport.job;
import static com.starrocks.persist.image.ReshardImageTestSupport.partition;
import static com.starrocks.persist.image.ReshardImageTestSupport.partitions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TabletReshardMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    private TabletReshardJobMgr manager() {
        TabletReshardJobMgr manager = new TabletReshardJobMgr();
        when(global.getTabletReshardJobMgr()).thenReturn(manager);
        return manager;
    }

    private TabletReshardJobMgr v2(TabletReshardJobMgr source) throws Exception {
        TabletReshardJobMgr restored = manager();
        FinalManagersImageTestSupport.v2(source::save, restored::load);
        return restored;
    }

    private TabletReshardJobMgr v3(TabletReshardJobMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        TabletReshardJobMgr restored = manager();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(360, sections.get(0).getType());
        assertEquals(source.getTabletReshardJobs().size(), sections.get(0).getNumEntries());
        assertEquals(0, sections.get(0).getManagerLength());
        return restored;
    }

    private void assertMappings(TabletReshardJobMgr manager, TabletReshardJob job) {
        TabletReshardJob.JobState state = getField(job, "jobState");
        boolean shouldRegister = state == TabletReshardJob.JobState.PREPARING
                || state == TabletReshardJob.JobState.RUNNING || state == TabletReshardJob.JobState.CLEANING;
        for (ReshardingPhysicalPartition partition : partitions(job).values()) {
            for (ReshardingMaterializedIndex index : partition.getReshardingIndexes().values()) {
                for (ReshardingTablet tablet : index.getReshardingTablets()) {
                    for (long oldId : tablet.getOldTabletIds()) {
                        assertNull(manager.getReshardingTablet(oldId, partition.getCommitVersion() - 1));
                        if (shouldRegister) {
                            assertSame(tablet, manager.getReshardingTablet(oldId, partition.getCommitVersion()));
                            assertSame(tablet, manager.getReshardingTablet(oldId, partition.getCommitVersion() + 1));
                        } else {
                            assertNull(manager.getReshardingTablet(oldId, Long.MAX_VALUE));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testV2AndV3RebuildMappingsForEveryJobState() throws Exception {
        TabletReshardJobMgr source = manager();
        long id = 1;
        for (boolean split : List.of(true, false)) {
            for (TabletReshardJob.JobState state : TabletReshardJob.JobState.values()) {
                TabletReshardJob job = job(id++, split, state);
                source.getTabletReshardJobs().put(job.getJobId(), job);
            }
        }
        TabletReshardJobMgr expected = v2(source);
        TabletReshardJobMgr restored = v3(source);
        GsonEquivalence.assertEquivalent(expected, restored);
        Map<Long, ReshardingTabletInfo> expectedMappings = getField(expected, "reshardingTabletInfos");
        Map<Long, ReshardingTabletInfo> actualMappings = getField(restored, "reshardingTabletInfos");
        assertEquals(expectedMappings.keySet(), actualMappings.keySet());
        for (long oldId : expectedMappings.keySet()) {
            assertEquals(expectedMappings.get(oldId).getVisibleVersion(), actualMappings.get(oldId).getVisibleVersion());
            GsonEquivalence.assertEquivalent(expectedMappings.get(oldId).getReshardingTablet(),
                    actualMappings.get(oldId).getReshardingTablet());
        }
        for (TabletReshardJob job : restored.getTabletReshardJobs().values()) {
            assertMappings(restored, job);
            for (TabletReshardJob recovered : List.of(job, expected.getTabletReshardJob(job.getJobId()))) {
                assertEquals(true, (boolean) getField(recovered, "admissionReady"));
                TabletReshardJob.JobState state = getField(recovered, "jobState");
                assertEquals(state == TabletReshardJob.JobState.CLEANING || state == TabletReshardJob.JobState.FINISHED,
                        (boolean) getField(recovered, "layoutReady"));
                assertNull(getField(recovered, "pendingCleaningLog"));
            }
        }

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testDerivedMapIsNotSerializedFromPreviousLeader() throws Exception {
        TabletReshardJobMgr source = manager();
        TabletReshardJob pending = job(1, true, TabletReshardJob.JobState.PENDING);
        source.getTabletReshardJobs().put(1L, pending);
        Map<Long, ReshardingTabletInfo> mappings = getField(source, "reshardingTabletInfos");
        mappings.put(9999L, new ReshardingTabletInfo(new IdenticalTablet(9999, 10000), 10));
        TabletReshardJobMgr restored = v3(source);
        assertNull(restored.getReshardingTablet(9999, Long.MAX_VALUE));
        assertTrue(((Map<?, ?>) getField(restored, "reshardingTabletInfos")).isEmpty());
        assertMappings(restored, restored.getTabletReshardJob(1));

    }

    @Test
    public void testUnknownNestedTabletSkipsWholeJobWithoutPartialMappings() throws Exception {
        TabletReshardJobPB first = job(1, true, TabletReshardJob.JobState.RUNNING).toProto();
        TabletReshardJobPB bad = ReshardImageTestSupport.withTablet(
                job(2, true, TabletReshardJob.JobState.RUNNING).toProto(), 1,
                ReshardingTabletPB.newBuilder().setKindValue(900).build());
        TabletReshardJobPB last = job(3, true, TabletReshardJob.JobState.RUNNING).toProto();
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.TABLET_RESHARD_JOB_MGR,
                    List.of(first, bad, last));
            Config.metadata_ignore_unknown_subtype = false;
            TabletReshardJobMgr strict = manager();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, strict.getImageV3Loaders()));
            assertEquals(Set.of(1L), strict.getTabletReshardJobs().keySet());
            assertNull(strict.getReshardingTablet(2010, Long.MAX_VALUE));
            Config.metadata_ignore_unknown_subtype = true;
            TabletReshardJobMgr tolerant = manager();
            LoadImageTestSupport.load(path, tolerant.getImageV3Loaders());
            assertEquals(Set.of(1L, 3L), tolerant.getTabletReshardJobs().keySet());
            assertNull(tolerant.getReshardingTablet(2010, Long.MAX_VALUE));
            assertNull(tolerant.getReshardingTablet(2020, Long.MAX_VALUE));
            assertMappings(tolerant, tolerant.getTabletReshardJob(1));
            assertMappings(tolerant, tolerant.getTabletReshardJob(3));

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testUnknownTopLevelKindDoesNotPreventFollowingJob() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = true;
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.TABLET_RESHARD_JOB_MGR,
                    List.of(TabletReshardJobPB.newBuilder().setKindValue(999).build(),
                            job(2, false, TabletReshardJob.JobState.CLEANING).toProto()));
            TabletReshardJobMgr restored = manager();
            LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(Set.of(2L), restored.getTabletReshardJobs().keySet());
            assertMappings(restored, restored.getTabletReshardJob(2));

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testMissingKindAndUnknownStateAlwaysFail() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = true;
            TabletReshardJobPB good = job(1, true, TabletReshardJob.JobState.RUNNING).toProto();
            for (TabletReshardJobPB bad : List.of(good.toBuilder().clearKind().build(), good.toBuilder()
                    .setCore(good.getCore().toBuilder().setJobStateValue(900)).build())) {
                Path path = FinalManagersImageTestSupport.entries(tempDir,
                        SectionId.TABLET_RESHARD_JOB_MGR, List.of(bad));
                TabletReshardJobMgr restored = manager();
                assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, restored.getImageV3Loaders()));
                assertTrue(restored.getTabletReshardJobs().isEmpty());
                assertTrue(((Map<?, ?>) getField(restored, "reshardingTabletInfos")).isEmpty());
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testRestoredMappingsCanBeUnregisteredByOriginalCleanup() throws Exception {
        for (boolean split : List.of(true, false)) {
            TabletReshardJobMgr source = manager();
            source.getTabletReshardJobs().put(1L, job(1, split, TabletReshardJob.JobState.CLEANING));
            TabletReshardJobMgr restored = v3(source);
            TabletReshardJob job = restored.getTabletReshardJob(1);
            assertMappings(restored, job);
            for (TabletReshardJob recovered : List.of(job)) {
                assertEquals(true, (boolean) getField(recovered, "admissionReady"));
                TabletReshardJob.JobState state = getField(recovered, "jobState");
                assertEquals(state == TabletReshardJob.JobState.CLEANING || state == TabletReshardJob.JobState.FINISHED,
                        (boolean) getField(recovered, "layoutReady"));
                assertNull(getField(recovered, "pendingCleaningLog"));
            }
            invoke(job, "unregisterReshardingTablets");
            assertTrue(((Map<?, ?>) getField(restored, "reshardingTabletInfos")).isEmpty());
            assertNotNull(restored.getTabletReshardJob(1));

        }
        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testMultiplePartitionsRetainTheirOwnVisibleVersionThresholds() throws Exception {
        TabletReshardJob sourceJob = job(1, true, TabletReshardJob.JobState.RUNNING);
        ReshardingPhysicalPartition second = partition(job(2, true, TabletReshardJob.JobState.RUNNING));
        second.setCommitVersion(500);
        partitions(sourceJob).put(second.getPhysicalPartitionId(), second);
        TabletReshardJobMgr source = manager();
        source.getTabletReshardJobs().put(1L, sourceJob);
        TabletReshardJobMgr restored = v3(source);
        assertNull(restored.getReshardingTablet(1010, 100));
        assertNotNull(restored.getReshardingTablet(1010, 101));
        assertNull(restored.getReshardingTablet(2010, 499));
        assertNotNull(restored.getReshardingTablet(2010, 500));
        assertEquals(2, partitions(restored.getTabletReshardJob(1)).size());
        assertMappings(restored, restored.getTabletReshardJob(1));

    }

    @Test
    public void testRestoredIdenticalFallbackStillRegistersItsOldTablet() throws Exception {
        TabletReshardJob sourceJob = job(1, true, TabletReshardJob.JobState.RUNNING);
        ((SplittingTablet) index(sourceJob).getReshardingTablets().get(0)).fallbackToIdenticalTablet();
        TabletReshardJobMgr source = manager();
        source.getTabletReshardJobs().put(1L, sourceJob);
        TabletReshardJobMgr restored = v3(source);
        ReshardingTablet tablet = restored.getReshardingTablet(1010, 101);
        assertNotNull(tablet);
        assertNotNull(tablet.getIdenticalTablet());
        assertEquals(List.of(1011L), tablet.getNewTabletIds());
        assertSame(index(restored.getTabletReshardJob(1)).getReshardingTablets().get(0), tablet);

    }

    @Test
    public void testLoadDoesNotPublishOrRetainLeaderRetryState() throws Exception {
        TabletReshardJob sourceJob = job(1, true, TabletReshardJob.JobState.RUNNING);
        partition(sourceJob).setPublishFuture(CompletableFuture.completedFuture(Map.of()));
        partition(sourceJob).setPublishFailureReason("retry from old leader");
        setField(partition(sourceJob), "nextPublishRetryTimeMs", Long.MAX_VALUE);
        TabletReshardJobMgr source = manager();
        source.getTabletReshardJobs().put(1L, sourceJob);
        TabletReshardJobMgr restored = v3(source);
        ReshardingPhysicalPartition partition = partition(restored.getTabletReshardJob(1));
        assertNull(partition.getPublishFailureReason());
        assertTrue(partition.isPublishRetryDue());
        assertEquals(ReshardingPhysicalPartition.PublishState.NOT_STARTED, partition.getPublishResult().publishState());

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testLargeJobListHasOneEntryPerJob() throws Exception {
        TabletReshardJobMgr source = manager();
        for (int i = 1; i <= 1024; i++) {
            source.getTabletReshardJobs().put((long) i, job(i, i % 2 == 0, TabletReshardJob.JobState.PENDING));
        }
        TabletReshardJobMgr expected = v2(source);
        TabletReshardJobMgr restored = v3(source);
        assertEquals(1024, restored.getTabletReshardJobs().size());
        assertTrue(((Map<?, ?>) getField(restored, "reshardingTabletInfos")).isEmpty());
        GsonEquivalence.assertEquivalent(expected, restored);
        restored.replayRemoveTabletReshardJob(1024L);
        assertNull(restored.getTabletReshardJob(1024L));
        assertEquals(1023, restored.getTabletReshardJobs().size());

    }

    @Test
    public void testEmptyImageKeepsExistingManagerStateLikeV2() throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, new TabletReshardJobMgr()::save);
        TabletReshardJobMgr restored = manager();
        TabletReshardJob existing = job(1, true, TabletReshardJob.JobState.RUNNING);
        restored.getTabletReshardJobs().put(1L, existing);
        invoke(existing, "registerReshardingTabletsOnRestart");
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertSame(existing, restored.getTabletReshardJob(1));
        assertMappings(restored, existing);

    }
}
