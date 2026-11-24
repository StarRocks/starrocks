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

import com.starrocks.common.Config;
import com.starrocks.lake.snapshot.ClusterSnapshot;
import com.starrocks.lake.snapshot.ClusterSnapshot.ClusterSnapshotType;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.persist.proto.ClusterSnapshotJobPB;
import com.starrocks.persist.proto.ClusterSnapshotMgrPB;
import com.starrocks.persist.proto.ClusterSnapshotPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;

public class ClusterSnapshotMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    static ClusterSnapshotJob job(long id, ClusterSnapshotJobState state) {
        ClusterSnapshotJob job = new ClusterSnapshotJob(id, "snapshot-" + id, "volume-" + id, 10000 + id);
        setField(job, "state", state);
        job.setJournalIds(123456789000L + id, 234567890000L + id);
        job.getSnapshot().setFinishedTimeMs(20000 + id);
        job.setErrMsg("previous error " + id);
        setField(job, "detailInfo", "upload detail " + id);
        return job;
    }

    private ClusterSnapshotMgr restore(ClusterSnapshotMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        ClusterSnapshotMgr restored = new ClusterSnapshotMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(340, sections.get(0).getType());
        assertEquals(source.getAutomatedSnapshotJobs().size(), sections.get(0).getNumEntries());
        assertTrue(sections.get(0).getManagerLength() > 0);
        return restored;
    }

    @Test
    public void testAllJobStatesAndSnapshotTypesPreserveEveryField() throws Exception {
        for (ClusterSnapshotJobState state : ClusterSnapshotJobState.values()) {
            for (ClusterSnapshotType type : ClusterSnapshotType.values()) {
                ClusterSnapshotJob source = job(17, state);
                setField(source.getSnapshot(), "type", type);
                FinalManagersImageTestSupport.assertAllFieldsPresent(source);
                FinalManagersImageTestSupport.assertAllFieldsPresent(source.getSnapshot());
                ClusterSnapshotJob restored = ClusterSnapshotJob.fromProto(
                        ClusterSnapshotJobPB.parseFrom(source.toProto().toByteArray()));
                GsonEquivalence.assertEquivalent(source, restored);
                assertEquals(source.isUnFinishedState(), restored.isUnFinishedState());
                assertEquals(source.isFinalState(), restored.isFinalState());
                assertEquals(source.isAutomated(), restored.isAutomated());
                assertEquals(source.getFeJournalId(), restored.getFeJournalId());
                assertEquals(source.getStarMgrJournalId(), restored.getStarMgrJournalId());
                assertEquals(source.getCreatedTimeMs(), restored.getCreatedTimeMs());
                assertEquals(source.getFinishedTimeMs(), restored.getFinishedTimeMs());
            }
        }
    }

    @Test
    public void testManagerAndSelectionAgreeWithV2() throws Exception {
        ClusterSnapshotMgr source = new ClusterSnapshotMgr();
        setField(source, "storageVolumeName", "backup-volume");
        setField(source, "automatedSnapshotIntervalSeconds", 7200L);
        source.getAutomatedSnapshotJobs().put(8L, job(8, ClusterSnapshotJobState.UPLOADING));
        source.getAutomatedSnapshotJobs().put(1L, job(1, ClusterSnapshotJobState.FINISHED));
        source.getAutomatedSnapshotJobs().put(3L, job(3, ClusterSnapshotJobState.FINISHED));
        source.getAutomatedSnapshotJobs().put(7L, job(7, ClusterSnapshotJobState.ERROR));
        source.getAutomatedSnapshotJobs().put(6L, job(6, ClusterSnapshotJobState.ERROR));
        ClusterSnapshotMgr expected = new ClusterSnapshotMgr();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        ClusterSnapshotMgr restored = restore(source);
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals("backup-volume", restored.getAutomatedSnapshotSvName());
        assertEquals(7200L, restored.getAutomatedSnapshotIntervalSeconds());
        assertEquals(3L, restored.getLastFinishedAutomatedClusterSnapshotJob().getId());
        assertEquals(8L, restored.getUnfinishedClusterSnapshotJob().getId());
        assertEquals(20003L, restored.getLastSuccessTimeMs());
        assertEquals(2, restored.getConsecutiveFailureCount());
        assertEquals(3L, restored.getAutomatedSnapshot().getId());
        assertNull(getField(restored, "clusterSnapshotJobScheduler"));

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testNullableAndEmptySnapshotFieldsRemainDistinct() {
        ClusterSnapshot source = new ClusterSnapshot();
        GsonEquivalence.assertEquivalent(source, ClusterSnapshot.fromProto(source.toProto()));
        ClusterSnapshotPB empty = ClusterSnapshotPB.newBuilder().setSnapshotName("").setStorageVolumeName("").build();
        ClusterSnapshot restored = ClusterSnapshot.fromProto(empty);
        assertEquals("", restored.getSnapshotName());
        assertEquals("", restored.getStorageVolumeName());
        assertNull(getField(restored, "type"));
        ClusterSnapshotJob restoredJob = ClusterSnapshotJob.fromProto(ClusterSnapshotJobPB.getDefaultInstance());
        assertNull(restoredJob.getSnapshot());
        assertNull(restoredJob.getState());
        assertNull(restoredJob.getErrMsg());
        assertNull(getField(restoredJob, "detailInfo"));
    }

    @Test
    public void testFinishedLoadDoesNotRunStateTransitionOrReplaceFinishTime() throws Exception {
        ClusterSnapshotMgr source = new ClusterSnapshotMgr();
        source.getAutomatedSnapshotJobs().put(1L, job(1, ClusterSnapshotJobState.FINISHED));
        source.getAutomatedSnapshotJobs().put(2L, job(2, ClusterSnapshotJobState.FINISHED));
        ClusterSnapshotMgr restored = restore(source);
        assertEquals(2, restored.getAutomatedSnapshotJobs().size());
        assertEquals(20001L, restored.getAutomatedSnapshotJobs().get(1L).getFinishedTimeMs());
        assertEquals(20002L, restored.getAutomatedSnapshotJobs().get(2L).getFinishedTimeMs());
        assertTrue(restored.getAutomatedSnapshotJobs().get(1L).isFinished());
        assertTrue(restored.getAutomatedSnapshotJobs().get(2L).isFinished());

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testCleanupUsesRestoredJobIdOrder() throws Exception {
        ClusterSnapshotMgr source = new ClusterSnapshotMgr();
        source.getAutomatedSnapshotJobs().put(10L, job(10, ClusterSnapshotJobState.ERROR));
        source.getAutomatedSnapshotJobs().put(4L, job(4, ClusterSnapshotJobState.UPLOADING));
        source.getAutomatedSnapshotJobs().put(2L, job(2, ClusterSnapshotJobState.ERROR));
        source.getAutomatedSnapshotJobs().put(1L, job(1, ClusterSnapshotJobState.ERROR));
        ClusterSnapshotMgr expected = new ClusterSnapshotMgr();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        ClusterSnapshotMgr restored = restore(source);
        expected.removeAutomatedFinalizeJobs(2);
        restored.removeAutomatedFinalizeJobs(2);
        assertEquals(List.of(4L, 10L), List.copyOf(restored.getAutomatedSnapshotJobs().keySet()));
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(10L, restored.getClusterSnapshotJobByName("snapshot-10").getId());
        assertNull(restored.getClusterSnapshotJobByName("snapshot-1"));

    }

    @Test
    public void testEmptyManagerReplacesOldJobsAndConfig() throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, new ClusterSnapshotMgr()::save);
        ClusterSnapshotMgr restored = new ClusterSnapshotMgr();
        restored.getAutomatedSnapshotJobs().put(1L, job(1, ClusterSnapshotJobState.ERROR));
        setField(restored, "storageVolumeName", "old-volume");
        setField(restored, "automatedSnapshotIntervalSeconds", 99L);
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertTrue(restored.getAutomatedSnapshotJobs().isEmpty());
        assertNull(restored.getAutomatedSnapshotSvName());
        assertEquals(0L, restored.getAutomatedSnapshotIntervalSeconds());
        assertNull(restored.getLastFinishedAutomatedClusterSnapshotJob());
        assertNull(restored.getUnfinishedClusterSnapshotJob());
        assertEquals(0, restored.getConsecutiveFailureCount());
        assertEquals(0L, restored.getLastSuccessTimeMs());

    }

    @Test
    public void testOrdinaryUnknownStateFailsEvenWhenIgnoringSubtypes() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            for (boolean ignore : List.of(false, true)) {
                Config.metadata_ignore_unknown_subtype = ignore;
                ClusterSnapshotJobPB bad = job(2, ClusterSnapshotJobState.ERROR).toProto().toBuilder()
                        .setStateValue(900).build();
                Path path = LoadImageTestSupport.save(tempDir, writer -> {
                    try (SectionWriter section = writer.beginSection(SectionId.CLUSTER_SNAPSHOT_MGR)) {
                        section.writeManager(ClusterSnapshotMgrPB.getDefaultInstance());
                        section.writeEntry(bad);
                    }
                });
                ClusterSnapshotMgr restored = new ClusterSnapshotMgr();
                assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, restored.getImageV3Loaders()));
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
        assertThrows(IllegalArgumentException.class, () -> ClusterSnapshot.fromProto(
                ClusterSnapshotPB.newBuilder().setTypeValue(901).build()));
    }

    @Test
    public void testLargeJobListKeepsJobsOutsideManagerMessage() throws Exception {
        ClusterSnapshotMgr source = new ClusterSnapshotMgr();
        for (int i = 1024; i >= 1; i--) {
            source.getAutomatedSnapshotJobs().put((long) i, job(i, ClusterSnapshotJobState.ERROR));
        }
        ClusterSnapshotMgr restored = restore(source);
        assertEquals(1024, restored.getAutomatedSnapshotJobs().size());
        assertEquals(1L, restored.getAutomatedSnapshotJobs().firstKey());
        assertEquals(1024L, restored.getAutomatedSnapshotJobs().lastKey());
        assertEquals(1024, restored.getConsecutiveFailureCount());

    }
}
