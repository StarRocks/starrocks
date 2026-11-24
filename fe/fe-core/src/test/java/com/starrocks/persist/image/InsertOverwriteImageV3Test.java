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

import com.starrocks.load.InsertOverwriteJob;
import com.starrocks.load.InsertOverwriteJobMgr;
import com.starrocks.load.InsertOverwriteJobState;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.InsertOverwriteJobPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertOverwriteImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
    }

    static InsertOverwriteJob job(long id, long tableId, InsertOverwriteJobState state) {
        InsertOverwriteJob job = new InsertOverwriteJob(id, 12, tableId, new ArrayList<>(List.of(31L, 32L)), true);
        job.setJobState(state);
        job.setTmpPartitionIds(new ArrayList<>(List.of(41L, 42L)));
        job.setSourcePartitionNames(new ArrayList<>(List.of("p1", "p2")));
        job.setTxnId(67L);
        setField(job, "warehouseId", 88L);
        return job;
    }

    private InsertOverwriteJobMgr v2(InsertOverwriteJobMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        InsertOverwriteJobMgr result = new InsertOverwriteJobMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private InsertOverwriteJobMgr v3(InsertOverwriteJobMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        InsertOverwriteJobMgr result = new InsertOverwriteJobMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(List.of(220, 221), sections.stream().map(IndexEntryPB::getType).toList());
        assertTrue(sections.stream().allMatch(s -> s.getManagerLength() == 0));
        return result;
    }

    @Test
    public void testAllFieldsAndStates() throws Exception {
        for (InsertOverwriteJobState state : InsertOverwriteJobState.values()) {
            InsertOverwriteJob source = job(123, 456, state);
            InsertOverwriteJob restored = InsertOverwriteJob.fromProto(
                    InsertOverwriteJobPB.parseFrom(source.toProto().toByteArray()));
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(source.isFinished(), restored.isFinished());
            assertNull(restored.getInsertStmt());
            assertEquals(88L, restored.getWarehouseId());
            assertEquals(67L, restored.getTxnId());
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        }
    }

    @Test
    public void testDefaultAndNullableLists() {
        InsertOverwriteJob source = new InsertOverwriteJob();
        InsertOverwriteJobPB pb = source.toProto();
        assertFalse(pb.hasSourcePartitionIds());
        assertFalse(pb.hasTmpPartitionIds());
        assertFalse(pb.hasSourcePartitionNames());
        InsertOverwriteJob restored = InsertOverwriteJob.fromProto(pb);
        GsonEquivalence.assertEquivalent(source, restored);
        assertEquals(-1L, InsertOverwriteJob.fromProto(InsertOverwriteJobPB.getDefaultInstance()).getTxnId());
        source.setSourcePartitionIds(new ArrayList<>());
        source.setTmpPartitionIds(new ArrayList<>());
        source.setSourcePartitionNames(new ArrayList<>());
        assertTrue(source.toProto().hasSourcePartitionIds());
        restored = InsertOverwriteJob.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(source, restored);
        restored.getSourcePartitionIds().add(1L);
        restored.getSourcePartitionNames().add("new");
        assertTrue(source.getSourcePartitionIds().isEmpty());
        assertTrue(source.getSourcePartitionNames().isEmpty());
    }

    @Test
    public void testDynamicOverwriteAndLegacyMissingTransaction() {
        InsertOverwriteJob legacy = GsonUtils.GSON.fromJson(
                "{\"jobId\":1,\"dynamicOverwrite\":true,\"sourcePartitionNames\":[\"p1\"]}", InsertOverwriteJob.class);
        InsertOverwriteJob restored = InsertOverwriteJob.fromProto(legacy.toProto());
        assertTrue(restored.isDynamicOverwrite());
        assertEquals(-1, restored.getTxnId());
        assertNull(restored.getTmpPartitionIds());
        GsonEquivalence.assertEquivalent(legacy, restored);
        for (long txnId : List.of(-1L, 0L, Long.MAX_VALUE)) {
            legacy.setTxnId(txnId);
            assertEquals(txnId, InsertOverwriteJob.fromProto(legacy.toProto()).getTxnId());
        }
    }

    @Test
    public void testEmptyManagerAndRepeatedLoadReplaceMaps() throws Exception {
        InsertOverwriteJobMgr restored = v3(new InsertOverwriteJobMgr());
        assertEquals(0, restored.getRunningJobSize());
        restored.registerOverwriteJob(job(1, 10, InsertOverwriteJobState.OVERWRITE_RUNNING));
        Path empty = LoadImageTestSupport.save(tempDir, new InsertOverwriteJobMgr()::save);
        LoadImageTestSupport.load(empty, restored.getImageV3Loaders());
        assertNull(restored.getInsertOverwriteJob(1));
        assertFalse(restored.hasRunningOverwriteJob(10));

    }

    @Test
    public void testIndependentTableIndexAndListOrderMatchV2() throws Exception {
        InsertOverwriteJobMgr source = new InsertOverwriteJobMgr();
        source.registerOverwriteJob(job(1, 10, InsertOverwriteJobState.OVERWRITE_RUNNING));
        source.registerOverwriteJob(job(2, 10, InsertOverwriteJobState.OVERWRITE_PENDING));
        Map<Long, List<Long>> index = getField(source, "tableToOverwriteJobs");
        index.put(10L, new ArrayList<>(List.of(2L, 1L)));
        index.put(20L, new ArrayList<>());
        index.put(30L, new ArrayList<>(List.of(998L, 999L)));
        InsertOverwriteJobMgr expected = v2(source);
        InsertOverwriteJobMgr restored = v3(source);
        GsonEquivalence.assertEquivalent(expected, restored);
        Map<Long, List<Long>> actualIndex = getField(restored, "tableToOverwriteJobs");
        assertEquals(List.of(2L, 1L), actualIndex.get(10L));
        assertTrue(actualIndex.get(20L).isEmpty());
        assertEquals(List.of(998L, 999L), actualIndex.get(30L));
        assertTrue(restored.deregisterOverwriteJob(2L));
        assertEquals(List.of(1L), actualIndex.get(10L));
        assertTrue(restored.hasRunningOverwriteJob(10));
        restored.deregisterOverwriteJob(1L);
        assertFalse(restored.hasRunningOverwriteJob(10));
        assertNull(restored.getInsertOverwriteJob(1L));
        assertEquals(List.of(2L, 1L), index.get(10L));

    }

    @Test
    public void testRunningJobsAreRebuiltOnlyOutsideCheckpoint() throws Exception {
        for (boolean checkpoint : List.of(false, true)) {
            new MockUp<GlobalStateMgr>() {
                @Mock
                public boolean isCheckpointThread() {
                    return checkpoint;
                }
            };
            InsertOverwriteJobMgr source = new InsertOverwriteJobMgr();
            int id = 1;
            for (InsertOverwriteJobState state : InsertOverwriteJobState.values()) {
                source.registerOverwriteJob(job(id++, 10, state));
            }
            InsertOverwriteJobMgr expected = v2(source);
            InsertOverwriteJobMgr restored = v3(source);
            assertEquals(expected.getRunningJobSize(), restored.getRunningJobSize());
            assertEquals(checkpoint ? 0 : 2, restored.getRunningJobSize());
            List<InsertOverwriteJob> running = getField(restored, "runningJobs");
            for (InsertOverwriteJob run : running) {
                assertFalse(run.isFinished());
                assertSame(restored.getInsertOverwriteJob(run.getJobId()), run);
            }

        }
    }

    @Test
    public void testJobsAreIndividualEntriesAndIndexContainsOnlyIds() throws Exception {
        InsertOverwriteJobMgr source = new InsertOverwriteJobMgr();
        for (int i = 1; i <= 1024; i++) {
            source.registerOverwriteJob(job(i, 10, InsertOverwriteJobState.OVERWRITE_RUNNING));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        InsertOverwriteJobMgr restored = new InsertOverwriteJobMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(1024L, 1L), sections.stream().map(IndexEntryPB::getNumEntries).toList());
        for (int i = 1; i <= 1024; i++) {
            GsonEquivalence.assertEquivalent(source.getInsertOverwriteJob(i), restored.getInsertOverwriteJob(i));
            restored.deregisterOverwriteJob(i);
        }
        assertFalse(restored.hasRunningOverwriteJob(10));

    }

    @Test
    public void testNullIndexValuesFollowV2Omission() throws Exception {
        InsertOverwriteJobMgr source = new InsertOverwriteJobMgr();
        source.registerOverwriteJob(job(1, 10, InsertOverwriteJobState.OVERWRITE_RUNNING));
        Map<Long, List<Long>> index = getField(source, "tableToOverwriteJobs");
        index.put(20L, null);
        InsertOverwriteJobMgr expected = v2(source);
        assertFalse(expected.hasRunningOverwriteJob(20));
        InsertOverwriteJobMgr restored = v3(source);
        assertFalse(restored.hasRunningOverwriteJob(20));
        assertTrue(restored.hasRunningOverwriteJob(10));
        Map<Long, List<Long>> expectedIndex = getField(expected, "tableToOverwriteJobs");
        Map<Long, List<Long>> actualIndex = getField(restored, "tableToOverwriteJobs");
        assertEquals(expectedIndex, actualIndex);

    }
}
