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
import com.starrocks.common.Pair;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.ComputeResourcePB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.StreamLoadMultiStmtTaskPB;
import com.starrocks.persist.proto.StreamLoadTaskMapPB;
import com.starrocks.persist.proto.StreamLoadTaskPB;
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
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.StreamLoadPersistenceImageV3Test.multi;
import static com.starrocks.persist.image.StreamLoadPersistenceImageV3Test.task;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamLoadMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = TaskImageTestSupport.global();
    }

    private StreamLoadMgr v2(StreamLoadMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        LoadImageTestSupport.transactions(global);
        StreamLoadMgr result = new StreamLoadMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private StreamLoadMgr v3(StreamLoadMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        LoadImageTestSupport.transactions(global);
        StreamLoadMgr result = new StreamLoadMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(List.of(240, 241), sections.stream().map(IndexEntryPB::getType).toList());
        assertTrue(sections.stream().allMatch(s -> s.getManagerLength() == 0));
        return result;
    }

    private Map<String, AbstractStreamLoadTask> byLabel(StreamLoadMgr manager) {
        return manager.getAllTasks().stream().collect(Collectors.toMap(AbstractStreamLoadTask::getLabel, t -> t));
    }

    @Test
    public void testBothTaskTypesAndIndexesMatchV2() throws Exception {
        StreamLoadMgr source = new StreamLoadMgr();
        source.addLoadTask(task(1, StreamLoadTask.State.PREPARED));
        source.addLoadTask(task(2, StreamLoadTask.State.FINISHED));
        source.addLoadTask(multi(3, StreamLoadMultiStmtTask.State.ABORTING));
        source.addLoadTask(multi(4, StreamLoadMultiStmtTask.State.FINISHED));
        StreamLoadMgr expected = v2(source);
        StreamLoadMgr restored = v3(source);
        GsonEquivalence.assertEquivalent(byLabel(expected), byLabel(restored));
        Map<Long, Map<String, AbstractStreamLoadTask>> dbIndex = getField(restored, "dbToLabelToStreamLoadTask");
        assertEquals(Set.of(10L), dbIndex.keySet());
        assertEquals(byLabel(restored).keySet(), dbIndex.get(10L).keySet());
        for (AbstractStreamLoadTask task : restored.getAllTasks()) {
            assertSame(task, restored.getTaskByLabel(task.getLabel()));
            assertSame(task, dbIndex.get(task.getDBId()).get(task.getLabel()));
            assertSame(task, global.getGlobalTransactionMgr().getCallbackFactory().getCallback(task.getId()));
        }

    }

    @Test
    public void testOnlyParentRegistersTransactionCallback() throws Exception {
        StreamLoadMgr source = new StreamLoadMgr();
        source.addLoadTask(multi(1, StreamLoadMultiStmtTask.State.ABORTING));
        StreamLoadMgr restored = v3(source);
        StreamLoadMultiStmtTask parent = (StreamLoadMultiStmtTask) restored.getTaskByLabel("multi-1");
        assertSame(parent, global.getGlobalTransactionMgr().getCallbackFactory().getCallback(1L));
        for (StreamLoadTask child : parent.getTasks()) {
            assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(child.getId()));
            assertNull(restored.getTaskByLabel(child.getLabel()));
        }

    }

    @Test
    public void testExpiredTasksSkippedWithoutRegisteringCallback() throws Exception {
        StreamLoadMgr source = new StreamLoadMgr();
        StreamLoadTask expired = task(1, StreamLoadTask.State.FINISHED);
        setField(expired, "endTimeMs", 1L);
        StreamLoadMultiStmtTask expiredMulti = multi(2, StreamLoadMultiStmtTask.State.CANCELLED);
        setField(expiredMulti, "endTimeMs", 1L);
        source.addLoadTask(expired);
        source.addLoadTask(expiredMulti);
        source.addLoadTask(task(3, StreamLoadTask.State.BEGIN));
        source.addLoadTask(multi(4, StreamLoadMultiStmtTask.State.ABORTING));
        StreamLoadMgr expected = v2(source);
        StreamLoadMgr restored = v3(source);
        assertEquals(Set.of("stream-3", "multi-4"), byLabel(restored).keySet());
        GsonEquivalence.assertEquivalent(byLabel(expected), byLabel(restored));
        assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(1L));
        assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(2L));

    }

    @Test
    public void testSyncIndexFollowsV2TransientFlag() throws Exception {
        StreamLoadMgr source = new StreamLoadMgr();
        StreamLoadTask sync = task(1, StreamLoadTask.State.BEGIN);
        sync.setIsSyncStreamLoad(true);
        source.addLoadTask(sync);
        Map<Long, StreamLoadTask> originalIndex = getField(source, "txnIdToSyncStreamLoadTasks");
        assertEquals(Set.of(sync.getTxnId()), originalIndex.keySet());
        StreamLoadMgr expected = v2(source);
        StreamLoadMgr restored = v3(source);
        Map<Long, StreamLoadTask> index = getField(restored, "txnIdToSyncStreamLoadTasks");
        Map<Long, StreamLoadTask> expectedIndex = getField(expected, "txnIdToSyncStreamLoadTasks");
        assertEquals(expectedIndex.keySet(), index.keySet());
        assertTrue(index.isEmpty());
        assertFalse(((StreamLoadTask) restored.getTaskByLabel("stream-1")).isSyncStreamLoad());

    }

    @Test
    public void testUnknownNestedComputeResourceSkipsWholeParentOnlyWhenConfigured() throws Exception {
        StreamLoadTaskPB badChild = task(1001, StreamLoadTask.State.BEGIN).toProto().toBuilder()
                .setComputeResource(ComputeResourcePB.newBuilder().setKindValue(999)).build();
        StreamLoadMultiStmtTaskPB badParent = multi(1, StreamLoadMultiStmtTask.State.ABORTING).toProto().toBuilder()
                .setTaskMaps(StreamLoadTaskMapPB.newBuilder().putValues("child", badChild)).build();
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            try (SectionWriter section = writer.beginSection(SectionId.STREAM_LOAD_TASK)) {
                section.writeEntry(task(3, StreamLoadTask.State.BEGIN).toProto());
            }
            try (SectionWriter section = writer.beginSection(SectionId.STREAM_LOAD_MULTI_STMT_TASK)) {
                section.writeEntry(badParent);
                section.writeEntry(multi(2, StreamLoadMultiStmtTask.State.ABORTING).toProto());
            }
        });
        boolean old = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = false;
            StreamLoadMgr strict = new StreamLoadMgr();
            assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(path, strict.getImageV3Loaders()));
            Config.metadata_ignore_unknown_subtype = true;
            LoadImageTestSupport.transactions(global);
            StreamLoadMgr restored = new StreamLoadMgr();
            LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(Set.of("stream-3", "multi-2"), byLabel(restored).keySet());
            assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(1L));
            assertNotNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(2L));
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }

    }

    @Test
    public void testEmptySections() throws Exception {
        assertTrue(v3(new StreamLoadMgr()).getAllTasks().isEmpty());

    }

    @Test
    public void testEachTaskHasItsOwnTypedEntry() throws Exception {
        int old = Config.stream_load_task_keep_max_num;
        try {
            Config.stream_load_task_keep_max_num = 10000;
            StreamLoadMgr source = new StreamLoadMgr();
            for (int i = 1; i <= 1024; i++) {
                source.addLoadTask(task(i, StreamLoadTask.State.BEGIN));
            }
            source.addLoadTask(multi(2000, StreamLoadMultiStmtTask.State.ABORTING));
            Path path = LoadImageTestSupport.save(tempDir, source::save);
            LoadImageTestSupport.transactions(global);
            StreamLoadMgr restored = new StreamLoadMgr();
            List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(List.of(1024L, 1L), sections.stream().map(IndexEntryPB::getNumEntries).toList());
            assertEquals(1025, restored.getAllTasks().size());
            GsonEquivalence.assertEquivalent(byLabel(source), byLabel(restored));

        } finally {
            Config.stream_load_task_keep_max_num = old;
        }
    }

    @Test
    public void testBothSectionsShareCutoffCapturedWhenLoadingStarts() throws Exception {
        StreamLoadMgr source = new StreamLoadMgr();
        source.addLoadTask(task(1, StreamLoadTask.State.BEGIN));
        source.addLoadTask(multi(2, StreamLoadMultiStmtTask.State.ABORTING));
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        List<Long> ordinaryCutoffs = new ArrayList<>();
        List<Long> multiCutoffs = new ArrayList<>();
        new MockUp<StreamLoadTask>() {
            @Mock
            public boolean checkNeedRemove(long currentMs, boolean force) throws InterruptedException {
                ordinaryCutoffs.add(currentMs);
                // Cross a clock tick so recomputing the cutoff for the second list is observable.
                Thread.sleep(30);
                return false;
            }
        };
        new MockUp<StreamLoadMultiStmtTask>() {
            @Mock
            public boolean checkNeedRemove(long currentMs, boolean force) {
                multiCutoffs.add(currentMs);
                return false;
            }
        };
        StreamLoadMgr restored = new StreamLoadMgr();
        List<Pair<SectionId, SectionLoader>> loaders = restored.getImageV3Loaders();
        Thread.sleep(30);
        long beforeLoad = System.currentTimeMillis();
        LoadImageTestSupport.load(path, loaders);
        assertEquals(1, ordinaryCutoffs.size());
        assertEquals(ordinaryCutoffs, multiCutoffs);
        assertTrue(ordinaryCutoffs.get(0) >= beforeLoad);
        LoadImageTestSupport.load(path, loaders);
        assertEquals(2, ordinaryCutoffs.size());
        assertEquals(ordinaryCutoffs, multiCutoffs);
        assertTrue(ordinaryCutoffs.get(1) > ordinaryCutoffs.get(0));

    }

    @Test
    public void testMultiStatementOnlyImageGetsCurrentCutoff() throws Exception {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.FINISHED);
        setField(source, "endTimeMs", -1L);
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            try (SectionWriter section = writer.beginSection(SectionId.STREAM_LOAD_MULTI_STMT_TASK)) {
                section.writeEntry(source.toProto());
            }
        });
        long before = System.currentTimeMillis();
        StreamLoadMgr restored = new StreamLoadMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        long after = System.currentTimeMillis();
        AbstractStreamLoadTask loaded = restored.getTaskByLabel("multi-1");
        assertNotNull(loaded);
        long restoredEndTime = getField(loaded, "endTimeMs");
        assertTrue(restoredEndTime >= before);
        assertTrue(restoredEndTime <= after);

    }
}
