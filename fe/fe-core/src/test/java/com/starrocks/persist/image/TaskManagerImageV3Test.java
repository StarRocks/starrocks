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
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.TaskCorePB;
import com.starrocks.persist.proto.TaskRunStatusPB;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.persist.image.TaskPersistenceImageV3Test.status;
import static com.starrocks.persist.image.TaskPersistenceImageV3Test.task;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskManagerImageV3Test {
    @TempDir
    Path tempDir;
    private boolean oldArchive;
    private boolean oldUnitTest;
    private int oldMaxHistory;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
        oldArchive = Config.enable_task_history_archive;
        oldMaxHistory = Config.task_runs_max_history_number;
        oldUnitTest = FeConstants.runningUnitTest;
        Config.enable_task_history_archive = false;
        Config.task_runs_max_history_number = 10000;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_task_history_archive = oldArchive;
        Config.task_runs_max_history_number = oldMaxHistory;
        FeConstants.runningUnitTest = oldUnitTest;
    }

    private TaskManager v2(TaskManager source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.saveTasksV2(image.getImageWriter());
        TaskManager result = new TaskManager();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.loadTasksV2(reader);
        reader.close();
        return result;
    }

    private TaskManager v3(TaskManager source) throws Exception {
        TaskManager result = new TaskManager();
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(List.of(200, 201), sections.stream().map(IndexEntryPB::getType).toList());
        assertTrue(sections.stream().allMatch(s -> s.getManagerLength() == 0));
        return result;
    }

    private static Map<String, TaskRunStatus> statuses(TaskManager manager) {
        return manager.getMatchedTaskRunStatus(null).stream()
                .collect(Collectors.toMap(TaskRunStatus::getQueryId, value -> value));
    }

    @Test
    public void testEmptySections() throws Exception {
        TaskManager restored = v3(new TaskManager());
        assertTrue(restored.getMatchedTaskRunStatus(null).isEmpty());

    }

    @Test
    public void testTaskAndTerminalHistoryMatchV2() throws Exception {
        TaskManager source = new TaskManager();
        source.replayCreateTask(task(1));
        source.replayCreateTask(task(2));
        int id = 1;
        for (Constants.TaskRunState state : List.of(Constants.TaskRunState.FAILED, Constants.TaskRunState.SUCCESS,
                Constants.TaskRunState.MERGED, Constants.TaskRunState.SKIPPED)) {
            TaskRunStatus run = status(id++, state);
            run.setProgress(37);
            source.getTaskRunHistory().addHistory(run);
        }
        TaskManager expected = v2(source);
        TaskManager restored = v3(source);
        GsonEquivalence.assertEquivalent(expected.getTask("task-1"), restored.getTask("task-1"));
        GsonEquivalence.assertEquivalent(expected.getTask("task-2"), restored.getTask("task-2"));
        GsonEquivalence.assertEquivalent(statuses(expected), statuses(restored));
        assertEquals(37, statuses(restored).get("query-1").getProgress());
        assertEquals(100, statuses(restored).get("query-2").getProgress());
        assertEquals(100, statuses(restored).get("query-3").getProgress());
        assertEquals(0, statuses(restored).get("query-4").getProgress());

    }

    @Test
    public void testRunningBecomesFailedAndPendingIsRebuilt() throws Exception {
        TaskManager source = new TaskManager();
        Task task = task(1);
        source.replayCreateTask(task);
        TaskRunStatus pending = status(1, Constants.TaskRunState.PENDING);
        pending.setSubmitUser("manual-user");
        source.replayCreateTaskRun(pending);
        TaskRunStatus running = status(2, Constants.TaskRunState.RUNNING);
        // Place RUNNING in the snapshot without invoking recovery ahead of the image load.
        source.getTaskRunHistory().addHistory(running);
        TaskManager expected = v2(source);
        TaskManager restored = v3(source);
        TaskRun run = restored.getTaskRunScheduler().getTaskRunByQueryId(1L, "query-1");
        assertNotNull(run);
        assertTrue(run.getExecuteOption().isReplay());
        assertEquals("manual-user", run.getStatus().getSubmitUser());
        assertEquals(Constants.TaskRunState.FAILED, statuses(restored).get("query-2").getState());
        // Rebuilding a pending run computes new expiry from current time; compare the persisted inputs and state.
        assertEquals(statuses(expected).keySet(), statuses(restored).keySet());
        assertEquals(pending.getCreateTime(), run.getStatus().getCreateTime());
        assertEquals(task.getDefinition(), run.getTask().getDefinition());

    }

    @Test
    public void testExpiredTaskAndHistoryAreSkipped() throws Exception {
        TaskManager source = new TaskManager();
        Task expired = task(1);
        source.replayCreateTask(expired);
        expired.setExpireTime(1);
        source.replayCreateTask(task(2));
        TaskRunStatus expiredStatus = status(1, Constants.TaskRunState.SUCCESS);
        expiredStatus.setExpireTime(1);
        source.getTaskRunHistory().addHistory(expiredStatus);
        source.getTaskRunHistory().addHistory(status(2, Constants.TaskRunState.FAILED));
        TaskManager expected = v2(source);
        TaskManager restored = v3(source);
        assertNull(restored.getTask("task-1"));
        assertNotNull(restored.getTask("task-2"));
        assertEquals(Set.of("query-2"), statuses(restored).keySet());
        GsonEquivalence.assertEquivalent(statuses(expected), statuses(restored));

    }

    @Test
    public void testUnknownStateAndInvalidHistoryDoNotStopNextEntry() throws Exception {
        for (boolean ignoreUnknownSubtype : List.of(false, true)) {
            boolean old = Config.metadata_ignore_unknown_subtype;
            try {
                Config.metadata_ignore_unknown_subtype = ignoreUnknownSubtype;
                Path path = LoadImageTestSupport.save(tempDir, writer -> {
                    try (SectionWriter section = writer.beginSection(SectionId.TASK)) {
                        section.writeEntry(task(1).toTaskCoreProto());
                    }
                    try (SectionWriter section = writer.beginSection(SectionId.TASK_RUN_STATUS)) {
                        section.writeEntry(status(1, Constants.TaskRunState.SUCCESS).toProto().toBuilder()
                                .setStateValue(999).build());
                        section.writeEntry(status(2, Constants.TaskRunState.SUCCESS).toProto().toBuilder()
                                .clearTaskName().build());
                        section.writeEntry(status(3, Constants.TaskRunState.PENDING).toProto());
                        section.writeEntry(status(4, Constants.TaskRunState.SUCCESS).toProto());
                    }
                });
                TaskManager restored = new TaskManager();
                LoadImageTestSupport.load(path, restored.getImageV3Loaders());
                assertEquals(Set.of("query-4"), statuses(restored).keySet());
            } finally {
                Config.metadata_ignore_unknown_subtype = old;
            }
        }

    }

    @Test
    public void testSaveRunsHistoryGcBeforeCountingEntries() throws Exception {
        Config.task_runs_max_history_number = 2;
        TaskManager source = new TaskManager();
        for (int i = 1; i <= 5; i++) {
            source.getTaskRunHistory().addHistory(status(i, Constants.TaskRunState.SUCCESS));
        }
        TaskManager restored = v3(source);
        assertEquals(Set.of("query-4", "query-5"), statuses(source).keySet());
        assertEquals(statuses(source).keySet(), statuses(restored).keySet());

    }

    @Test
    public void testEachTaskAndStatusIsAnEntry() throws Exception {
        TaskManager source = new TaskManager();
        for (int i = 1; i <= 1024; i++) {
            source.replayCreateTask(task(i));
            source.getTaskRunHistory().addHistory(status(i, Constants.TaskRunState.SUCCESS));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        List<Long> ids = new ArrayList<>();
        List<String> queries = new ArrayList<>();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, List.of(
                Pair.create(SectionId.TASK, reader -> reader.readEntries(TaskCorePB.parser(), pb -> ids.add(pb.getId()))),
                Pair.create(SectionId.TASK_RUN_STATUS, reader ->
                        reader.readEntries(TaskRunStatusPB.parser(), pb -> queries.add(pb.getQueryId())))));
        assertEquals(List.of(1024L, 1024L), sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertEquals(1024, Set.copyOf(ids).size());
        assertEquals(1024, Set.copyOf(queries).size());
        TaskManager restored = new TaskManager();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1024, statuses(restored).size());

    }

    @Test
    public void testPendingWithoutSubmitterUsesExistingSystemFallback() throws Exception {
        TaskManager source = new TaskManager();
        source.replayCreateTask(task(1));
        source.replayCreateTaskRun(status(1, Constants.TaskRunState.PENDING));
        TaskManager restored = v3(source);
        TaskRun run = restored.getTaskRunScheduler().getTaskRunByQueryId(1L, "query-1");
        assertNotNull(run);
        assertEquals("system", run.getStatus().getSubmitUser());
        assertTrue(run.getExecuteOption().isReplay());

    }
}
