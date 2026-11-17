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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.ExecuteOptionPB;
import com.starrocks.persist.proto.MVTaskRunExtraMessagePB;
import com.starrocks.persist.proto.TaskCorePB;
import com.starrocks.persist.proto.TaskRunStatePB;
import com.starrocks.persist.proto.TaskRunStatusPB;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskSchedule;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskPersistenceImageV3Test {
    static Task task(long id) {
        Task task = new Task("task-" + id);
        task.setId(id);
        task.setSource(Constants.TaskSource.CTAS);
        task.setDbName("db");
        task.setCatalogName("default_catalog");
        task.setDefinition("insert into t values (1)");
        task.setProperties(new HashMap<>());
        return task;
    }

    static TaskRunStatus status(long id, Constants.TaskRunState state) {
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskId(id);
        status.setTaskName("task-" + id);
        status.setQueryId("query-" + id);
        status.setTaskRunId("run-" + id);
        status.setState(state);
        status.setCreateTime(10000 + id);
        status.setExpireTime(Long.MAX_VALUE);
        status.setDbName("db");
        status.setSource(Constants.TaskSource.CTAS);
        return status;
    }

    @Test
    public void testTaskCoreFieldsAndSchedule() throws Exception {
        Task source = task(123);
        source.setType(Constants.TaskType.PERIODICAL);
        source.setState(Constants.TaskState.PAUSE);
        source.setSchedule(new TaskSchedule(12345, 17, TimeUnit.MINUTES));
        setField(source, "postRun", "analyze table t");
        setField(source, "createUser", "legacy-user");
        setField(source, "userIdentity", new UserIdentity("operator", "%"));
        setField(source, "expireTime", Long.MAX_VALUE);
        setField(source, "lastScheduleTime", 31L);
        setField(source, "nextScheduleTime", 48L);
        source.getProperties().put("warehouse", "wh");
        Task restored = Task.fromTaskCoreProto(TaskCorePB.parseFrom(source.toTaskCoreProto().toByteArray()));
        GsonEquivalence.assertEquivalent(source, restored);
        assertEquals(TimeUnit.MINUTES, restored.getSchedule().getTimeUnit());
        restored.getProperties().put("new", "value");
        assertFalse(source.getProperties().containsKey("new"));
    }

    @Test
    public void testTaskEnumsAndOldDatabasePrefix() throws Exception {
        for (Constants.TaskType type : Constants.TaskType.values()) {
            for (Constants.TaskState state : Constants.TaskState.values()) {
                for (Constants.TaskSource source : Constants.TaskSource.values()) {
                    Task task = task(1);
                    task.setType(type);
                    task.setState(state);
                    task.setSource(source);
                    Task restored = Task.fromTaskCoreProto(task.toTaskCoreProto());
                    GsonEquivalence.assertEquivalent(task, restored);
                }
            }
        }
        Task restored = Task.fromTaskCoreProto(task(1).toTaskCoreProto().toBuilder()
                .setDbName("default_cluster:db").build());
        assertEquals("db", restored.getDbName());
    }

    @Test
    public void testStatusAllFields() throws Exception {
        TaskRunStatus source = status(123, Constants.TaskRunState.FAILED);
        setField(source, "startTaskRunId", "batch-1");
        setField(source, "catalogName", "catalog");
        setField(source, "definition", "insert into t select * from s");
        setField(source, "postRun", "analyze table t");
        setField(source, "user", "legacy-user");
        setField(source, "userIdentity", new UserIdentity("owner", "%"));
        setField(source, "submitUser", "operator");
        setField(source, "priority", 41);
        setField(source, "mergeRedundant", true);
        setField(source, "errorCode", 1064);
        setField(source, "errorMessage", "test error");
        setField(source, "finishTime", 4321L);
        setField(source, "processStartTime", 1234L);
        setField(source, "progress", 73);
        setField(source, "dataCacheSelectExtraMessage", "{\"readBytes\":123456}");
        setField(source, "properties", new HashMap<>(Map.of("warehouse", "wh")));
        setField(source, "mvTaskRunExtraMessage", extra());
        TaskRunStatus restored = TaskRunStatus.fromProto(TaskRunStatusPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(source, restored, Set.of(),
                Set.of(".*PartitionsToRefresh", ".*PartitionsToRefreshMap\\.[^.]+"));
        assertEquals("operator", restored.getSubmitUser());
        assertEquals("test error", restored.getErrorMessage());
        assertEquals(4321L, restored.getFinishTime());
        assertEquals(1234L, restored.getProcessStartTime());
        TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
    }

    static MVTaskRunExtraMessage extra() {
        MVTaskRunExtraMessage extra = new MVTaskRunExtraMessage();
        setField(extra, "forceRefresh", true);
        setField(extra, "partitionStart", "2026-01-01");
        setField(extra, "partitionEnd", "2026-01-03");
        setField(extra, "mvPartitionsToRefresh", Set.of("p1", "p2"));
        setField(extra, "refBasePartitionsToRefreshMap", Map.of("ref", Set.of("p1", "p2")));
        setField(extra, "basePartitionsToRefreshMap", Map.of("base", Set.of("p3", "p4")));
        setField(extra, "nextPartitionStart", "2026-01-03");
        setField(extra, "nextPartitionEnd", "2026-01-05");
        setField(extra, "nextPartitionValues", "[\"2026-01-03\"]");
        setField(extra, "processStartTime", 987654L);
        ExecuteOption option = new ExecuteOption(47, true, Map.of("warehouse", "wh"));
        option.setManual(true);
        option.setSync(true);
        option.setReplay(true);
        setField(extra, "executeOption", option);
        setField(extra, "planBuilderMessage", Map.of("partitions", "4"));
        setField(extra, "adaptivePartitionRefreshNumber", 17);
        setField(extra, "refreshMode", "PCT");
        setField(extra, "pinnedSnapshotIdMap", Map.of("iceberg.db.t", Long.MAX_VALUE));
        setField(extra, "imvSourceVersionRange", Map.of("cat.db.t", Map.of("start", "10", "end", "20")));
        setField(extra, "imvSourceTimestampRange", Map.of("cat.db.t", Map.of("start", "1000", "end", "2000")));
        return extra;
    }

    @Test
    public void testExtraAllFieldsAndMutableCollections() throws Exception {
        MVTaskRunExtraMessage source = extra();
        MVTaskRunExtraMessage restored = MVTaskRunExtraMessage.fromProto(
                MVTaskRunExtraMessagePB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(source, restored, Set.of(),
                Set.of(".*PartitionsToRefresh", ".*PartitionsToRefreshMap\\.[^.]+"));
        TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        restored.getPinnedSnapshotIdMap().put("other", 22L);
        restored.getImvSourceVersionRange().get("cat.db.t").put("start", "30");
        restored.getBasePartitionsToRefreshMap().get("base").add("p5");
        assertFalse(source.getPinnedSnapshotIdMap().containsKey("other"));
        assertEquals("10", source.getImvSourceVersionRange().get("cat.db.t").get("start"));
        assertEquals(Set.of("p3", "p4"), source.getBasePartitionsToRefreshMap().get("base"));
    }

    @Test
    public void testExtraLoadDoesNotApplyCurrentTruncationConfig() {
        int old = Config.max_mv_task_run_meta_message_values_length;
        try {
            MVTaskRunExtraMessage source = extra();
            MVTaskRunExtraMessagePB pb = source.toProto();
            Config.max_mv_task_run_meta_message_values_length = 1;
            MVTaskRunExtraMessage restored = MVTaskRunExtraMessage.fromProto(pb);
            assertEquals(2, restored.getMvPartitionsToRefresh().size());
            assertEquals(2, restored.getBasePartitionsToRefreshMap().get("base").size());
            GsonEquivalence.assertEquivalent(source, restored, Set.of(),
                    Set.of(".*PartitionsToRefresh", ".*PartitionsToRefreshMap\\.[^.]+"));
        } finally {
            Config.max_mv_task_run_meta_message_values_length = old;
        }
    }

    @Test
    public void testStatusSerializesRawStartId() throws Exception {
        TaskRunStatus source = status(1, Constants.TaskRunState.SUCCESS);
        setField(source, "startTaskRunId", "raw-batch");
        setField(source, "properties", Map.of(TaskRun.START_TASK_RUN_ID, "effective-batch"));
        assertEquals("effective-batch", source.getStartTaskRunId());
        assertEquals("raw-batch", source.toProto().getStartTaskRunId());
        TaskRunStatus restored = TaskRunStatus.fromProto(source.toProto());
        assertEquals("raw-batch", getField(restored, "startTaskRunId"));
        assertEquals("effective-batch", restored.getStartTaskRunId());
        GsonEquivalence.assertEquivalent(source, restored);
    }

    @Test
    public void testExecuteOptionSerializesRawMergeFlagAndOmitsSubmitter() {
        boolean old = Config.enable_mv_refresh_sync_refresh_mergeable;
        try {
            Config.enable_mv_refresh_sync_refresh_mergeable = false;
            ExecuteOption source = new ExecuteOption(23, true, Map.of("warehouse", "wh"));
            source.setManual(true);
            source.setSync(true);
            source.setReplay(true);
            source.setSubmitUser("operator");
            assertFalse(source.isMergeRedundant());
            assertTrue(source.toProto().getIsMergeRedundant());
            ExecuteOption restored = ExecuteOption.fromProto(source.toProto());
            GsonEquivalence.assertEquivalent(source, restored);
            assertFalse(restored.isMergeRedundant());
            Config.enable_mv_refresh_sync_refresh_mergeable = true;
            assertTrue(restored.isMergeRedundant());
            assertNull(restored.getSubmitUser());
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        } finally {
            Config.enable_mv_refresh_sync_refresh_mergeable = old;
        }
    }

    @Test
    public void testDefaultAndAbsentTaskFields() throws Exception {
        TaskRunStatus source = new TaskRunStatus();
        GsonEquivalence.assertEquivalent(source, TaskRunStatus.fromProto(source.toProto()));
        GsonEquivalence.assertEquivalent(new MVTaskRunExtraMessage(),
                MVTaskRunExtraMessage.fromProto(MVTaskRunExtraMessagePB.getDefaultInstance()));
        ExecuteOption option = ExecuteOption.fromProto(ExecuteOptionPB.getDefaultInstance());
        assertNull(option.getTaskRunProperties());
        assertFalse(option.isReplay());
        assertFalse(option.isManual());
        assertEquals(-1, MVTaskRunExtraMessage.fromProto(
                MVTaskRunExtraMessagePB.getDefaultInstance()).getAdaptivePartitionRefreshNumber());
        TaskRunStatus missing = TaskRunStatus.fromProto(TaskRunStatusPB.getDefaultInstance());
        assertEquals(Constants.TaskRunState.PENDING, missing.getState());
        assertEquals(Constants.TaskSource.UNKNOWN, missing.getSource());
        assertNotNull(missing.getMvTaskRunExtraMessage());
    }

    @Test
    public void testUnknownStatusEnumsMatchGson() throws Exception {
        TaskRunStatus oldReader = GsonUtils.GSON.fromJson(
                "{\"state\":\"FUTURE_STATE\",\"source\":\"FUTURE_SOURCE\"}", TaskRunStatus.class);
        TaskRunStatus fromProto = TaskRunStatus.fromProto(TaskRunStatusPB.newBuilder()
                .setStateValue(999).setSourceValue(999).build());
        assertNull(oldReader.getState());
        assertNull(fromProto.getState());
        assertNull(fromProto.getSource());
        GsonEquivalence.assertEquivalent(oldReader, fromProto);
        assertNull(TaskRunStatus.fromProto(TaskRunStatusPB.newBuilder()
                .setState(TaskRunStatePB.TASK_RUN_STATE_UNSPECIFIED).build()).getState());
    }

    @Test
    public void testEveryStatusEnum() throws Exception {
        for (Constants.TaskRunState state : Constants.TaskRunState.values()) {
            for (Constants.TaskSource source : Constants.TaskSource.values()) {
                TaskRunStatus status = status(1, state);
                status.setSource(source);
                GsonEquivalence.assertEquivalent(status, TaskRunStatus.fromProto(status.toProto()));
            }
        }
    }

    @Test
    public void testNullAndEmptyProperties() throws Exception {
        for (Map<String, String> properties : List.of(Map.<String, String>of(), Map.of("a", "b"))) {
            ExecuteOption source = new ExecuteOption(0, false, properties);
            GsonEquivalence.assertEquivalent(source, ExecuteOption.fromProto(source.toProto()));
            TaskRunStatus status = status(1, Constants.TaskRunState.SUCCESS);
            setField(status, "properties", properties);
            GsonEquivalence.assertEquivalent(status, TaskRunStatus.fromProto(status.toProto()));
        }
        ExecuteOption source = new ExecuteOption(0, false, null);
        assertFalse(source.toProto().hasTaskRunProperties());
        assertNull(ExecuteOption.fromProto(source.toProto()).getTaskRunProperties());
    }

    @Test
    public void testNullMapValuesFollowV2Omission() throws Exception {
        TaskRunStatus source = status(1, Constants.TaskRunState.FAILED);
        Map<String, String> properties = new HashMap<>();
        properties.put("retained", "v");
        properties.put("absent", null);
        setField(source, "properties", properties);
        TaskRunStatus v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), TaskRunStatus.class);
        GsonEquivalence.assertEquivalent(v2, TaskRunStatus.fromProto(source.toProto()));
    }

    @Test
    public void testMissingAndExplicitNullStateAreDifferentFromUnknown() throws Exception {
        TaskRunStatus absent = GsonUtils.GSON.fromJson("{}", TaskRunStatus.class);
        TaskRunStatus explicitNull = GsonUtils.GSON.fromJson("{\"state\":null}", TaskRunStatus.class);
        assertEquals(Constants.TaskRunState.PENDING, absent.getState());
        assertNull(explicitNull.getState());
        // v2 omits null fields on save, so a null state round-trips back to PENDING.
        TaskRunStatus v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(explicitNull), TaskRunStatus.class);
        GsonEquivalence.assertEquivalent(v2, TaskRunStatus.fromProto(explicitNull.toProto()));
    }
}
