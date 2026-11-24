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

import com.google.protobuf.UnknownFieldSet;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.PulsarProgress;
import com.starrocks.load.routineload.PulsarRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.KafkaPartitionOffsetsPB;
import com.starrocks.persist.proto.PulsarRoutineLoadJobPB;
import com.starrocks.persist.proto.RoutineLoadJobCorePB;
import com.starrocks.persist.proto.RoutineLoadJobPB;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.ImportMetadataStmt;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RoutineLoadJobProtoTest {
    @BeforeAll
    public static void beforeClass() {
        ExtensionManager.getInstance().loadExtensionsFromClassPath("target/classes");
    }

    static final String SQL = "ALTER ROUTINE LOAD FOR db.job COLUMNS(a, b, c=a+1), WHERE a > 0, "
            + "COLUMNS TERMINATED BY ',', PARTITION(p2, p1), ROWS TERMINATED BY 'A'";

    static KafkaRoutineLoadJob kafka(long id) {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(id, "job", 10, 20, "broker:9092", "topic");
        job.setOrigStmt(new OriginStatementInfo(SQL, 0));
        return job;
    }

    static PulsarRoutineLoadJob pulsar(long id) {
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(id, "job", 10, 20,
                "pulsar://broker:6650", "persistent://tenant/ns/topic", "subscription");
        job.setOrigStmt(new OriginStatementInfo(SQL, 0));
        return job;
    }

    static RoutineLoadJob gsonCopy(RoutineLoadJob source) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source, RoutineLoadJob.class), RoutineLoadJob.class);
    }

    private static void common(RoutineLoadJob job) {
        setField(job, "desireTaskConcurrentNum", 7);
        setField(job, "state", RoutineLoadJob.JobState.PAUSED);
        setField(job, "maxFilterRatio", 0.125);
        setField(job, "maxErrorNum", 91L);
        setField(job, "jobProperties", new HashMap<>(Map.of("format", "json", "strict_mode", "true")));
        setField(job, "sessionVariables", new HashMap<>(Map.of(SessionVariable.SQL_MODE, "0",
                SessionVariable.PARSE_TOKENS_LIMIT, "10000")));
        setField(job, "taskSchedIntervalS", 17L);
        setField(job, "maxBatchRows", 7654321L);
        setField(job, "maxBatchSizeBytes", 98765432L);
        long value = 100;
        for (String field : List.of("createTimestamp", "pauseTimestamp", "endTimestamp", "currentErrorRows",
                "currentTotalRows", "errorRows", "totalRows", "unselectedRows", "receivedBytes", "totalTaskExcutionTimeMs",
                "committedTaskNum", "abortedTaskNum", "taskConsumeSecond", "taskTimeoutSecond")) {
            setField(job, field, value++);
        }
        setField(job, "warehouseId", 80L);
        setField(job, "computeResource", new WarehouseComputeResource(81));
    }

    @Test
    public void testKafkaAllPersistedFieldsAndDerivedDescription() throws Exception {
        KafkaRoutineLoadJob source = kafka(101);
        common(source);
        setField(source, "customKafkaPartitions", new ArrayList<>(List.of(9, 2, 4)));
        setField(source, "currentKafkaPartitions", new ArrayList<>(List.of(9, 2, 4, 11)));
        setField(source, "customProperties", new HashMap<>(Map.of("group.id", "consumer-1", "security.protocol", "SASL_SSL")));
        setField(source, "confluentSchemaRegistryUrl", "https://registry:8081");
        setField(source, "customeKafkaPartitionOffsets", new ArrayList<>(List.of(
                Pair.create(9, -2L), Pair.create(2, 500L), Pair.create(9, 501L))));
        setField(source, "progress", new KafkaProgress(new ConcurrentHashMap<>(Map.of(9, 400L, 2, 700L))));
        setField(source, "timestampProgress", new KafkaProgress(new ConcurrentHashMap<>(Map.of(9, 10000L, 2, 20000L))));
        KafkaRoutineLoadJob result = (KafkaRoutineLoadJob) RoutineLoadJob.fromProto(
                RoutineLoadJobPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(gsonCopy(source), result);
        assertEquals("broker:9092", result.getBrokerList());
        assertEquals("topic", result.getTopic());
        assertEquals("https://registry:8081", result.getConfluentSchemaRegistryUrl());
        assertEquals(List.of(Pair.create(9, -2L), Pair.create(2, 500L), Pair.create(9, 501L)),
                getField(result, "customeKafkaPartitionOffsets"));
        assertEquals(List.of(9, 2, 4, 11), getField(result, "currentKafkaPartitions"));
        assertEquals(Map.of(9, 400L, 2, 700L), ((KafkaProgress) result.getProgress()).getPartitionIdToOffset(List.of(9, 2)));
        assertEquals(Map.of(9, 10000L, 2, 20000L),
                ((KafkaProgress) getField(result, "timestampProgress")).getPartitionIdToOffset(List.of(9, 2)));
        assertDescription(result);
    }

    private static void assertDescription(RoutineLoadJob job) {
        assertNotNull(job.getColumnDescs());
        assertEquals(List.of("a", "b", "c"), job.getColumnDescs().stream().map(c -> c.getColumnName()).toList());
        assertNotNull(job.getColumnDescs().get(2).getExpr());
        assertNotNull(job.getWhereExpr());
        assertEquals(List.of("p2", "p1"), job.getPartitions().getPartitionNames());
        assertEquals(",", job.getColumnSeparator().getColumnSeparator());
        assertEquals("A", job.getRowDelimiter().getRowDelimiter());
    }

    @Test
    public void testPulsarAllPersistedFieldsAndRuntimeInitialization() throws Exception {
        PulsarRoutineLoadJob source = pulsar(102);
        common(source);
        setField(source, "customPulsarPartitions", new ArrayList<>(List.of("topic-partition-2", "topic-partition-0")));
        setField(source, "customProperties", new HashMap<>(Map.of("auth.token", "test-token", "receiverQueueSize", "77")));
        PulsarProgress progress = new PulsarProgress();
        setField(progress, "partitionToBacklogNum", new ConcurrentHashMap<>(Map.of("topic-partition-2", 90L)));
        setField(source, "progress", progress);
        PulsarProgress timestamps = new PulsarProgress();
        setField(timestamps, "partitionToBacklogNum", new ConcurrentHashMap<>(Map.of("topic-partition-2", 9000L)));
        setField(source, "timestampProgress", timestamps);
        setField(source, "currentPulsarPartitions", new ArrayList<>(List.of("runtime-only")));
        setField(source, "dataSourceConfigVersion", 900L);
        PulsarRoutineLoadJob result = (PulsarRoutineLoadJob) RoutineLoadJob.fromProto(
                RoutineLoadJobPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(gsonCopy(source), result);
        assertEquals("pulsar://broker:6650", result.getServiceUrl());
        assertEquals("subscription", result.getSubscription());
        assertEquals(Map.of("topic-partition-2", 90L), getField(result.getProgress(), "partitionToBacklogNum"));
        assertEquals(Map.of("topic-partition-2", 9000L),
                getField((Object) getField(result, "timestampProgress"), "partitionToBacklogNum"));
        assertTrue(((List<?>) getField(result, "currentPulsarPartitions")).isEmpty());
        assertTrue(result.getConvertedCustomProperties().isEmpty());
        assertEquals(0L, (long) getField(result, "dataSourceConfigVersion"));
        assertEquals(0, result.getSizeOfRoutineLoadTaskInfoList());
        assertDescription(result);
    }

    @Test
    public void testEveryStateRemainsUnchanged() {
        for (RoutineLoadJob.JobState state : RoutineLoadJob.JobState.values()) {
            for (RoutineLoadJob source : List.of(kafka(1), pulsar(2))) {
                setField(source, "state", state);
                setField(source, "endTimestamp", 98765L);
                RoutineLoadJob result = RoutineLoadJob.fromProto(source.toProto());
                assertEquals(state, result.getState());
                assertEquals(state.isFinalState(), result.isFinal());
                assertEquals(source.getClass(), result.getClass());
            }
        }
    }

    @Test
    public void testMissingFieldsKeepConstructorDefaults() {
        for (RoutineLoadJob source : List.of(kafka(1), pulsar(2))) {
            RoutineLoadJobPB minimal = source.toProto().toBuilder().setCore(RoutineLoadJobCorePB.newBuilder()
                    .setOrigStmt(source.getOrigStmt().toProto())).build();
            RoutineLoadJob result = RoutineLoadJob.fromProto(minimal);
            assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, result.getState());
            assertEquals(1.0, (double) getField(result, "maxFilterRatio"));
            assertEquals(1L, (long) getField(result, "totalTaskExcutionTimeMs"));
            assertEquals(-1L, (long) getField(result, "pauseTimestamp"));
            assertEquals(-1L, (long) getField(result, "endTimestamp"));
            assertEquals(Config.max_routine_load_batch_size, (long) getField(result, "maxBatchSizeBytes"));
            assertEquals(Config.routine_load_task_consume_second, (long) getField(result, "taskConsumeSecond"));
            assertEquals(Config.routine_load_task_timeout_second, (long) getField(result, "taskTimeoutSecond"));
            assertNotNull(result.getProgress());
            assertNotNull(getField(result, "timestampProgress"));
            assertNotNull(getField(result, "computeResource"));
            assertNotNull(getField(result, "lock"));
            assertTrue(((Map<?, ?>) getField(result, "jobProperties")).isEmpty());
            assertTrue(((Map<?, ?>) getField(result, "sessionVariables")).isEmpty());
        }
    }

    @Test
    public void testExplicitZeroAndEmptyValuesAreNotDefaults() {
        RoutineLoadJobPB pb = kafka(1).toProto();
        RoutineLoadJob result = RoutineLoadJob.fromProto(pb.toBuilder().setCore(pb.getCore().toBuilder()
                .setName("").setMaxFilterRatio(0).setTotalTaskExcutionTimeMs(0).setPauseTimestamp(0)
                .setTaskConsumeSecond(0).setTaskTimeoutSecond(0).setMaxBatchSizeBytes(0)).build());
        assertEquals("", result.getName());
        assertEquals(0.0, (double) getField(result, "maxFilterRatio"));
        assertEquals(0L, (long) getField(result, "totalTaskExcutionTimeMs"));
        assertEquals(0L, (long) getField(result, "pauseTimestamp"));
        assertEquals(0L, (long) getField(result, "taskConsumeSecond"));
        assertEquals(0L, (long) getField(result, "taskTimeoutSecond"));
        assertEquals(0L, (long) getField(result, "maxBatchSizeBytes"));
    }

    @Test
    public void testNullableOffsetListAndRegistryUrl() {
        KafkaRoutineLoadJob source = kafka(1);
        KafkaRoutineLoadJob absent = (KafkaRoutineLoadJob) RoutineLoadJob.fromProto(source.toProto());
        assertNull(getField(absent, "customeKafkaPartitionOffsets"));
        assertNull(absent.getConfluentSchemaRegistryUrl());
        setField(source, "customeKafkaPartitionOffsets", new ArrayList<>());
        source.setConfluentSchemaRegistryUrl("");
        KafkaRoutineLoadJob empty = (KafkaRoutineLoadJob) RoutineLoadJob.fromProto(source.toProto());
        assertEquals(List.of(), getField(empty, "customeKafkaPartitionOffsets"));
        assertEquals("", empty.getConfluentSchemaRegistryUrl());
        GsonEquivalence.assertEquivalent(gsonCopy(source), empty);
    }

    @Test
    public void testStatementIndexSqlModeAndMetadataAreRebuilt() {
        KafkaRoutineLoadJob source = kafka(1);
        String sql = "SELECT 1; CREATE ROUTINE LOAD `order` ON tbl "
                + "INCLUDE METADATA(PARTITION AS p, HEADERS AS h), COLUMNS(a, b=\"x\"||\"y\") "
                + "FROM KAFKA (\"kafka_topic\"=\"topic\")";
        source.setOrigStmt(new OriginStatementInfo(sql, 1));
        setField(source, "sessionVariables", new HashMap<>(Map.of(SessionVariable.SQL_MODE,
                Long.toString(SqlModeHelper.MODE_PIPES_AS_CONCAT))));
        RoutineLoadJob result = RoutineLoadJob.fromProto(source.toProto());
        RoutineLoadJob v2 = gsonCopy(source);
        ImportMetadataStmt metadata = getField(result, "metadata");
        assertNotNull(metadata);
        assertEquals(((ImportMetadataStmt) getField(v2, "metadata")).toSql(), metadata.toSql());
        assertEquals(AstToSQLBuilder.toSQL(v2.getColumnDescs().get(1).getExpr()),
                AstToSQLBuilder.toSQL(result.getColumnDescs().get(1).getExpr()));
        assertTrue(AstToSQLBuilder.toSQL(result.getColumnDescs().get(1).getExpr()).toLowerCase().contains("concat"));
        assertEquals(1, result.getOrigStmt().idx);
        assertEquals(sql, result.getOrigStmt().originStmt);
    }

    @Test
    public void testDecodedCollectionsCanBeChangedDuringReplay() {
        KafkaRoutineLoadJob result = (KafkaRoutineLoadJob) RoutineLoadJob.fromProto(kafka(1).toProto());
        Map<String, String> properties = getField(result, "customProperties");
        properties.put("group.id", "new-group");
        List<Integer> partitions = getField(result, "currentKafkaPartitions");
        partitions.add(7);
        Map<String, String> session = getField(result, "sessionVariables");
        session.put("new-key", "new-value");
        KafkaProgress progress = (KafkaProgress) result.getProgress();
        Map<Integer, Long> offsets = getField(progress, "partitionIdToOffset");
        offsets.put(7, 700L);
        assertEquals(Map.of(7, 700L), progress.getPartitionIdToOffset(List.of(7)));
        assertEquals("new-group", properties.get("group.id"));
        assertEquals(List.of(7), partitions);
    }

    @Test
    public void testUnknownFieldsAreForwardCompatible() {
        RoutineLoadJobPB pb = kafka(1).toProto();
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder().addField(1000,
                UnknownFieldSet.Field.newBuilder().addVarint(123).build()).build();
        RoutineLoadJob result = RoutineLoadJob.fromProto(pb.toBuilder().setUnknownFields(unknown)
                .setCore(pb.getCore().toBuilder().setUnknownFields(unknown)).build());
        GsonEquivalence.assertEquivalent(gsonCopy(kafka(1)), result, java.util.Set.of("\\$.c"), java.util.Set.of());
        assertDescription(result);
    }

    @Test
    public void testInvalidKindsTypesAndProgressAreRejected() {
        RoutineLoadJobPB valid = kafka(1).toProto();
        List<RoutineLoadJobPB> invalid = List.of(
                valid.toBuilder().clearKind().build(),
                valid.toBuilder().clearCore().build(),
                valid.toBuilder().clearKafka().build(),
                valid.toBuilder().setPulsar(PulsarRoutineLoadJobPB.getDefaultInstance()).build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder().setStateValue(999)).build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder()
                        .setDataSourceType(RoutineLoadJobCorePB.SourceType.PULSAR)).build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder().setProgress(new PulsarProgress().toProto())).build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder()
                        .setTimestampProgress(new PulsarProgress().toProto())).build(),
                valid.toBuilder().setKafka(valid.getKafka().toBuilder().setCustomeKafkaPartitionOffsets(
                        KafkaPartitionOffsetsPB.newBuilder().addValues(KafkaPartitionOffsetsPB.Entry.newBuilder()
                                .setPartition(1)))).build());
        for (RoutineLoadJobPB bad : invalid) {
            assertThrows(IllegalArgumentException.class, () -> RoutineLoadJob.fromProto(bad));
        }
    }
}
