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
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.Pair;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.FailMsg;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.load.loadv2.BulkLoadJob;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.SparkLoadAppHandle;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.persist.BrokerPropertiesPersistInfo;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.BrokerLoadJobPB;
import com.starrocks.persist.proto.InsertLoadJobPB;
import com.starrocks.persist.proto.JSONOptionsPB;
import com.starrocks.persist.proto.LoadJobCorePB;
import com.starrocks.persist.proto.LoadJobPB;
import com.starrocks.persist.proto.ResourcePB;
import com.starrocks.persist.proto.SparkLoadAppHandlePB;
import com.starrocks.persist.proto.SparkLoadFilesPB;
import com.starrocks.persist.proto.SparkLoadJobPB;
import com.starrocks.qe.SessionVariable;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoadJobProtoTest {
    @BeforeAll
    public static void beforeClass() {
        ExtensionManager.getInstance().loadExtensionsFromClassPath("target/classes");
    }

    static BrokerLoadJob broker(long id, JobState state) {
        BrokerLoadJob job = new BrokerLoadJob();
        identity(job, id, state);
        return job;
    }

    static SparkLoadJob spark(long id, JobState state) {
        SparkLoadJob job = new SparkLoadJob();
        identity(job, id, state);
        return job;
    }

    static InsertLoadJob insert(long id, JobState state) {
        InsertLoadJob job = new InsertLoadJob();
        identity(job, id, state);
        setField(job, "tableId", 20L);
        return job;
    }

    private static void identity(LoadJob job, long id, JobState state) {
        setField(job, "id", id);
        setField(job, "dbId", 10L);
        setField(job, "label", "label");
        setField(job, "state", state);
        setField(job, "createTimestamp", 1234L);
        setField(job, "finishTimestamp", System.currentTimeMillis());
    }

    static LoadJob gsonCopy(LoadJob source) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source, LoadJob.class), LoadJob.class);
    }

    private static void common(LoadJob job) {
        setField(job, "timeoutSecond", 7654L);
        setField(job, "loadMemLimit", 9876543210L);
        setField(job, "maxFilterRatio", 0.25);
        setField(job, "strictMode", true);
        setField(job, "timezone", "Asia/Shanghai");
        setField(job, "partialUpdate", true);
        setField(job, "partialUpdateMode", "column");
        setField(job, "priority", 2);
        setField(job, "logRejectedRecordNum", 100L);
        setField(job, "loadStartTimestamp", 2345L);
        setField(job, "loadCommittedTimestamp", 3456L);
        setField(job, "transactionId", 789L);
        setField(job, "failMsg", new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED, "bad rows"));
        EtlStatus status = new EtlStatus();
        status.setTrackingUrl("https://tracking/job");
        setField(status, "counters", new HashMap<>(Map.of("dpp.norm.ALL", "15", "dpp.abnorm.ALL", "2")));
        setField(status, "stats", new HashMap<>(Map.of("phase", "loading")));
        setField(job, "loadingStatus", status);
        setField(job, "progress", 61);
        setField(job, "warehouseId", 55L);
        setField(job, "computeResource", new WarehouseComputeResource(56));
        setField(job, "mergeCondition", "event_time");
        setField(job, "user", "load_user");
        LoadJob.JSONOptions options = new LoadJob.JSONOptions();
        options.stripOuterArray = true;
        options.jsonPaths = "[\"$.a\"]";
        options.jsonRoot = "$.payload";
        options.envelope = "debezium";
        setField(job, "jsonOptions", options);
    }

    private static void bulk(BulkLoadJob job) {
        BulkLoadJob.setTableIds(job, new java.util.HashSet<>(java.util.Set.of(501L, 502L)));
        setField(job, "brokerPersistInfo", new BrokerPropertiesPersistInfo("broker", Map.of("username", "user")));
        setField(job, "originStmt", new OriginStatementInfo("LOAD LABEL db.label (DATA INFILE('s3://bucket/file') "
                + "INTO TABLE tbl) WITH BROKER 'broker'", 3));
        setField(job, "sessionVariables", new HashMap<>(Map.of(SessionVariable.SQL_MODE, "0",
                SessionVariable.ENABLE_TABLET_PRE_SPLIT, "false", "current.qualified.user", "load_user")));
    }

    @Test
    public void testBrokerCommonAndBulkFields() throws Exception {
        BrokerLoadJob source = broker(101, JobState.LOADING);
        common(source);
        bulk(source);
        BrokerLoadJob result = (BrokerLoadJob) LoadJob.fromProto(LoadJobPB.parseFrom(
                assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(gsonCopy(source), result);
        assertEquals(101, result.getId());
        assertEquals(JobState.LOADING, result.getState());
        assertEquals(61, result.getProgress());
        assertEquals(789L, (long) getField(result, "transactionId"));
        OriginStatementInfo original = getField(result, "originStmt");
        assertEquals(3, original.idx);
        assertTrue(original.originStmt.contains("s3://bucket/file"));
        assertEquals("false", ((Map<?, ?>) getField(result, "sessionVariables")).get(SessionVariable.ENABLE_TABLET_PRE_SPLIT));
        BrokerPropertiesPersistInfo broker = getField(result, "brokerPersistInfo");
        assertEquals("broker", broker.getName());
        assertEquals(Map.of("username", "user"), broker.getProperties());
        LoadJob.JSONOptions json = getField(result, "jsonOptions");
        assertTrue(json.stripOuterArray);
        assertEquals("$.payload", json.jsonRoot);
        assertEquals("debezium", json.envelope);
    }

    @Test
    public void testInsertFieldsAndTransientCoordinator() throws Exception {
        InsertLoadJob source = insert(102, JobState.FINISHED);
        common(source);
        setField(source, "estimateScanRow", 99999L);
        InsertLoadJob result = (InsertLoadJob) LoadJob.fromProto(LoadJobPB.parseFrom(
                assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(gsonCopy(source), result);
        assertEquals(20L, (long) getField(result, "tableId"));
        assertNull(getField(result, "coordinator"));
        assertNull(getField(result, "txnCallback"));
        assertNull(getField(result, "loadType"));
        assertEquals(0L, (long) getField(result, "estimateScanRow"));
        assertTrue(result.isCompleted());
    }

    static SparkResource resource() {
        SparkResource resource = new SparkResource("spark");
        setField(resource, "sparkConfigs", new HashMap<>(Map.of("spark.master", "yarn", "spark.executor.memory", "1g")));
        setField(resource, "workingDir", "hdfs://cluster/spark");
        setField(resource, "broker", "broker");
        setField(resource, "brokerProperties", new HashMap<>(Map.of("username", "user")));
        return resource;
    }

    @Test
    public void testSparkEtlAndLoadingFields() throws Exception {
        SparkLoadJob source = spark(103, JobState.ETL);
        common(source);
        bulk(source);
        setField(source, "sparkResource", resource());
        setField(source, "etlStartTimestamp", 4000L);
        setField(source, "appId", "application_123_456");
        setField(source, "etlOutputPath", "hdfs://cluster/output/103");
        Map<String, Pair<String, Long>> files = new HashMap<>(Map.of(
                "20.30.40.0.50", Pair.create("hdfs://cluster/part-0.parquet", 123L),
                "20.30.40.1.50", Pair.create("hdfs://cluster/part-1.parquet", 456L)));
        setField(source, "tabletMetaToFileInfo", files);
        SparkLoadAppHandle handle = new SparkLoadAppHandle();
        setField(handle, "appId", "application_123_456");
        setField(handle, "state", SparkLoadAppHandle.State.RUNNING);
        setField(handle, "queue", "load_queue");
        setField(handle, "startTime", 4999L);
        setField(handle, "finalStatus", FinalApplicationStatus.UNDEFINED);
        setField(handle, "trackingUrl", "https://yarn/application_123_456");
        setField(handle, "user", "spark_user");
        setField(handle, "logPath", "/tmp/spark-103.log");
        setField(source, "sparkLoadAppHandle", handle);
        SparkLoadJob result = (SparkLoadJob) LoadJob.fromProto(LoadJobPB.parseFrom(
                assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(gsonCopy(source), result);
        assertEquals(files, getField(result, "tabletMetaToFileInfo"));
        assertEquals("application_123_456", getField(result, "appId"));
        assertEquals(4000L, (long) getField(result, "etlStartTimestamp"));
        assertEquals("hdfs://cluster/output/103", getField(result, "etlOutputPath"));
        SparkLoadAppHandle restoredHandle = getField(result, "sparkLoadAppHandle");
        assertEquals(SparkLoadAppHandle.State.RUNNING, restoredHandle.getState());
        assertEquals("load_queue", restoredHandle.getQueue());
        assertEquals(FinalApplicationStatus.UNDEFINED, restoredHandle.getFinalStatus());
        assertEquals(4999L, restoredHandle.getStartTime());
        assertNull(restoredHandle.getProcess());
        assertNull(getField(restoredHandle, "listeners"));
        assertTrue(((Map<?, ?>) getField(result, "tabletToSentReplicaPushTask")).isEmpty());
    }

    @Test
    public void testSparkHandleStatesAndFinalStatuses() throws Exception {
        SparkLoadAppHandle source = new SparkLoadAppHandle();
        for (SparkLoadAppHandle.State state : SparkLoadAppHandle.State.values()) {
            for (FinalApplicationStatus status : FinalApplicationStatus.values()) {
                setField(source, "state", state);
                setField(source, "finalStatus", status);
                SparkLoadAppHandle result = SparkLoadAppHandle.fromProto(
                        SparkLoadAppHandlePB.parseFrom(assertInstances(source.toProto()).toByteArray()));
                assertEquals(state, result.getState());
                assertEquals(status, result.getFinalStatus());
                assertEquals(state.isFinal(), result.getState().isFinal());
            }
        }
    }

    @Test
    public void testSparkHandleDoesNotRestoreProcessOrNotifyListeners() {
        Process process = Mockito.mock(Process.class);
        SparkLoadAppHandle.Listener listener = Mockito.mock(SparkLoadAppHandle.Listener.class);
        SparkLoadAppHandle source = new SparkLoadAppHandle(process);
        source.addListener(listener);
        setField(source, "state", SparkLoadAppHandle.State.SUBMITTED);
        SparkLoadAppHandle result = SparkLoadAppHandle.fromProto(source.toProto());
        assertNull(result.getProcess());
        assertNull(getField(result, "listeners"));
        assertEquals(SparkLoadAppHandle.State.SUBMITTED, result.getState());
        Mockito.verifyNoInteractions(process, listener);
    }

    @Test
    public void testAllJobStatesAndSubtypes() {
        for (JobState state : JobState.values()) {
            for (LoadJob source : List.of(broker(1, state), spark(2, state), insert(3, state))) {
                LoadJob result = LoadJob.fromProto(source.toProto());
                assertEquals(state, result.getState());
                assertEquals(source.getClass(), result.getClass());
                assertEquals(source.getJobType(), result.getJobType());
                assertEquals(source.isCompleted(), result.isCompleted());
                assertEquals(source.isTxnDone(), result.isTxnDone());
                GsonEquivalence.assertEquivalent(gsonCopy(source), result);
            }
        }
    }

    @Test
    public void testAbsentCoreFieldsKeepDeserializationDefaults() {
        for (LoadJob source : List.of(new BrokerLoadJob(), new SparkLoadJob(), new InsertLoadJob())) {
            LoadJob result = LoadJob.fromProto(source.toProto().toBuilder()
                    .setCore(LoadJobCorePB.getDefaultInstance()).build());
            GsonEquivalence.assertEquivalent(source, result);
            assertEquals(JobState.PENDING, result.getState());
            assertEquals(-1L, (long) getField(result, "finishTimestamp"));
            assertEquals(-1L, (long) getField(result, "loadCommittedTimestamp"));
            assertEquals("row", getField(result, "partialUpdateMode"));
            assertNotNull(getField(result, "jsonOptions"));
            assertNotNull(getField(result, "loadingStatus"));
            assertNotNull(getField(result, "computeResource"));
            assertNotNull(getField(result, "lock"));
        }
    }

    @Test
    public void testExplicitZerosEmptyStringsAndFalseFlags() {
        BrokerLoadJob source = broker(1, JobState.PENDING);
        common(source);
        LoadJobPB pb = source.toProto();
        LoadJob result = LoadJob.fromProto(pb.toBuilder().setCore(pb.getCore().toBuilder().setTimeoutSecond(0)
                .setStrictMode(false).setPartialUpdate(false).setFinishTimestamp(0).setLoadCommittedTimestamp(0)
                .setTimezone("").setPartialUpdateMode("").setUser("").setJsonOptions(JSONOptionsPB.newBuilder()
                        .setStripOuterArray(false).setJsonPaths("").setJsonRoot("").setEnvelope(""))).build());
        assertEquals(0L, (long) getField(result, "timeoutSecond"));
        assertEquals(0L, (long) getField(result, "finishTimestamp"));
        assertEquals(0L, (long) getField(result, "loadCommittedTimestamp"));
        assertFalse((boolean) getField(result, "strictMode"));
        assertFalse((boolean) getField(result, "partialUpdate"));
        assertEquals("", getField(result, "timezone"));
        assertEquals("", getField(result, "partialUpdateMode"));
        assertEquals("", getField(result, "user"));
        LoadJob.JSONOptions json = getField(result, "jsonOptions");
        assertFalse(json.stripOuterArray);
        assertEquals("", json.jsonRoot);
        assertEquals("", json.jsonPaths);
        assertEquals("", json.envelope);
    }

    @Test
    public void testAbsentOptionalObjectsAndEmptyCollections() {
        for (LoadJob source : List.of(broker(1, JobState.PENDING), spark(2, JobState.PENDING))) {
            LoadJob result = LoadJob.fromProto(source.toProto());
            assertNull(getField(result, "failMsg"));
            assertNull(getField(result, "mergeCondition"));
            assertNull(getField(result, "originStmt"));
            assertNull(getField(result, "brokerPersistInfo"));
            assertTrue(((Map<?, ?>) getField(result, "sessionVariables")).isEmpty());
            assertTrue(result.getTableIds().isEmpty());
            LoadJob.JSONOptions json = getField(result, "jsonOptions");
            assertNull(json.jsonPaths);
            assertNull(json.jsonRoot);
            assertNull(json.envelope);
            assertTrue(((Map<?, ?>) getField(result, "idToTasks")).isEmpty());
            assertTrue(((List<?>) getField(result, "loadIds")).isEmpty());
        }
        SparkLoadJob result = (SparkLoadJob) LoadJob.fromProto(spark(3, JobState.PENDING).toProto());
        assertNull(getField(result, "sparkResource"));
        assertTrue(((Map<?, ?>) getField(result, "tabletMetaToFileInfo")).isEmpty());
        assertEquals(SparkLoadAppHandle.State.UNKNOWN, ((SparkLoadAppHandle) getField(result, "sparkLoadAppHandle")).getState());
    }

    @Test
    public void testDecodedCollectionsRemainMutable() {
        SparkLoadJob source = spark(1, JobState.ETL);
        bulk(source);
        setField(source, "tabletMetaToFileInfo", Map.of("key", Pair.create("path", 100L)));
        SparkLoadJob result = (SparkLoadJob) LoadJob.fromProto(source.toProto());
        Map<String, Pair<String, Long>> files = getField(result, "tabletMetaToFileInfo");
        files.put("new-key", Pair.create("new-path", 200L));
        Map<String, String> variables = getField(result, "sessionVariables");
        variables.put("new-variable", "new-value");
        assertEquals(2, files.size());
        assertEquals("new-value", variables.get("new-variable"));
    }

    @Test
    public void testUnknownFieldsDoNotAffectKnownState() {
        LoadJobPB pb = spark(1, JobState.COMMITTED).toProto();
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder().addField(1000,
                UnknownFieldSet.Field.newBuilder().addVarint(123).build()).build();
        LoadJob result = LoadJob.fromProto(pb.toBuilder().setUnknownFields(unknown)
                .setCore(pb.getCore().toBuilder().setUnknownFields(unknown)).build());
        GsonEquivalence.assertEquivalent(LoadJob.fromProto(pb), result);
        assertEquals(JobState.COMMITTED, result.getState());
        assertFalse(result.isCompleted());
    }

    @Test
    public void testMalformedKindsCoreAndBulkCannotBeLoaded() {
        LoadJobPB valid = broker(1, JobState.PENDING).toProto();
        List<LoadJobPB> invalid = List.of(valid.toBuilder().clearKind().build(),
                valid.toBuilder().clearCore().build(),
                valid.toBuilder().clearBroker().build(),
                valid.toBuilder().setInsert(InsertLoadJobPB.getDefaultInstance()).build(),
                valid.toBuilder().setBroker(BrokerLoadJobPB.getDefaultInstance()).build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder().setStateValue(999)).build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder().setJobType(LoadJobCorePB.JobType.SPARK)).build());
        for (LoadJobPB bad : invalid) {
            assertThrows(IllegalArgumentException.class, () -> LoadJob.fromProto(bad));
        }
    }

    @Test
    public void testMalformedSparkFilesResourceAndHandle() {
        LoadJobPB valid = spark(1, JobState.ETL).toProto();
        SparkLoadJobPB spark = valid.getSpark();
        SparkLoadFilesPB files = SparkLoadFilesPB.newBuilder().putValues("key",
                SparkLoadFilesPB.FileInfo.newBuilder().setPath("path").build()).build();
        assertThrows(IllegalArgumentException.class,
                () -> LoadJob.fromProto(valid.toBuilder().setSpark(spark.toBuilder().setTabletMetaToFileInfo(files)).build()));
        assertThrows(IllegalArgumentException.class,
                () -> LoadJob.fromProto(valid.toBuilder().setSpark(spark.toBuilder().setSparkResource(
                        ResourcePB.newBuilder().setKind(ResourcePB.Kind.HIVE_RESOURCE))).build()));
        assertThrows(IllegalArgumentException.class,
                () -> SparkLoadAppHandle.fromProto(SparkLoadAppHandlePB.newBuilder().setStateValue(999).build()));
        assertThrows(IllegalArgumentException.class,
                () -> SparkLoadAppHandle.fromProto(SparkLoadAppHandlePB.newBuilder().setFinalStatusValue(999).build()));
    }
}
