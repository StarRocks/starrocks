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

import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.ReplicationJobCorePB;
import com.starrocks.persist.proto.ReplicationJobPB;
import com.starrocks.persist.proto.ReplicationPartitionMapPB;
import com.starrocks.replication.LakeReplicationJob;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.replication.ReplicationJobState;
import com.starrocks.replication.ReplicationMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class ReplicationMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    static ReplicationJob job(long tableId, boolean lake, int state) {
        JsonObject json = GsonUtils.GSON.fromJson("""
                {
                  "clazz": "ReplicationJob",
                  "jobId": "replication-job",
                  "createdTimeMs": 101,
                  "finishedTimeMs": 202,
                  "srcToken": "source-token",
                  "databaseId": 303,
                  "tableId": 404,
                  "tableType": "OLAP",
                  "srcTableType": "OLAP",
                  "replicationDataSize": 4294967297,
                  "replicationReplicaCount": 7,
                  "partitionInfos": {
                    "501": {
                      "partitionId": 502, "version": 503, "dataVersion": 504,
                      "srcVersion": 505, "srcVersionEpoch": 506, "srcPartitionId": 507,
                      "indexInfos": {
                        "601": {
                          "indexId": 602, "schemaHash": 603, "srcSchemaHash": 604,
                          "tabletInfos": {
                            "701": {
                              "tabletId": 702, "srcTabletId": 703,
                              "replicaInfos": {
                                "801": {
                                  "backendId": 802,
                                  "srcBackendInfos": [
                                    {"host": "host-a", "bePort": 901, "httpPort": 902},
                                    {"host": "host-b", "bePort": 903, "httpPort": 904}
                                  ],
                                  "srcSnapshotInfo": {
                                    "backendInfo": {"host": "snapshot-host", "bePort": 905, "httpPort": 906},
                                    "snapshotPath": "/snapshot/path", "incrementalSnapshot": true
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  },
                  "transactionId": 1001,
                  "state": {"id": 3}
                }
                """, JsonObject.class);
        json.addProperty("tableId", tableId);
        json.getAsJsonObject("state").addProperty("id", state);
        if (lake) {
            json.addProperty("clazz", "LakeReplicationJob");
            json.addProperty("tableType", "LAKE");
            json.addProperty("srcTableType", "LAKE");
            json.addProperty("virtualTabletId", 1101);
            json.addProperty("srcDatabaseId", 1102);
            json.addProperty("srcTableId", 1103);
        }
        ReplicationJob job = GsonUtils.GSON.fromJson(json, ReplicationJob.class);
        if (lake) {
            FilePathInfo.Builder path = FilePathInfo.newBuilder().setFullPath("s3://bucket/service/db/table");
            path.getFsInfoBuilder().getS3FsInfoBuilder().setBucket("bucket")
                    .setPartitionedPrefixEnabled(true).setNumPartitionedPrefix(1024);
            setField(job, "srcTableFilePathInfo", path.build());
        }
        return job;
    }

    static void put(ReplicationMgr manager, String mapName, ReplicationJob job) {
        Map<Long, ReplicationJob> jobs = getField(manager, mapName);
        jobs.put(job.getTableId(), job);
    }

    private ReplicationMgr restore(ReplicationMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        ReplicationMgr restored = new ReplicationMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(290), sections.stream().map(IndexEntryPB::getType).toList());
        assertTrue(sections.stream().allMatch(section -> section.getManagerLength() == 0));
        return restored;
    }

    @Test
    public void testBothJobKindsAndAllStatesPreserveEveryPersistedField() throws Exception {
        for (boolean lake : List.of(false, true)) {
            for (int state = 1; state <= 5; state++) {
                ReplicationJob source = job(404, lake, state);
                FinalManagersImageTestSupport.assertAllFieldsPresent(source);
                ReplicationJobPB pb = ReplicationJobPB.parseFrom(source.toProto().toByteArray());
                assertEquals(lake ? ReplicationJobPB.Kind.LAKE_REPLICATION : ReplicationJobPB.Kind.REPLICATION, pb.getKind());
                assertEquals(lake ? ReplicationJobPB.InstanceCase.LAKE_REPLICATION : ReplicationJobPB.InstanceCase.REPLICATION,
                        pb.getInstanceCase());
                ReplicationJob restored = ReplicationJob.fromProto(pb);
                GsonEquivalence.assertEquivalent(FinalManagersImageTestSupport.gson(source, ReplicationJob.class), restored);
                assertEquals(source.getClass(), restored.getClass());
                assertEquals(101, restored.getCreatedTimeMs());
                assertEquals(202, restored.getFinishedTimeMs());
                assertEquals(1001, restored.getTransactionId());
                assertEquals(4294967297L, restored.getReplicationDataSize());
                assertEquals(source.getReplicationReplicaCount(), restored.getReplicationReplicaCount());
                assertEquals(state, restored.getState().getId());
            }
        }
        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testNestedMapKeysAndSourceBackendOrderAreIndependentOfObjectIds() {
        ReplicationJob source = job(404, false, 3);
        ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
        ReplicationJobCorePB core = restored.toProto().getReplication();
        var partitions = core.getPartitionInfos().getValuesMap();
        assertEquals(List.of(501L), List.copyOf(partitions.keySet()));
        assertEquals(502, partitions.get(501L).getPartitionId());
        var indexes = partitions.get(501L).getIndexInfos().getValuesMap();
        assertEquals(602, indexes.get(601L).getIndexId());
        var tablets = indexes.get(601L).getTabletInfos().getValuesMap();
        assertEquals(702, tablets.get(701L).getTabletId());
        var replicas = tablets.get(701L).getReplicaInfos().getValuesMap();
        assertEquals(802, replicas.get(801L).getBackendId());
        assertEquals(List.of("host-a", "host-b"), replicas.get(801L).getSrcBackendInfos().getValuesList().stream()
                .map(backend -> backend.getHost()).toList());
        assertEquals("snapshot-host", replicas.get(801L).getSrcSnapshotInfo().getBackendInfo().getHost());
        assertTrue(replicas.get(801L).getSrcSnapshotInfo().getIncrementalSnapshot());
        GsonEquivalence.assertEquivalent(source, restored);
    }

    @Test
    public void testSingleSectionRestoresEveryStateToTheSameCollectionAsV2() throws Exception {
        ReplicationMgr source = new ReplicationMgr();
        for (boolean lake : List.of(false, true)) {
            for (int state = 1; state <= 5; state++) {
                source.replayReplicationJob(job((lake ? 100 : 0) + state, lake, state));
            }
        }
        source.replayReplicationJob(job(200, false, 12345));
        ReplicationMgr expected = new ReplicationMgr();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        ReplicationMgr restored = restore(source);
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(List.of(1L, 2L, 3L, 101L, 102L, 103L, 200L),
                restored.getRunningJobs().stream().map(ReplicationJob::getTableId).sorted().toList());
        assertEquals(List.of(4L, 104L),
                restored.getCommittedJobs().stream().map(ReplicationJob::getTableId).sorted().toList());
        assertEquals(List.of(5L, 105L),
                restored.getAbortedJobs().stream().map(ReplicationJob::getTableId).sorted().toList());

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testSameTableRetainsCurrentAndHistoricalJobsInAnyEntryOrder() throws Exception {
        ReplicationJob running = job(1, false, 3);
        ReplicationJob committed = job(1, true, 4);
        ReplicationJob aborted = job(1, false, 5);
        setField(running, "jobId", "current");
        setField(committed, "jobId", "previous-committed");
        setField(aborted, "jobId", "previous-aborted");
        ReplicationMgr source = new ReplicationMgr();
        put(source, "runningJobs", running);
        put(source, "committedJobs", committed);
        put(source, "abortedJobs", aborted);
        ReplicationMgr expected = new ReplicationMgr();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        List<List<ReplicationJob>> orders = List.of(
                List.of(running, committed, aborted), List.of(running, aborted, committed),
                List.of(committed, running, aborted), List.of(committed, aborted, running),
                List.of(aborted, running, committed), List.of(aborted, committed, running));
        GsonEquivalence.assertEquivalent(expected, restore(source));
        for (List<ReplicationJob> order : orders) {
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.REPLICATION_MGR,
                    order.stream().map(ReplicationJob::toProto).toList());
            ReplicationMgr restored = new ReplicationMgr();
            List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(1, sections.size());
            assertEquals(3, sections.get(0).getNumEntries());
            GsonEquivalence.assertEquivalent(expected, restored);
            assertEquals("current", restored.getRunningJobs().iterator().next().getJobId());
            assertEquals("previous-committed", restored.getCommittedJobs().iterator().next().getJobId());
            assertEquals("previous-aborted", restored.getAbortedJobs().iterator().next().getJobId());
            assertInstanceOf(LakeReplicationJob.class, restored.getCommittedJobs().iterator().next());
        }

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testLeaderTaskBookkeepingIsEmptySoRecoveryCanRetry() {
        for (boolean lake : List.of(false, true)) {
            ReplicationJob source = job(1, lake, 3);
            Map<AgentTask, AgentTask> running = getField(source, "runningTasks");
            Map<AgentTask, AgentTask> finished = getField(source, "finishedTasks");
            AgentTask task = mock(AgentTask.class);
            running.put(task, task);
            finished.put(task, task);
            setField(source, "taskNum", 2);
            ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
            assertEquals(0, restored.getRunningTaskNum());
            assertTrue(((Map<?, ?>) getField(restored, "finishedTasks")).isEmpty());
            assertEquals(0, (int) getField(restored, "taskNum"));
            assertTrue((boolean) invoke(restored, "isCrashRecovery"));
            assertFalse((boolean) invoke(source, "isCrashRecovery"));
            assertEquals(1001, restored.getTransactionId());
            assertEquals(ReplicationJobState.REPLICATING, restored.getState());
        }
    }

    @Test
    public void testLakeFilePathAndCopyForPersistKeepPartitionedPrefixMetadata() {
        LakeReplicationJob source = (LakeReplicationJob) job(1, true, 3);
        LakeReplicationJob restored = (LakeReplicationJob) ReplicationJob.fromProto(source.toProto());
        FilePathInfo expected = getField(source, "srcTableFilePathInfo");
        FilePathInfo actual = getField(restored, "srcTableFilePathInfo");
        assertEquals(expected, actual);
        assertTrue(actual.getFsInfo().getS3FsInfo().getPartitionedPrefixEnabled());
        assertEquals(1024, actual.getFsInfo().getS3FsInfo().getNumPartitionedPrefix());
        assertEquals(1101L, (long) getField(restored, "virtualTabletId"));
        assertEquals(1102L, (long) getField(restored, "srcDatabaseId"));
        assertEquals(1103L, (long) getField(restored, "srcTableId"));
        ReplicationJob copy = restored.copyForPersist();
        GsonEquivalence.assertEquivalent(restored, copy);
        assertEquals(actual, getField(copy, "srcTableFilePathInfo"));
        assertEquals(source.toProto(), copy.toProto());
    }

    @Test
    public void testLakeMissingAndMalformedFilePathBytes() {
        ReplicationJobPB good = job(1, true, 3).toProto();
        ReplicationJobPB absent = good.toBuilder().setLakeReplication(good.getLakeReplication().toBuilder()
                .clearSrcTableFilePathInfo()).build();
        ReplicationJob restored = ReplicationJob.fromProto(absent);
        assertNull(getField(restored, "srcTableFilePathInfo"));
        assertNull(getField(restored, "srcTableFilePathInfoBytes"));
        ReplicationJobPB corrupt = good.toBuilder().setLakeReplication(good.getLakeReplication().toBuilder()
                .setSrcTableFilePathInfo(ByteString.copyFrom(new byte[] {(byte) 0xff}))).build();
        assertThrows(IllegalArgumentException.class, () -> ReplicationJob.fromProto(corrupt));
    }

    @Test
    public void testNullAndEmptyCollectionsAndOptionalFields() {
        ReplicationJobPB absent = ReplicationJobPB.newBuilder().setKind(ReplicationJobPB.Kind.REPLICATION)
                .setReplication(ReplicationJobCorePB.getDefaultInstance()).build();
        ReplicationJob restored = ReplicationJob.fromProto(absent);
        assertNull(restored.getJobId());
        assertNull(restored.getState());
        assertNull(getField(restored, "partitionInfos"));
        assertNull(getField(restored, "srcToken"));
        assertNull(getField(restored, "tableType"));
        assertEquals(0, restored.getCreatedTimeMs());
        ReplicationJobPB empty = absent.toBuilder().setReplication(ReplicationJobCorePB.newBuilder()
                .setJobId("").setSrcToken("").setPartitionInfos(ReplicationPartitionMapPB.getDefaultInstance())).build();
        restored = ReplicationJob.fromProto(empty);
        assertEquals("", restored.getJobId());
        assertEquals("", getField(restored, "srcToken"));
        Map<?, ?> partitions = getField(restored, "partitionInfos");
        assertTrue(partitions.isEmpty());
        assertTrue(restored.toProto().getReplication().hasPartitionInfos());
    }

    @Test
    public void testExtensibleNumericStateAndTableTypesArePreserved() {
        ReplicationJob source = job(1, false, 12345);
        ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
        assertEquals(12345, restored.getState().getId());
        assertEquals("UNKNOWN", restored.getState().name());
        for (Table.TableType type : List.of(Table.TableType.OLAP, Table.TableType.CLOUD_NATIVE,
                Table.TableType.MATERIALIZED_VIEW, Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW)) {
            setField(source, "tableType", type);
            setField(source, "srcTableType", type);
            restored = ReplicationJob.fromProto(source.toProto());
            assertEquals(type, getField(restored, "tableType"));
            assertEquals(type, getField(restored, "srcTableType"));
            GsonEquivalence.assertEquivalent(source, restored);
        }
    }

    @Test
    public void testUnknownKindBeforeConversionAndReaderContinuation() throws Exception {
        ReplicationJobPB unknown = ReplicationJobPB.newBuilder().setKindValue(999)
                .setReplication(ReplicationJobCorePB.newBuilder().setTableTypeValue(999)).build();
        assertThrows(SubtypeNotFoundException.class, () -> ReplicationJob.fromProto(unknown));
        assertThrows(IllegalArgumentException.class, () -> ReplicationJob.fromProto(ReplicationJobPB.getDefaultInstance()));
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.REPLICATION_MGR,
                    List.of(job(1, false, 3).toProto(), unknown, job(2, true, 3).toProto()));
            Config.metadata_ignore_unknown_subtype = false;
            ReplicationMgr strict = new ReplicationMgr();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, strict.getImageV3Loaders()));
            assertEquals(1, strict.getRunningJobs().size());
            Config.metadata_ignore_unknown_subtype = true;
            ReplicationMgr tolerant = new ReplicationMgr();
            LoadImageTestSupport.load(path, tolerant.getImageV3Loaders());
            assertEquals(List.of(1L, 2L),
                    tolerant.getRunningJobs().stream().map(ReplicationJob::getTableId).sorted().toList());

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testNestedCollectionsCanBeChangedWithoutMutatingSource() {
        ReplicationJob source = job(1, false, 3);
        ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
        Map<Long, Object> partitions = getField(restored, "partitionInfos");
        Object partition = partitions.get(501L);
        Map<Long, Object> indexes = getField(partition, "indexInfos");
        Map<Long, Object> tablets = getField(indexes.get(601L), "tabletInfos");
        Map<Long, Object> replicas = getField(tablets.get(701L), "replicaInfos");
        List<Object> backends = getField(replicas.get(801L), "srcBackendInfos");
        backends.remove(0);
        replicas.put(900L, replicas.get(801L));
        tablets.put(800L, tablets.get(701L));
        indexes.put(700L, indexes.get(601L));
        partitions.put(600L, partition);
        assertEquals(2, partitions.size());
        assertEquals(1, ((Map<?, ?>) getField(source, "partitionInfos")).size());
        assertEquals(2, source.toProto().getReplication().getPartitionInfos().getValuesOrThrow(501)
                .getIndexInfos().getValuesOrThrow(601).getTabletInfos().getValuesOrThrow(701)
                .getReplicaInfos().getValuesOrThrow(801).getSrcBackendInfos().getValuesCount());
    }

    @Test
    public void testLargeCollectionsRemainJobEntriesAndEmptyImageMergesLikeV2() throws Exception {
        ReplicationMgr source = new ReplicationMgr();
        for (int i = 1; i <= 1024; i++) {
            source.replayReplicationJob(job(i, i % 2 == 0, (i - 1) % 5 + 1));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        ReplicationMgr restored = new ReplicationMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(1024L), sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertEquals(615, restored.getRunningJobs().size());
        assertEquals(205, restored.getCommittedJobs().size());
        assertEquals(204, restored.getAbortedJobs().size());
        GsonEquivalence.assertEquivalent(source, restored);
        Path empty = LoadImageTestSupport.save(tempDir, new ReplicationMgr()::save);
        LoadImageTestSupport.load(empty, restored.getImageV3Loaders());
        assertEquals(615, restored.getRunningJobs().size());
        assertEquals(205, restored.getCommittedJobs().size());
        assertEquals(204, restored.getAbortedJobs().size());

    }

    @Test
    public void testReplayedCompletionAndDeletionWorkAfterImageLoad() throws Exception {
        ReplicationMgr source = new ReplicationMgr();
        put(source, "runningJobs", job(1, false, 3));
        ReplicationMgr restored = restore(source);
        ReplicationJob running = restored.getRunningJobs().iterator().next();
        ReplicationJob completed = running.copyForPersist();
        setField(completed, "state", ReplicationJobState.COMMITTED);
        setField(completed, "finishedTimeMs", 123456L);
        restored.replayReplicationJob(completed);
        assertTrue(restored.getRunningJobs().isEmpty());
        assertEquals(1, restored.getCommittedJobs().size());
        assertEquals(123456L, restored.getCommittedJobs().iterator().next().getFinishedTimeMs());
        restored.replayDeleteReplicationJob(completed);
        assertTrue(restored.getCommittedJobs().isEmpty());

        verifyNoInteractions(global.getEditLog());
    }
}
