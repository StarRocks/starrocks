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
import com.starrocks.persist.proto.ReplicationJobPB;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.replication.ReplicationMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicationNestedImageV3Test {
    private static final List<String> MAP_FIELDS = List.of("partitionInfos", "indexInfos", "tabletInfos", "replicaInfos");
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
    }

    private Map<Long, Object> map(ReplicationJob job, int level) {
        Object object = job;
        for (int i = 0; i < level; i++) {
            Map<Long, Object> children = getField(object, MAP_FIELDS.get(i));
            object = children.values().iterator().next();
        }
        return getField(object, MAP_FIELDS.get(level));
    }

    private Object owner(ReplicationJob job, int level) {
        return level == 0 ? job : map(job, level - 1).values().iterator().next();
    }

    private Object replica(ReplicationJob job) {
        return map(job, 3).values().iterator().next();
    }

    @Test
    public void testEveryNestedMapPreservesNullVersusEmpty() {
        for (int level = 0; level < MAP_FIELDS.size(); level++) {
            ReplicationJob source = ReplicationMgrImageV3Test.job(1, false, 3);
            setField(owner(source, level), MAP_FIELDS.get(level), null);
            ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
            assertNull(map(restored, level));
            GsonEquivalence.assertEquivalent(FinalManagersImageTestSupport.gson(source, ReplicationJob.class), restored);
            setField(owner(source, level), MAP_FIELDS.get(level), new HashMap<>());
            restored = ReplicationJob.fromProto(source.toProto());
            assertTrue(map(restored, level).isEmpty());
            GsonEquivalence.assertEquivalent(FinalManagersImageTestSupport.gson(source, ReplicationJob.class), restored);
            Map<Long, Object> mutable = map(restored, level);
            Object example = map(ReplicationMgrImageV3Test.job(2, false, 3), level).values().iterator().next();
            mutable.put(999L, example);
            assertEquals(1, mutable.size());
            assertTrue(map(source, level).isEmpty());
        }
    }

    @Test
    public void testNullableBackendListSnapshotAndSnapshotBackend() {
        for (String field : List.of("srcBackendInfos", "srcSnapshotInfo")) {
            ReplicationJob source = ReplicationMgrImageV3Test.job(1, false, 3);
            setField(replica(source), field, null);
            ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
            assertNull(getField(replica(restored), field));
            GsonEquivalence.assertEquivalent(source, restored);
        }
        ReplicationJob source = ReplicationMgrImageV3Test.job(1, false, 3);
        setField(replica(source), "srcBackendInfos", new ArrayList<>());
        Object snapshot = getField(replica(source), "srcSnapshotInfo");
        setField(snapshot, "backendInfo", null);
        setField(snapshot, "snapshotPath", null);
        ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
        List<?> backends = getField(replica(restored), "srcBackendInfos");
        assertTrue(backends.isEmpty());
        Object restoredSnapshot = getField(replica(restored), "srcSnapshotInfo");
        assertNull(getField(restoredSnapshot, "backendInfo"));
        assertNull(getField(restoredSnapshot, "snapshotPath"));
        assertTrue((boolean) getField(restoredSnapshot, "incrementalSnapshot"));
        GsonEquivalence.assertEquivalent(source, restored);
    }

    @Test
    public void testBackendNullAndEmptyHostAndZeroPorts() {
        for (String host : new String[] {null, "", "ipv6::host"}) {
            ReplicationJob source = ReplicationMgrImageV3Test.job(1, false, 3);
            List<Object> backends = getField(replica(source), "srcBackendInfos");
            setField(backends.get(0), "host", host);
            setField(backends.get(0), "bePort", 0);
            setField(backends.get(0), "httpPort", 0);
            ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
            List<Object> restoredBackends = getField(replica(restored), "srcBackendInfos");
            assertEquals(host, getField(restoredBackends.get(0), "host"));
            assertEquals(0, (int) getField(restoredBackends.get(0), "bePort"));
            assertEquals(0, (int) getField(restoredBackends.get(0), "httpPort"));
            assertEquals("host-b", getField(restoredBackends.get(1), "host"));
            GsonEquivalence.assertEquivalent(source, restored);
        }
    }

    @Test
    public void testSnapshotPathAndIncrementalFlagAreIndependent() {
        for (String path : new String[] {null, "", "/snapshot/with spaces/中文"}) {
            for (boolean incremental : List.of(false, true)) {
                ReplicationJob source = ReplicationMgrImageV3Test.job(1, false, 3);
                Object snapshot = getField(replica(source), "srcSnapshotInfo");
                setField(snapshot, "snapshotPath", path);
                setField(snapshot, "incrementalSnapshot", incremental);
                ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
                Object restoredSnapshot = getField(replica(restored), "srcSnapshotInfo");
                assertEquals(path, getField(restoredSnapshot, "snapshotPath"));
                assertEquals(incremental, (boolean) getField(restoredSnapshot, "incrementalSnapshot"));
                GsonEquivalence.assertEquivalent(source, restored);
            }
        }
    }

    @Test
    public void testLargeNestedMapsRemainInsideTheirOwningJobAndKeepEveryKey() throws Exception {
        for (int level = 0; level < MAP_FIELDS.size(); level++) {
            ReplicationJob job = ReplicationMgrImageV3Test.job(1, false, 3);
            Map<Long, Object> values = map(job, level);
            Object child = values.values().iterator().next();
            values.clear();
            for (int i = 1; i <= 1024; i++) {
                values.put((long) i, child);
            }
            ReplicationMgr source = new ReplicationMgr();
            ReplicationMgrImageV3Test.put(source, "runningJobs", job);
            ReplicationMgr expected = new ReplicationMgr();
            FinalManagersImageTestSupport.v2(source::save, expected::load);
            Path path = LoadImageTestSupport.save(tempDir, source::save);
            ReplicationMgr restored = new ReplicationMgr();
            var sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(1, sections.get(0).getNumEntries());
            ReplicationJob actual = restored.getRunningJobs().iterator().next();
            assertEquals(1024, map(actual, level).size());
            assertEquals(values.keySet(), map(actual, level).keySet());
            GsonEquivalence.assertEquivalent(expected, restored);

        }
    }

    @Test
    public void testManySourceBackendsPreserveDuplicatesAndOrder() throws Exception {
        ReplicationJob source = ReplicationMgrImageV3Test.job(1, false, 3);
        List<Object> backends = getField(replica(source), "srcBackendInfos");
        Object first = backends.get(0);
        Object second = backends.get(1);
        backends.clear();
        for (int i = 0; i < 1024; i++) {
            backends.add(i % 2 == 0 ? first : second);
        }
        ReplicationJob restored = ReplicationJob.fromProto(ReplicationJobPB.parseFrom(source.toProto().toByteArray()));
        List<Object> actual = getField(replica(restored), "srcBackendInfos");
        assertEquals(1024, actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertEquals(i % 2 == 0 ? "host-a" : "host-b", getField(actual.get(i), "host"));
        }
        actual.remove(0);
        assertEquals(1024, backends.size());
        assertEquals(1023, actual.size());
    }

    @Test
    public void testBoundaryVersionsTimestampsAndTransactionIdsAreNotRecomputed() {
        ReplicationJob source = ReplicationMgrImageV3Test.job(1, false, 3);
        setField(source, "createdTimeMs", 0L);
        setField(source, "finishedTimeMs", 0L);
        setField(source, "transactionId", Long.MAX_VALUE);
        Object partition = map(source, 0).values().iterator().next();
        setField(partition, "version", -1L);
        setField(partition, "dataVersion", 0L);
        setField(partition, "srcVersion", Long.MAX_VALUE);
        setField(partition, "srcVersionEpoch", Long.MIN_VALUE);
        ReplicationJob restored = ReplicationJob.fromProto(source.toProto());
        assertEquals(0L, restored.getCreatedTimeMs());
        assertEquals(0L, restored.getFinishedTimeMs());
        assertEquals(Long.MAX_VALUE, restored.getTransactionId());
        assertFalse(restored.isExpired());
        GsonEquivalence.assertEquivalent(source, restored);
    }

    @Test
    public void testInvalidKnownTableTypeIsNotIgnoredAsUnknownSubtype() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = true;
            ReplicationJobPB good = ReplicationMgrImageV3Test.job(1, false, 3).toProto();
            ReplicationJobPB bad = good.toBuilder().setReplication(good.getReplication().toBuilder()
                    .setTableTypeValue(999)).build();
            Path path = FinalManagersImageTestSupport.entries(tempDir,
                    SectionId.REPLICATION_MGR, List.of(bad, good));
            ReplicationMgr restored = new ReplicationMgr();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, restored.getImageV3Loaders()));
            assertTrue(restored.getRunningJobs().isEmpty());
        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }
}
