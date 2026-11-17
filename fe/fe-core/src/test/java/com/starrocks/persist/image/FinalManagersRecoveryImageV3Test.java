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

import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.encryption.KeyMgr;
import com.starrocks.encryption.NormalKey;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeId;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.meta.SqlDigestBlackList;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.ReplicationJobPB;
import com.starrocks.replication.ReplicationMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.HistoricalNodeMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class FinalManagersRecoveryImageV3Test {
    private static final List<Integer> IDS = List.of(290, 300, 310, 330, 340, 350, 360, 370);
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        when(global.getWarehouseMgr()).thenReturn(mock(WarehouseManager.class));
        new MockUp<HistoricalNodeMgr>() {
            @Mock
            public boolean isResourceAvailable(String key) {
                return true;
            }
        };
    }

    private final class Managers {
        final ReplicationMgr replication = new ReplicationMgr();
        final KeyMgr keys = new KeyMgr();
        final PipeManager pipes = new PipeManager();
        final SqlBlackList patterns = new SqlBlackList();
        final ClusterSnapshotMgr snapshots = new ClusterSnapshotMgr();
        final HistoricalNodeMgr nodes = new HistoricalNodeMgr();
        final TabletReshardJobMgr reshard = new TabletReshardJobMgr();
        final SqlDigestBlackList digests = new SqlDigestBlackList();

        void populate() {
            ReplicationMgrImageV3Test.put(replication, "runningJobs", ReplicationMgrImageV3Test.job(1, true, 3));
            ReplicationMgrImageV3Test.put(replication, "committedJobs", ReplicationMgrImageV3Test.job(2, false, 4));
            ReplicationMgrImageV3Test.put(replication, "abortedJobs", ReplicationMgrImageV3Test.job(3, false, 5));
            NormalKey master = KeyMgrImageV3Test.master(1);
            KeyMgrImageV3Test.add(keys, master);
            NormalKey child = (NormalKey) master.generateKey();
            child.setId(2);
            KeyMgrImageV3Test.add(keys, child);
            pipes.putPipe(PipeMgrImageV3Test.pipe(1, Pipe.State.RUNNING));
            patterns.put(7, Pattern.compile("forbidden"));
            snapshots.getAutomatedSnapshotJobs().put(1L,
                    ClusterSnapshotMgrImageV3Test.job(1, ClusterSnapshotJob.ClusterSnapshotJobState.FINISHED));
            nodes.updateHistoricalBackendIds(10, 20, List.of(30L, 40L), 50);
            nodes.updateHistoricalComputeNodeIds(10, 20, List.of(60L), 70);
            reshard.getTabletReshardJobs().put(1L,
                    ReshardImageTestSupport.job(1, true, TabletReshardJob.JobState.RUNNING));
            digests.put("digest");
        }

        void save(ImageWriterV3 writer) throws IOException {
            replication.save(writer);
            saveAfterReplication(writer);
        }

        void saveAfterReplication(ImageWriterV3 writer) throws IOException {
            keys.save(writer);
            pipes.getRepo().save(writer);
            patterns.save(writer);
            snapshots.save(writer);
            nodes.save(writer);
            reshard.save(writer);
            digests.save(writer);
        }

        List<Pair<SectionId, SectionLoader>> loaders() {
            when(global.getTabletReshardJobMgr()).thenReturn(reshard);
            List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>();
            loaders.addAll(replication.getImageV3Loaders());
            loaders.addAll(keys.getImageV3Loaders());
            loaders.addAll(pipes.getRepo().getImageV3Loaders());
            loaders.addAll(patterns.getImageV3Loaders());
            loaders.addAll(snapshots.getImageV3Loaders());
            loaders.addAll(nodes.getImageV3Loaders());
            loaders.addAll(reshard.getImageV3Loaders());
            loaders.addAll(digests.getImageV3Loaders());
            return loaders;
        }

        void assertPopulated() {
            assertEquals(1, replication.getRunningJobs().size());
            assertEquals(1, replication.getCommittedJobs().size());
            assertEquals(1, replication.getAbortedJobs().size());
            assertEquals(2, keys.numKeys());
            assertEquals(keys.getKeyById(1), keys.getKeyById(2).getParent());
            assertEquals("pipe-1", pipes.getPipeById(new PipeId(8, 1)).getName());
            assertEquals(1, patterns.getBlackLists().size());
            assertEquals(7, patterns.getBlackLists().get(0).id);
            assertEquals(1L, snapshots.getLastFinishedAutomatedClusterSnapshotJob().getId());
            assertEquals(List.of(30L, 40L), nodes.getHistoricalBackendIds(10, 20));
            assertEquals(List.of(60L), nodes.getHistoricalComputeNodeIds(10, 20));
            assertEquals(70, nodes.getLastUpdateTime(10, 20));
            assertEquals(1, reshard.getTabletReshardJobs().size());
            assertNotNull(reshard.getReshardingTablet(1010, 101));
            assertNull(reshard.getReshardingTablet(1010, 100));
            assertEquals(Set.of("digest"), digests.getDigests());
        }
    }

    @Test
    public void testPopulatedBatchHasEightSectionsInV2SaveOrder() throws Exception {
        Managers source = new Managers();
        source.populate();
        TabletReshardJobMgr expectedReshard = new TabletReshardJobMgr();
        when(global.getTabletReshardJobMgr()).thenReturn(expectedReshard);
        FinalManagersImageTestSupport.v2(source.reshard::save, expectedReshard::load);
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        Managers restored = new Managers();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.loaders());
        assertEquals(IDS, sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(List.of(3L, 2L, 1L, 1L, 1L, 1L, 1L, 1L),
                sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertEquals(List.of(340), sections.stream().filter(section -> section.getManagerLength() > 0)
                .map(IndexEntryPB::getType).toList());
        restored.assertPopulated();
        GsonEquivalence.assertEquivalent(source.replication, restored.replication);
        GsonEquivalence.assertEquivalent(source.snapshots, restored.snapshots);
        GsonEquivalence.assertEquivalent(expectedReshard, restored.reshard);

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testEmptyBatchWritesNoArtificialManagerOrWarehouseRecords() throws Exception {
        Managers source = new Managers();
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        Managers restored = new Managers();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.loaders());
        assertEquals(IDS, sections.stream().map(IndexEntryPB::getType).toList());
        assertTrue(sections.stream().allMatch(section -> section.getNumEntries() == 0));
        assertEquals(0, restored.keys.numKeys());
        assertTrue(restored.replication.getRunningJobs().isEmpty());
        assertTrue(restored.pipes.getAllPipes().isEmpty());
        assertTrue(restored.patterns.getBlackLists().isEmpty());
        assertTrue(restored.snapshots.getAutomatedSnapshotJobs().isEmpty());
        assertTrue(restored.nodes.getAllHistoricalNodeSet().isEmpty());
        assertTrue(restored.reshard.getTabletReshardJobs().isEmpty());
        assertTrue(restored.digests.getDigests().isEmpty());

    }

    @Test
    public void testV2LoaderOrderIsIndependentOfPhysicalSectionLayout() throws Exception {
        Managers source = new Managers();
        source.populate();
        Managers restored = new Managers();
        List<Integer> visited = new ArrayList<>();
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>();
        for (Pair<SectionId, SectionLoader> loader : restored.loaders()) {
            loaders.add(Pair.create(loader.first, reader -> {
                visited.add(reader.id().getValue());
                loader.second.load(reader);
            }));
        }
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            source.digests.save(writer);
            source.reshard.save(writer);
            source.nodes.save(writer);
            source.snapshots.save(writer);
            source.patterns.save(writer);
            source.pipes.getRepo().save(writer);
            source.keys.save(writer);
            source.replication.save(writer);
        });
        var sections = LoadImageTestSupport.load(path, loaders);
        assertEquals(List.of(370, 360, 350, 340, 330, 310, 300, 290),
                sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(IDS, visited);
        restored.assertPopulated();

    }

    @Test
    public void testUnknownEntryDoesNotDesynchronizeFollowingSections() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Managers source = new Managers();
            source.populate();
            Path path = LoadImageTestSupport.save(tempDir, writer -> {
                try (SectionWriter section = writer.beginSection(SectionId.REPLICATION_MGR)) {
                    section.writeEntry(ReplicationJobPB.newBuilder().setKindValue(999).build());
                    section.writeEntry(ReplicationMgrImageV3Test.job(1, true, 3).toProto());
                    section.writeEntry(ReplicationMgrImageV3Test.job(2, false, 4).toProto());
                    section.writeEntry(ReplicationMgrImageV3Test.job(3, false, 5).toProto());
                }
                source.saveAfterReplication(writer);
            });
            Config.metadata_ignore_unknown_subtype = false;
            Managers strict = new Managers();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, strict.loaders()));
            assertEquals(0, strict.keys.numKeys());
            assertTrue(strict.digests.getDigests().isEmpty());
            Config.metadata_ignore_unknown_subtype = true;
            Managers restored = new Managers();
            List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.loaders());
            assertEquals(4, sections.get(0).getNumEntries());
            restored.assertPopulated();

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testNewSectionIdsHaveDedicatedManagerRangesAndNoCollisions() throws Exception {
        Managers managers = new Managers();
        assertEquals(IDS, managers.loaders().stream().map(loader -> loader.first.getValue()).toList());
        Set<Integer> allIds = new HashSet<>();
        for (Field field : SectionId.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == SectionId.class) {
                SectionId id = (SectionId) field.get(null);
                assertTrue(allIds.add(id.getValue()), "duplicate section ID " + id);
            }
        }
        assertEquals(63, allIds.size());
        assertEquals(1, SectionId.GLOBAL_STATE.getValue());
        for (int reserved = 320; reserved < 330; reserved++) {
            assertTrue(!allIds.contains(reserved), "empty CE WarehouseManager must not allocate " + reserved);
        }
        assertEquals(List.of(29, 30, 31, 33, 34, 35, 36, 37),
                IDS.stream().map(id -> id / 10).toList());
    }
}
