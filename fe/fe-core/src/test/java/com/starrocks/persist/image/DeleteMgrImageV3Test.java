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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.load.DeleteJob;
import com.starrocks.load.DeleteMgr;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.DeleteMgrPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.Int64ListPB;
import com.starrocks.persist.proto.MultiDeleteInfoPB;
import com.starrocks.persist.proto.StringListPB;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;
    private LocalMetastore metastore;
    private OlapTable table;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        Mockito.when(global.getVariableMgr()).thenReturn(new VariableMgr());
        metastore = Mockito.mock(LocalMetastore.class);
        Mockito.when(global.getLocalMetastore()).thenReturn(metastore);
        Mockito.when(metastore.getDb(10)).thenReturn(new Database(10, "db"));
        table = Mockito.mock(OlapTable.class);
        Mockito.when(metastore.getTable(10L, 20L)).thenReturn(table);
    }

    static MultiDeleteInfo info(long db, String name, long created) {
        MultiDeleteInfo info = new MultiDeleteInfo(db, 20, name, new ArrayList<>(List.of("k = 1", "v IS NULL")));
        setField(info, "createTimeMs", created);
        info.setPartitions(false, new ArrayList<>(List.of(3L, 1L, 3L)), new ArrayList<>(List.of("p3", "p1", "p3")));
        return info;
    }

    static Map<Long, List<MultiDeleteInfo>> history(DeleteMgr manager) {
        return getField(manager, "dbToDeleteInfos");
    }

    private static DeleteMgr manager(Map<Long, List<MultiDeleteInfo>> entries) {
        DeleteMgr manager = new DeleteMgr();
        entries.forEach((db, infos) -> history(manager).put(db, new ArrayList<>(infos)));
        return manager;
    }

    private DeleteMgr v2(DeleteMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        DeleteMgr restored = new DeleteMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restored.load(reader);
        reader.close();
        return restored;
    }

    private DeleteMgr v3(DeleteMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        DeleteMgr restored = new DeleteMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(150), sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(source.getDeleteInfoCount(), sections.get(0).getNumEntries());
        return restored;
    }

    @Test
    public void testHistoryFieldsAndOrderedPartitionLists() throws Exception {
        MultiDeleteInfo source = info(Long.MAX_VALUE, "table", 123456789);
        MultiDeleteInfo restored = MultiDeleteInfo.fromProto(MultiDeleteInfoPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(source, restored);
        assertEquals(Long.MAX_VALUE, restored.getDbId());
        assertEquals(123456789, restored.getCreateTimeMs());
        assertEquals(List.of(3L, 1L, 3L), restored.getPartitionIds());
        assertEquals(List.of("p3", "p1", "p3"), restored.getPartitionNames());
        assertEquals(List.of("k = 1", "v IS NULL"), restored.getDeleteConditions());
        assertFalse(restored.isNoPartitionSpecified());
    }

    @Test
    public void testAbsentFieldsMatchGsonDefaultsWithoutANewTimestamp() {
        MultiDeleteInfo baseline = GsonUtils.GSON.fromJson("{}", MultiDeleteInfo.class);
        MultiDeleteInfo restored = MultiDeleteInfo.fromProto(MultiDeleteInfoPB.getDefaultInstance());
        GsonEquivalence.assertEquivalent(baseline, restored);
        assertEquals(0, restored.getCreateTimeMs());
        assertNull(restored.getTableName());
        assertNull(restored.getPartitionIds());
        assertNull(restored.getPartitionNames());
        assertNull(restored.getDeleteConditions());
        MultiDeleteInfo empty = MultiDeleteInfo.fromProto(MultiDeleteInfoPB.newBuilder().setTableName("")
                .setDeleteConditions(StringListPB.getDefaultInstance()).setPartitionIds(Int64ListPB.getDefaultInstance())
                .setPartitionNames(StringListPB.getDefaultInstance()).setNoPartitionSpecified(true).build());
        assertEquals("", empty.getTableName());
        assertTrue(empty.getPartitionIds().isEmpty());
        assertTrue(empty.getPartitionNames().isEmpty());
        assertTrue(empty.getDeleteConditions().isEmpty());
        assertTrue(empty.isNoPartitionSpecified());
    }

    @Test
    public void testRestoredListsAreIndependentAndMutable() {
        MultiDeleteInfo source = info(10, "table", 1000);
        MultiDeleteInfo restored = MultiDeleteInfo.fromProto(source.toProto());
        restored.getPartitionIds().add(8L);
        restored.getPartitionNames().add("p8");
        restored.getDeleteConditions().add("x > 2");
        assertEquals(3, source.getPartitionIds().size());
        assertEquals(3, source.getPartitionNames().size());
        assertEquals(2, source.getDeleteConditions().size());
        restored.setPartitions(true, new ArrayList<>(), new ArrayList<>());
        assertTrue(restored.isNoPartitionSpecified());
        assertTrue(restored.getPartitionNames().isEmpty());
    }

    @Test
    public void testUnknownFieldsAndIndependentLegacyPartitionLists() throws Exception {
        MultiDeleteInfoPB pb = info(10, "table", 1000).toProto().toBuilder()
                .setPartitionNames(StringListPB.newBuilder().addValues("legacy-name"))
                .setUnknownFields(UnknownFieldSet.newBuilder().addField(99,
                        UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        MultiDeleteInfo restored = MultiDeleteInfo.fromProto(MultiDeleteInfoPB.parseFrom(pb.toByteArray()));
        // Loading a snapshot does not invoke the mutation API's partition-list validation.
        assertEquals(List.of(3L, 1L, 3L), restored.getPartitionIds());
        assertEquals(List.of("legacy-name"), restored.getPartitionNames());
    }

    @Test
    public void testDatabaseHistoriesAndOrderMatchV2() throws Exception {
        DeleteMgr source = manager(Map.of(10L, List.of(info(10, "newer", 2000), info(10, "older", 1000)),
                12L, List.of(info(12, "other-db", 3000))));
        DeleteMgr baseline = v2(source);
        DeleteMgr restored = v3(source);
        GsonEquivalence.assertEquivalent(baseline, restored);
        assertEquals(List.of("newer", "older"), history(restored).get(10L).stream()
                .map(MultiDeleteInfo::getTableName).toList());
        assertEquals(12, history(restored).get(12L).get(0).getDbId());
        assertEquals(3, restored.getDeleteInfoCount());
        assertEquals(Map.of("DeleteInfo", 3L, "DeleteJob", 0L), restored.estimateCount());

        Mockito.verifyNoInteractions(table, global.getEditLog());
    }

    @Test
    public void testShowDeleteRetainsPartitionAndConditionDisplay() throws Exception {
        MultiDeleteInfo allPartitions = info(10, "all", 2000);
        allPartitions.setPartitions(true, List.of(), List.of());
        MultiDeleteInfo noNames = info(10, "legacy", 1000);
        setField(noNames, "partitionNames", null);
        DeleteMgr source = manager(Map.of(10L, List.of(allPartitions, noNames, info(10, "explicit", 3000))));
        DeleteMgr baseline = v2(source);
        DeleteMgr restored = v3(source);
        List<List<Comparable>> rows = restored.getDeleteInfosByDb(10);
        assertEquals(baseline.getDeleteInfosByDb(10), rows);
        assertEquals(List.of("legacy", "all", "explicit"), rows.stream().map(row -> row.get(0)).toList());
        assertEquals("", rows.get(0).get(1));
        assertEquals("*", rows.get(1).get(1));
        assertEquals("p3, p1, p3", rows.get(2).get(1));
        assertEquals("k = 1, v IS NULL", rows.get(2).get(3));
        assertEquals("FINISHED", rows.get(2).get(4));
        assertTrue(restored.getDeleteInfosByDb(999).isEmpty());

    }

    @Test
    public void testImageLoadKeepsExpiredHistoryAndDoesNotReplayTableMutation() throws Exception {
        int keep = Config.label_keep_max_second;
        Config.label_keep_max_second = 1;
        try {
            DeleteMgr source = manager(Map.of(10L, List.of(info(10, "expired", 1))));
            DeleteMgr restored = v3(source);
            assertEquals(1, restored.getDeleteInfoCount());
            Mockito.verifyNoInteractions(table);
            restored.replayMultiDelete(info(10, "expired-journal", 1), global);
            assertEquals(1, restored.getDeleteInfoCount());
            Mockito.verify(table).setHasDelete();
            restored.replayMultiDelete(info(10, "new-journal", System.currentTimeMillis()), global);
            assertEquals(2, restored.getDeleteInfoCount());
            Mockito.verify(table, Mockito.times(2)).setHasDelete();
        } finally {
            Config.label_keep_max_second = keep;
        }
        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testExistingCleanerAppliesAgeAndCountAfterLoad() throws Exception {
        int keepSeconds = Config.label_keep_max_second;
        int keepCount = Config.label_keep_max_num;
        Config.label_keep_max_second = 10;
        Config.label_keep_max_num = 2;
        try {
            long future = System.currentTimeMillis() + 60_000;
            DeleteMgr source = manager(Map.of(10L, List.of(info(10, "last", future + 3000),
                    info(10, "expired", 0), info(10, "first", future), info(10, "middle", future + 1000)),
                    11L, List.of(info(11, "expired-db", 0))));
            DeleteMgr baseline = v2(source);
            baseline.removeOldDeleteInfo();
            DeleteMgr restored = v3(source);
            assertEquals(5, restored.getDeleteInfoCount());
            restored.removeOldDeleteInfo();
            GsonEquivalence.assertEquivalent(baseline, restored);
            assertEquals(List.of("middle", "last"), history(restored).get(10L).stream()
                    .map(MultiDeleteInfo::getTableName).toList());
            assertFalse(history(restored).containsKey(11L));
            history(restored).get(10L).add(info(10, "appended", future + 4000));
            assertEquals(3, restored.getDeleteInfoCount());

            MultiDeleteInfo boundary = info(10, "boundary", future - 10_999);
            assertFalse((boolean) invoke(source, "isDeleteInfoExpired", boundary, future));
            setField(boundary, "createTimeMs", future - 11_000);
            assertTrue((boolean) invoke(source, "isDeleteInfoExpired", boundary, future));
        } finally {
            Config.label_keep_max_second = keepSeconds;
            Config.label_keep_max_num = keepCount;
        }
    }

    @Test
    public void testRuntimeDeleteJobsAndKillRequestsAreNotPersisted() throws Exception {
        DeleteMgr source = manager(Map.of(10L, List.of(info(10, "history", 1))));
        Map<Long, DeleteJob> jobs = getField(source, "idToDeleteJob");
        jobs.put(99L, Mockito.mock(DeleteJob.class));
        source.killJob(99);
        DeleteMgr restored = v3(source);
        assertEquals(1, restored.getDeleteInfoCount());
        assertEquals(0, restored.getDeleteJobCount());
        assertFalse(restored.removeKillJob(99));
        assertTrue(source.removeKillJob(99));
    }

    @Test
    public void testEmptyHistoriesAreOmittedAndMissingDatabaseIdIsRejected() throws Exception {
        assertTrue(history(v3(new DeleteMgr())).isEmpty());
        assertTrue(history(v3(manager(Map.of(10L, List.of())))).isEmpty());
        DeleteMgr source = manager(Map.of(10L, List.of(), 11L, List.of(info(11, "table", 1000))));
        DeleteMgr restored = v3(source);
        assertFalse(history(restored).containsKey(10L));
        assertEquals(1, restored.getDeleteInfoCount());
        assertEquals("table", history(restored).get(11L).get(0).getTableName());

        Path invalid = LoadImageTestSupport.entries(tempDir, SectionId.DELETE_MGR, DeleteMgrPB.getDefaultInstance(),
                List.of(MultiDeleteInfoPB.getDefaultInstance()));
        ImageFormatException failure = assertThrows(ImageFormatException.class,
                () -> LoadImageTestSupport.load(invalid, new DeleteMgr().getImageV3Loaders()));
        assertTrue(failure.getMessage().contains("DELETE_MGR"));
    }

    @Test
    public void testInterleavedDatabaseEntriesAppendInFileOrder() throws Exception {
        List<MultiDeleteInfoPB> entries = List.of(info(10, "first", 2000).toProto(),
                info(11, "other", 3000).toProto(), info(10, "second", 1000).toProto());
        DeleteMgr expected = manager(Map.of(10L, List.of(info(10, "first", 2000), info(10, "second", 1000)),
                11L, List.of(info(11, "other", 3000))));
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            try (SectionWriter section = writer.beginSection(SectionId.DELETE_MGR)) {
                section.writeManager(DeleteMgrPB.getDefaultInstance());
                for (MultiDeleteInfoPB entry : entries) {
                    section.writeEntry(entry);
                }
            }
        });
        DeleteMgr restored = new DeleteMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(List.of("first", "second"), history(restored).get(10L).stream()
                .map(MultiDeleteInfo::getTableName).toList());

    }

    @Test
    public void testLargeDatabaseHistoryUsesOneMessagePerDeleteInfo() throws Exception {
        List<MultiDeleteInfo> infos = new ArrayList<>();
        String condition = "k = '" + "x".repeat(1024) + "'";
        for (int i = 0; i < 1024; i++) {
            MultiDeleteInfo info = info(10, "table-" + i, i);
            info.getDeleteConditions().add(condition);
            infos.add(info);
        }
        DeleteMgr source = manager(Map.of(10L, infos));
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        List<MultiDeleteInfoPB> entries = new ArrayList<>();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path,
                List.of(Pair.create(SectionId.DELETE_MGR, reader -> {
                    reader.readManager(DeleteMgrPB.parser());
                    reader.readEntries(MultiDeleteInfoPB.parser(), entries::add);
                })));
        assertEquals(infos.size(), sections.get(0).getNumEntries());
        assertEquals(infos.size(), entries.size());
        for (int i = 0; i < entries.size(); i++) {
            MultiDeleteInfoPB entry = entries.get(i);
            assertEquals(10, entry.getDbId());
            assertEquals(infos.get(i).toProto(), entry);
            assertTrue(entry.getSerializedSize() < 2048);
        }
        assertTrue(entries.stream().mapToLong(MultiDeleteInfoPB::getSerializedSize).sum() > 1024 * 1024);
        DeleteMgr restored = new DeleteMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        GsonEquivalence.assertEquivalent(source, restored);

    }
}
