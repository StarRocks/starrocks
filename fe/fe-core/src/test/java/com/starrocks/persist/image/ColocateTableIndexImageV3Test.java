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

import com.google.common.collect.Multimap;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.common.Range;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.ColocateGroupBucketsEntryPB;
import com.starrocks.persist.proto.ColocateGroupNameEntryPB;
import com.starrocks.persist.proto.ColocateGroupRangesPB;
import com.starrocks.persist.proto.ColocateGroupSchemaEntryPB;
import com.starrocks.persist.proto.ColocateGroupTablesEntryPB;
import com.starrocks.persist.proto.ColocateRangePB;
import com.starrocks.persist.proto.ColocateTableGroupEntryPB;
import com.starrocks.persist.proto.ColocateTableIndexPB;
import com.starrocks.persist.proto.GroupIdPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.persist.image.ColocateTableIndexProtoTest.tuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColocateTableIndexImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;
    private LocalMetastore metastore;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        metastore = Mockito.mock(LocalMetastore.class);
        Mockito.when(global.getLocalMetastore()).thenReturn(metastore);
        runMode(RunMode.SHARED_NOTHING);
    }

    private static void runMode(RunMode mode) {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return mode;
            }
        };
    }

    static void group(ColocateTableIndex index, GroupId id, String name, DistributionInfoType distribution,
                      long... tableIds) {
        Map<String, GroupId> names = getField(index, "groupName2Id");
        Multimap<GroupId, Long> groups = getField(index, "group2Tables");
        Map<Long, GroupId> reverse = getField(index, "table2Group");
        Map<GroupId, ColocateGroupSchema> schemas = getField(index, "group2Schema");
        Map<GroupId, List<List<Long>>> buckets = getField(index, "group2BackendsPerBucketSeq");
        names.put(name, id);
        for (long tableId : tableIds) {
            groups.put(id, tableId);
            reverse.put(tableId, id);
        }
        schemas.put(id, new ColocateGroupSchema(id, List.of(new Column("k", IntegerType.BIGINT)),
                2, (short) 2, distribution));
        buckets.put(id, List.of(List.of(9L, 3L), List.of(3L, 7L)));
    }

    private void present(long dbId, boolean lake, long... ids) {
        Database db = new Database(dbId, "db" + dbId);
        Mockito.when(metastore.getDbIncludeRecycleBin(dbId)).thenReturn(db);
        for (long id : ids) {
            OlapTable table = Mockito.mock(OlapTable.class);
            Mockito.when(table.getId()).thenReturn(id);
            Mockito.when(table.isCloudNativeTableOrMaterializedView()).thenReturn(lake);
            Mockito.when(metastore.getTableIncludeRecycleBin(db, id)).thenReturn(table);
        }
    }

    private ColocateTableIndex v2(ColocateTableIndex source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.saveColocateTableIndexV2(image.getImageWriter());
        ColocateTableIndex result = new ColocateTableIndex();
        {
            SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
            result.loadColocateTableIndexV2(reader);
            reader.close();
        }
        return result;
    }

    private ColocateTableIndex v3(ColocateTableIndex source) throws Exception {
        ColocateTableIndex result = new ColocateTableIndex();
        Path file = LoadImageTestSupport.save(tempDir, source::save);
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, result.getImageV3Loaders());
        assertEquals(List.of(100), sections.stream().map(IndexEntryPB::getType).toList());
        IndexEntryPB section = sections.get(0);
        assertEquals(0, section.getNumEntries());
        assertTrue(section.getManagerLength() > 0);
        assertEquals(section.getLength(), section.getManagerLength());
        return result;
    }

    private void assertIndexEquals(ColocateTableIndex expected, ColocateTableIndex actual) {
        // Complex-key maps and sets have no inter-group iteration contract. Values remain ordered.
        for (String field : List.of("groupName2Id", "group2Tables", "table2Group", "group2BackendsPerBucketSeq",
                "unstableGroups", "metaGroups")) {
            assertEquals((Object) getField(expected, field), (Object) getField(actual, field), field);
        }
        Map<GroupId, ColocateGroupSchema> schemas = getField(expected, "group2Schema");
        Map<GroupId, ColocateGroupSchema> actualSchemas = getField(actual, "group2Schema");
        assertEquals(schemas.keySet(), actualSchemas.keySet());
        schemas.forEach((id, schema) -> GsonEquivalence.assertEquivalent(schema, actualSchemas.get(id),
                CatalogEquivalenceRules.TYPE_LABELS_ADDED_ON_LOAD, Set.of()));
        assertEquals((Object) getField(expected.getColocateRangeMgr(), "colocateGroupToRanges"),
                (Object) getField(actual.getColocateRangeMgr(), "colocateGroupToRanges"));
    }

    @Test
    public void testAllIndexesAndSharedRangeGroups() throws Exception {
        ColocateTableIndex source = new ColocateTableIndex();
        GroupId hash = new GroupId(10, 500);
        GroupId rangeA = new GroupId(10, 600);
        GroupId rangeB = new GroupId(11, 600);
        group(source, hash, "10_hash", DistributionInfoType.HASH, 102, 101, 102);
        group(source, rangeA, "10_range", DistributionInfoType.RANGE, 103);
        group(source, rangeB, "11_range", DistributionInfoType.RANGE, 111);
        Set<GroupId> unstable = getField(source, "unstableGroups");
        unstable.add(hash);
        source.getColocateRangeMgr().setColocateRanges(600, List.of(
                new ColocateRange(Range.of(null, tuple(10), false, false), 901),
                new ColocateRange(Range.of(tuple(10), null, true, false), 902)));
        present(10, false, 101, 102, 103);
        present(11, false, 111);
        ColocateTableIndex baseline = v2(source);
        ColocateTableIndex result = v3(source);
        assertIndexEquals(baseline, result);
        assertEquals(List.of(102L, 101L, 102L), result.getAllTableIds(hash));
        assertEquals(List.of(List.of(9L, 3L), List.of(3L, 7L)), result.getBackendsPerBucketSeq(hash));
        assertEquals(hash, result.getGroup(101));
        assertTrue(result.isGroupUnstable(hash));
        assertEquals(List.of(rangeB), result.getColocateWithGroupsInOtherDb(rangeA));
        assertEquals(1, result.getColocateRangeMgr().getColocateRangeIndex(600, tuple(10)));
        assertEquals(Set.of(901L, 902L), result.getAllPackShardGroupIds());

    }

    @Test
    public void testCleanupKeepsRecycleBinReferencesAndOtherDatabaseRanges() throws Exception {
        ColocateTableIndex source = new ColocateTableIndex();
        GroupId retained = new GroupId(1, 100);
        GroupId missingDb = new GroupId(2, 100);
        GroupId missingTable = new GroupId(3, 300);
        group(source, retained, "1_group", DistributionInfoType.RANGE, 11);
        group(source, missingDb, "2_group", DistributionInfoType.RANGE, 21);
        group(source, missingTable, "3_group", DistributionInfoType.HASH, 31, 32);
        source.getColocateRangeMgr().initColocateGroup(100, 900);
        present(1, false, 11);
        present(3, false, 32);
        // Live lookups deliberately return null: the v2 cleanup must use IncludeRecycleBin.
        assertNull(metastore.getDb(1));
        ColocateTableIndex loaded = v3(source);
        assertIndexEquals(v2(source), loaded);
        assertEquals(retained, loaded.getGroup(11));
        assertFalse(loaded.isColocateTable(21));
        assertFalse(loaded.isColocateTable(31));
        assertEquals(List.of(32L), loaded.getAllTableIds(missingTable));
        assertNull(loaded.getGroupSchema(missingDb));
        assertEquals(Set.of(900L), loaded.getAllPackShardGroupIds());
        assertNotNull(loaded.getGroupSchema("1_group"));

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testCleanupRemovesLastRangeGroup() throws Exception {
        ColocateTableIndex source = new ColocateTableIndex();
        GroupId id = new GroupId(7, 700);
        group(source, id, "7_orphan", DistributionInfoType.RANGE, 71);
        source.getColocateRangeMgr().initColocateGroup(700, 701);
        Set<GroupId> unstable = getField(source, "unstableGroups");
        unstable.add(id);
        ColocateTableIndex loaded = v3(source);
        assertIndexEquals(v2(source), loaded);
        assertTrue(loaded.getAllGroupIds().isEmpty());
        assertTrue(loaded.getUnstableGroupIds().isEmpty());
        assertTrue(loaded.getAllPackShardGroupIds().isEmpty());
        assertNull(loaded.getGroupSchema("7_orphan"));
    }

    @Test
    public void testSharedDataRebuildsHashMetaGroupsOnly() throws Exception {
        runMode(RunMode.SHARED_DATA);
        ColocateTableIndex source = new ColocateTableIndex();
        GroupId hashLake = new GroupId(1, 10);
        GroupId rangeLake = new GroupId(1, 20);
        GroupId hashLocal = new GroupId(2, 30);
        group(source, hashLake, "1_hash", DistributionInfoType.HASH, 11);
        group(source, rangeLake, "1_range", DistributionInfoType.RANGE, 12);
        group(source, hashLocal, "2_hash", DistributionInfoType.HASH, 21);
        source.getColocateRangeMgr().initColocateGroup(20, 200);
        present(1, true, 11, 12);
        present(2, false, 21);
        ColocateTableIndex loaded = v3(source);
        assertIndexEquals(v2(source), loaded);
        Set<GroupId> meta = getField(loaded, "metaGroups");
        assertEquals(Set.of(hashLake), meta);
        assertFalse(meta.contains(rangeLake));
        assertEquals(Set.of(200L), loaded.getAllPackShardGroupIds());

        Mockito.verify(global, Mockito.never()).getStarOSAgent();
    }

    @Test
    public void testEmptyAndAbsentRangeRecords() throws Exception {
        ColocateTableIndex loaded = v3(new ColocateTableIndex());
        assertTrue(loaded.getAllGroupIds().isEmpty());
        assertNotNull(loaded.getColocateRangeMgr());
        assertTrue(loaded.getAllPackShardGroupIds().isEmpty());

        ColocateTableIndex source = new ColocateTableIndex();
        group(source, new GroupId(1, 2), "1_hash", DistributionInfoType.HASH, 3);
        present(1, false, 3);
        assertTrue(v3(source).getAllPackShardGroupIds().isEmpty());
    }

    @Test
    public void testLargeManagerRoundTrip() throws Exception {
        int numGroups = 1000;
        int numBuckets = 256;
        ColocateTableIndex source = new ColocateTableIndex();
        Database db = new Database(1, "db1");
        Mockito.when(metastore.getDbIncludeRecycleBin(1)).thenReturn(db);
        Mockito.when(metastore.getTableIncludeRecycleBin(Mockito.eq(db), Mockito.anyLong()))
                .thenReturn(Mockito.mock(OlapTable.class));
        Map<GroupId, ColocateGroupSchema> schemas = getField(source, "group2Schema");
        Map<GroupId, List<List<Long>>> buckets = getField(source, "group2BackendsPerBucketSeq");
        for (int g = 0; g < numGroups; g++) {
            GroupId id = new GroupId(1, g + 1000);
            group(source, id, "1_group" + g, DistributionInfoType.HASH, g + 10000);
            schemas.put(id, new ColocateGroupSchema(id, List.of(new Column("k", IntegerType.BIGINT)),
                    numBuckets, (short) 3, DistributionInfoType.HASH));
            List<List<Long>> groupBuckets = new ArrayList<>();
            for (int b = 0; b < numBuckets; b++) {
                long backend = 100000L + (g + b) % 100;
                groupBuckets.add(List.of(backend, backend + 100, backend + 200));
            }
            buckets.put(id, groupBuckets);
        }
        ColocateTableIndex baseline = v2(source);
        Path file = LoadImageTestSupport.save(tempDir, source::save);
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            assertEquals(1, reader.sections().size());
            IndexEntryPB section = reader.sections().get(0);
            assertEquals(100, section.getType());
            assertEquals(0, section.getNumEntries());
            assertEquals(section.getLength(), section.getManagerLength());
            reader.load(List.of(Pair.create(SectionId.COLOCATE_TABLE_INDEX, sectionReader -> {
                ColocateTableIndexPB pb = sectionReader.readManager(ColocateTableIndexPB.parser());
                assertEquals(numGroups, pb.getGroupNamesCount());
                assertEquals(numGroups, pb.getGroupTablesCount());
                assertEquals(numGroups, pb.getTableGroupsCount());
                assertEquals(numGroups, pb.getGroupSchemasCount());
                assertEquals(numGroups, pb.getGroupBucketsCount());
                assertTrue(pb.getSerializedSize() > 1 << 20);
                assertTrue(pb.getSerializedSize() < ImageV3Format.MANAGER_MESSAGE_SOFT_LIMIT_BYTES);
                System.out.printf("Colocate: %d groups, %d buckets/group, 3 replicas, PB=%d bytes, image=%d bytes%n",
                        numGroups, numBuckets, pb.getSerializedSize(), Files.size(file));
            })));
            ColocateTableIndex result = new ColocateTableIndex();
            reader.load(result.getImageV3Loaders());
            assertIndexEquals(baseline, result);
        }

    }

    @Test
    public void testInvalidManagerDoesNotInstallPartOfTheIndex() throws Exception {
        ColocateTableIndex destination = new ColocateTableIndex();
        GroupId original = new GroupId(9, 99);
        group(destination, original, "9_existing", DistributionInfoType.HASH, 90);
        GroupIdPB group = new GroupId(1, 2).toProto();
        List<ColocateTableIndexPB.Builder> invalid = List.of(
                ColocateTableIndexPB.newBuilder().addGroupNames(ColocateGroupNameEntryPB.getDefaultInstance()),
                ColocateTableIndexPB.newBuilder().addGroupTables(ColocateGroupTablesEntryPB.getDefaultInstance()),
                ColocateTableIndexPB.newBuilder().addTableGroups(ColocateTableGroupEntryPB.newBuilder().setGroupId(group)),
                ColocateTableIndexPB.newBuilder().addGroupSchemas(ColocateGroupSchemaEntryPB.newBuilder().setGroupId(group)),
                ColocateTableIndexPB.newBuilder().addGroupBuckets(ColocateGroupBucketsEntryPB.newBuilder().setGroupId(group)),
                ColocateTableIndexPB.newBuilder().addUnstableGroups(GroupIdPB.newBuilder().setDbId(1)),
                ColocateTableIndexPB.newBuilder().addGroupRanges(ColocateGroupRangesPB.getDefaultInstance()),
                ColocateTableIndexPB.newBuilder().addGroupRanges(ColocateGroupRangesPB.newBuilder().setGroupId(2)
                        .addRanges(ColocateRangePB.getDefaultInstance())));
        for (ColocateTableIndexPB.Builder bad : invalid) {
            ColocateTableIndexPB pb = bad.addGroupNames(ColocateGroupNameEntryPB.newBuilder().setName("3_staged")
                    .setGroupId(new GroupId(3, 4).toProto())).build();
            Path path = LoadImageTestSupport.save(tempDir, writer -> {
                try (SectionWriter section = writer.beginSection(SectionId.COLOCATE_TABLE_INDEX)) {
                    section.writeManager(pb);
                }
            });
            ImageFormatException exception = assertThrows(ImageFormatException.class,
                    () -> LoadImageTestSupport.load(path, destination.getImageV3Loaders()));
            assertTrue(exception.getMessage().contains("COLOCATE_TABLE_INDEX manager"));
            assertEquals(original, destination.getGroup(90));
            assertEquals(Map.of("9_existing", original), getField(destination, "groupName2Id"));
            assertTrue(destination.getUnstableGroupIds().isEmpty());

        }
    }
}
