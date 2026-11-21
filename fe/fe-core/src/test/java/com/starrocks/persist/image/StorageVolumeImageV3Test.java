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
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.SharedDataStorageVolumeMgrPB;
import com.starrocks.persist.proto.StorageVolumeMgrPB;
import com.starrocks.persist.proto.StorageVolumePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.SharedDataStorageVolumeMgr;
import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class StorageVolumeImageV3Test {
    @TempDir
    Path tempDir;
    private StarOSAgent starOS;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr global = LoadImageTestSupport.global();
        starOS = mock(StarOSAgent.class);
        when(global.getStarOSAgent()).thenReturn(starOS);
    }

    static StorageVolume volume(String id) throws Exception {
        StorageVolume volume = new StorageVolume(id, "volume-" + id, "s3", List.of("s3://bucket/path"),
                new HashMap<>(Map.of(AWS_S3_REGION, "region", AWS_S3_ENDPOINT, "endpoint",
                        AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true")), false, "saved volume");
        volume.setVTabletId(567);
        volume.setVTabletGroupId(678);
        return volume;
    }

    private StorageVolumeMgr emptyLike(StorageVolumeMgr manager) {
        return manager instanceof SharedNothingStorageVolumeMgr
                ? new SharedNothingStorageVolumeMgr() : new SharedDataStorageVolumeMgr();
    }

    private StorageVolumeMgr v2(StorageVolumeMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        StorageVolumeMgr result = emptyLike(source);
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private StorageVolumeMgr v3(StorageVolumeMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        StorageVolumeMgr result = emptyLike(source);
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(List.of(270), sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(0, sections.get(0).getNumEntries());
        assertTrue(sections.get(0).getManagerLength() > 0);
        return result;
    }

    private StorageVolumeMgrPB readManager(Path path) throws Exception {
        List<StorageVolumeMgrPB> managers = new ArrayList<>();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, List.of(
                Pair.create(SectionId.STORAGE_VOLUME_MGR,
                        reader -> managers.add(reader.readManager(StorageVolumeMgrPB.parser())))));
        assertEquals(List.of(270), sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(0, sections.get(0).getNumEntries());
        assertTrue(sections.get(0).getManagerLength() > 0);
        return managers.get(0);
    }

    private static Map<String, Set<Long>> dbBindings(StorageVolumeMgr manager) {
        return getField(manager, "storageVolumeToDbs");
    }

    private static Map<String, Set<Long>> tableBindings(StorageVolumeMgr manager) {
        return getField(manager, "storageVolumeToTables");
    }

    @Test
    public void testVolumeFieldsAndCloudConfigurationMatchV2() throws Exception {
        StorageVolume source = volume("id1");
        StorageVolume expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), StorageVolume.class);
        StorageVolume restored = StorageVolume.fromProto(StorageVolumePB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(expected.getCloudConfiguration().toFileStoreInfo(), restored.getCloudConfiguration().toFileStoreInfo());
        assertEquals(567, restored.getVTabletId());
        assertEquals(678, restored.getVTabletGroupId());
        assertFalse(restored.getEnabled());
        TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
    }

    @Test
    public void testLocationsAndPropertiesRemainIndependentAndMutable() throws Exception {
        StorageVolume source = volume("id1");
        StorageVolume restored = StorageVolume.fromProto(source.toProto());
        restored.getLocations().add("s3://other/path");
        restored.getProperties().put("new", "value");
        assertEquals(List.of("s3://bucket/path"), source.getLocations());
        assertFalse(source.getProperties().containsKey("new"));
    }

    @Test
    public void testSharedDataBindingsAndReverseIndexesMatchV2() throws Exception {
        SharedDataStorageVolumeMgr source = new SharedDataStorageVolumeMgr();
        setField(source, "defaultStorageVolumeId", "sv1");
        dbBindings(source).put("sv1", new HashSet<>(Set.of(11L, 12L)));
        dbBindings(source).put("sv2", new HashSet<>(Set.of(13L)));
        tableBindings(source).put("sv1", new HashSet<>(Set.of(21L, 22L)));
        tableBindings(source).put("sv2", new HashSet<>(Set.of(23L)));
        StorageVolumeMgr expected = v2(source);
        StorageVolumeMgr restored = v3(source);
        GsonEquivalence.assertEquivalent(expected, restored, Set.of(), Set.of("\\$.svToDbs.*", "\\$.svToTables.*"));
        assertEquals("sv1", restored.getDefaultStorageVolumeId());
        for (long id : List.of(11L, 12L, 13L)) {
            assertEquals(expected.getStorageVolumeIdOfDb(id), restored.getStorageVolumeIdOfDb(id));
        }
        for (long id : List.of(21L, 22L, 23L)) {
            assertEquals(expected.getStorageVolumeIdOfTable(id), restored.getStorageVolumeIdOfTable(id));
        }
        restored.unbindDbToStorageVolume(11);
        restored.unbindTableToStorageVolume(21);
        assertNull(restored.getStorageVolumeIdOfDb(11));
        assertNull(restored.getStorageVolumeIdOfTable(21));
        assertEquals(Set.of(12L), dbBindings(restored).get("sv1"));
        assertEquals(Set.of(22L), tableBindings(restored).get("sv1"));

        verifyNoInteractions(starOS);
    }

    @Test
    public void testLegacyNullVolumeBindingsAreRemovedAsInV2() throws Exception {
        SharedDataStorageVolumeMgr source = new SharedDataStorageVolumeMgr();
        dbBindings(source).put("null", new HashSet<>(Set.of(1L)));
        tableBindings(source).put("null", new HashSet<>(Set.of(2L)));
        dbBindings(source).put("sv", new HashSet<>(Set.of(3L)));
        StorageVolumeMgr expected = v2(source);
        StorageVolumeMgr restored = v3(source);
        assertFalse(dbBindings(restored).containsKey("null"));
        assertFalse(tableBindings(restored).containsKey("null"));
        assertNull(restored.getStorageVolumeIdOfDb(1));
        assertNull(restored.getStorageVolumeIdOfTable(2));
        assertEquals("sv", restored.getStorageVolumeIdOfDb(3));
        assertEquals(dbBindings(expected), dbBindings(restored));

    }

    @Test
    public void testEmptyBindingSetsAreRetained() throws Exception {
        SharedDataStorageVolumeMgr source = new SharedDataStorageVolumeMgr();
        dbBindings(source).put("empty-db", new HashSet<>());
        tableBindings(source).put("empty-table", new HashSet<>());
        StorageVolumeMgr restored = v3(source);
        assertEquals(Map.of("empty-db", Set.of()), dbBindings(restored));
        assertEquals(Map.of("empty-table", Set.of()), tableBindings(restored));

    }

    @Test
    public void testSharedNothingVolumesAndDefaultMatchV2() throws Exception {
        SharedNothingStorageVolumeMgr source = new SharedNothingStorageVolumeMgr();
        source.replayCreateStorageVolume(volume("sv1"));
        source.replayCreateStorageVolume(volume("sv2"));
        setField(source, "defaultStorageVolumeId", "sv2");
        StorageVolumeMgr expected = v2(source);
        StorageVolumeMgr restored = v3(source);
        assertEquals(Set.copyOf(expected.listStorageVolumeNames()), Set.copyOf(restored.listStorageVolumeNames()));
        assertEquals("sv2", restored.getDefaultStorageVolumeId());
        for (String id : List.of("sv1", "sv2")) {
            StorageVolume actual = restored.getStorageVolume(id);
            GsonEquivalence.assertEquivalent(expected.getStorageVolume(id), actual);
            assertEquals(expected.getStorageVolume(id).getCloudConfiguration().toFileStoreInfo(),
                    actual.getCloudConfiguration().toFileStoreInfo());
        }

        verifyNoInteractions(starOS);
    }

    @Test
    public void testModeMismatchIsRejectedBeforeInstallingBindings() throws Exception {
        for (StorageVolumeMgr source : List.of(new SharedDataStorageVolumeMgr(), new SharedNothingStorageVolumeMgr())) {
            Path path = LoadImageTestSupport.save(tempDir, source::save);
            StorageVolumeMgr target = source instanceof SharedNothingStorageVolumeMgr
                    ? new SharedDataStorageVolumeMgr() : new SharedNothingStorageVolumeMgr();
            assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(path, target.getImageV3Loaders()));
            assertTrue(dbBindings(target).isEmpty());
            assertTrue(tableBindings(target).isEmpty());

        }
    }

    @Test
    public void testUnknownManagerKindCannotBeIgnoredAsAnEntry() throws Exception {
        boolean old = Config.metadata_ignore_unknown_subtype;
        try {
            for (boolean ignore : List.of(false, true)) {
                Config.metadata_ignore_unknown_subtype = ignore;
                Path path = LoadImageTestSupport.save(tempDir, writer -> {
                    try (SectionWriter section = writer.beginSection(SectionId.STORAGE_VOLUME_MGR)) {
                        section.writeManager(StorageVolumeMgrPB.newBuilder().setKindValue(999).build());
                    }
                });
                StorageVolumeMgr target = new SharedDataStorageVolumeMgr();
                assertThrows(SubtypeNotFoundException.class,
                        () -> LoadImageTestSupport.load(path, target.getImageV3Loaders()));
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }
    }

    @Test
    public void testAllBindingsAreStoredInManagerMessage() throws Exception {
        SharedDataStorageVolumeMgr source = new SharedDataStorageVolumeMgr();
        Set<Long> dbs = LongStream.rangeClosed(1, 1024).boxed().collect(Collectors.toSet());
        Set<Long> tables = LongStream.rangeClosed(10001, 11024).boxed().collect(Collectors.toSet());
        dbBindings(source).put("sv", dbs);
        tableBindings(source).put("sv", tables);
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        StorageVolumeMgrPB pb = readManager(path);
        assertEquals(dbs, Set.copyOf(pb.getSharedData().getStorageVolumeToDbsOrThrow("sv").getValuesList()));
        assertEquals(tables, Set.copyOf(pb.getSharedData().getStorageVolumeToTablesOrThrow("sv").getValuesList()));
        StorageVolumeMgr restored = new SharedDataStorageVolumeMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        for (long id : dbs) {
            assertEquals("sv", restored.getStorageVolumeIdOfDb(id));
        }
        for (long id : tables) {
            assertEquals("sv", restored.getStorageVolumeIdOfTable(id));
        }

    }

    @Test
    public void testEmptyManagersAndReloadClearOldBindings() throws Exception {
        for (StorageVolumeMgr source : List.of(new SharedDataStorageVolumeMgr(), new SharedNothingStorageVolumeMgr())) {
            Path path = LoadImageTestSupport.save(tempDir, source::save);
            StorageVolumeMgrPB pb = readManager(path);
            if (source instanceof SharedNothingStorageVolumeMgr) {
                assertEquals(StorageVolumeMgrPB.Kind.SHARED_NOTHING, pb.getKind());
                assertEquals(StorageVolumeMgrPB.InstanceCase.SHARED_NOTHING, pb.getInstanceCase());
                assertTrue(pb.getSharedNothing().hasSharedData());
            } else {
                assertEquals(StorageVolumeMgrPB.Kind.SHARED_DATA, pb.getKind());
                assertEquals(StorageVolumeMgrPB.InstanceCase.SHARED_DATA, pb.getInstanceCase());
            }
            StorageVolumeMgr target = emptyLike(source);
            dbBindings(target).put("stale", new HashSet<>(Set.of(1L)));
            tableBindings(target).put("stale", new HashSet<>(Set.of(2L)));
            setField(target, "defaultStorageVolumeId", "stale");
            if (target instanceof SharedNothingStorageVolumeMgr) {
                target.replayCreateStorageVolume(volume("stale"));
            }
            LoadImageTestSupport.load(path, target.getImageV3Loaders());
            assertTrue(dbBindings(target).isEmpty());
            assertTrue(tableBindings(target).isEmpty());
            assertEquals("", target.getDefaultStorageVolumeId());
            if (target instanceof SharedNothingStorageVolumeMgr) {
                assertTrue(target.listStorageVolumeNames().isEmpty());
            }

        }
    }

    @Test
    public void testHdfsVolumeAndMissingVirtualTabletIdsMatchV2() throws Exception {
        StorageVolume source = new StorageVolume("hdfs", "hdfs-volume", "hdfs", List.of("hdfs://host/path"),
                new HashMap<>(Map.of("hadoop.username", "hdfs-user")), true, "");
        StorageVolume expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), StorageVolume.class);
        StorageVolume restored = StorageVolume.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(expected.getCloudConfiguration().toFileStoreInfo(), restored.getCloudConfiguration().toFileStoreInfo());
        assertEquals(-1L, restored.getVTabletId());
        assertEquals(-1L, restored.getVTabletGroupId());
        StorageVolumePB absentIds = source.toProto().toBuilder().clearVirtualTabletId().clearVirtualTabletGroupId().build();
        com.google.gson.JsonObject legacyJson = GsonUtils.GSON.toJsonTree(source).getAsJsonObject();
        legacyJson.remove("vt");
        legacyJson.remove("vtg");
        StorageVolume legacy = GsonUtils.GSON.fromJson(legacyJson, StorageVolume.class);
        StorageVolume restoredLegacy = StorageVolume.fromProto(absentIds);
        assertEquals(legacy.getVTabletId(), restoredLegacy.getVTabletId());
        assertEquals(legacy.getVTabletGroupId(), restoredLegacy.getVTabletGroupId());
    }

    @Test
    public void testRestoreDoesNotApplyNewVolumeCreationConstraints() throws Exception {
        StorageVolume source = volume("sv");
        // This combination may exist in old metadata even though creation now rejects it.
        source.getProperties().put("aws.s3.enable_partitioned_prefix", "true");
        StorageVolume expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), StorageVolume.class);
        StorageVolume restored = StorageVolume.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(List.of("s3://bucket/path"), restored.getLocations());
        assertEquals(expected.getCloudConfiguration().toFileStoreInfo(), restored.getCloudConfiguration().toFileStoreInfo());
    }

    @Test
    public void testZeroObjectIdIsARealBindingAndNotAnEmptySet() throws Exception {
        SharedDataStorageVolumeMgr source = new SharedDataStorageVolumeMgr();
        dbBindings(source).put("zero", new HashSet<>(Set.of(0L)));
        tableBindings(source).put("zero", new HashSet<>(Set.of(0L)));
        dbBindings(source).put("empty", new HashSet<>());
        StorageVolumeMgr restored = v3(source);
        assertEquals(Set.of(0L), dbBindings(restored).get("zero"));
        assertEquals(Set.of(0L), tableBindings(restored).get("zero"));
        assertEquals("zero", restored.getStorageVolumeIdOfDb(0));
        assertEquals("zero", restored.getStorageVolumeIdOfTable(0));
        assertTrue(dbBindings(restored).get("empty").isEmpty());

    }

    @Test
    public void testAllSharedNothingVolumesAreStoredInManagerMessage() throws Exception {
        SharedNothingStorageVolumeMgr source = new SharedNothingStorageVolumeMgr();
        for (int i = 0; i < 1024; i++) {
            source.replayCreateStorageVolume(volume("sv-" + i));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        SharedNothingStorageVolumeMgr restored = new SharedNothingStorageVolumeMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(270), sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(0, sections.get(0).getNumEntries());
        assertEquals(1024, readManager(path).getSharedNothing().getIdToStorageVolumeCount());
        assertEquals(1024, restored.listStorageVolumeNames().size());
        assertNotNull(restored.getStorageVolume("sv-1023").getCloudConfiguration());

    }

    @Test
    public void testManagerKindAndInstanceMatchRunMode() throws Exception {
        for (StorageVolumeMgr source : List.of(new SharedDataStorageVolumeMgr(), new SharedNothingStorageVolumeMgr())) {
            setField(source, "defaultStorageVolumeId", "sv");
            dbBindings(source).put("sv", new HashSet<>(Set.of(11L)));
            tableBindings(source).put("sv", new HashSet<>(Set.of(21L)));
            if (source instanceof SharedNothingStorageVolumeMgr) {
                source.replayCreateStorageVolume(volume("sv"));
            }
            StorageVolumeMgrPB pb = readManager(LoadImageTestSupport.save(tempDir, source::save));
            SharedDataStorageVolumeMgrPB sharedData;
            if (source instanceof SharedNothingStorageVolumeMgr) {
                assertEquals(StorageVolumeMgrPB.Kind.SHARED_NOTHING, pb.getKind());
                assertEquals(StorageVolumeMgrPB.InstanceCase.SHARED_NOTHING, pb.getInstanceCase());
                assertTrue(pb.getSharedNothing().hasSharedData());
                sharedData = pb.getSharedNothing().getSharedData();
                assertEquals("sv", pb.getSharedNothing().getIdToStorageVolumeOrThrow("sv").getId());
            } else {
                assertEquals(StorageVolumeMgrPB.Kind.SHARED_DATA, pb.getKind());
                assertEquals(StorageVolumeMgrPB.InstanceCase.SHARED_DATA, pb.getInstanceCase());
                sharedData = pb.getSharedData();
            }
            assertEquals("sv", sharedData.getDefaultStorageVolumeId());
            assertEquals(List.of(11L), sharedData.getStorageVolumeToDbsOrThrow("sv").getValuesList());
            assertEquals(List.of(21L), sharedData.getStorageVolumeToTablesOrThrow("sv").getValuesList());
            StorageVolumeMgr expected = v2(source);
            StorageVolumeMgr restored = v3(source);
            GsonEquivalence.assertEquivalent(expected, restored);
            assertEquals(expected.getStorageVolumeIdOfDb(11), restored.getStorageVolumeIdOfDb(11));
            assertEquals(expected.getStorageVolumeIdOfTable(21), restored.getStorageVolumeIdOfTable(21));

        }
        verifyNoInteractions(starOS);
    }

    @Test
    public void testSharedNothingVolumeMapKeysArePreserved() throws Exception {
        SharedNothingStorageVolumeMgr source = new SharedNothingStorageVolumeMgr();
        Map<String, StorageVolume> volumes = getField(source, "idToSV");
        volumes.put("original-key", volume("sv"));
        StorageVolumeMgr expected = v2(source);
        StorageVolumeMgr restored = v3(source);
        assertNull(restored.getStorageVolume("sv"));
        GsonEquivalence.assertEquivalent(expected.getStorageVolume("original-key"),
                restored.getStorageVolume("original-key"));

    }
}
