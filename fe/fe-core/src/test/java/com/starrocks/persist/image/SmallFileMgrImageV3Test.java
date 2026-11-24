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
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.PulsarRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.persist.RemoveSmallFileLog;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.SmallFileMgrPB;
import com.starrocks.persist.proto.SmallFilePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SmallFileMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        LocalMetastore metastore = Mockito.mock(LocalMetastore.class);
        Mockito.when(global.getLocalMetastore()).thenReturn(metastore);
        Mockito.when(metastore.getDb("db")).thenReturn(new Database(10, "db"));
    }

    static SmallFile file(long id, String catalog, String name, byte[] content) {
        return new SmallFile(10L, catalog, name, id, Base64.getEncoder().encodeToString(content),
                content.length, DigestUtils.md5Hex(content), true);
    }

    private static SmallFileMgr manager(SmallFile... files) {
        SmallFileMgr manager = new SmallFileMgr();
        for (SmallFile file : files) {
            manager.replayCreateFile(file);
        }
        return manager;
    }

    private SmallFileMgr v2(SmallFileMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.saveSmallFilesV2(image.getImageWriter());
        SmallFileMgr restored = new SmallFileMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restored.loadSmallFilesV2(reader);
        reader.close();
        return restored;
    }

    private SmallFileMgr v3(SmallFileMgr source) throws Exception {
        Path file = LoadImageTestSupport.save(tempDir, source::save);
        SmallFileMgr restored = new SmallFileMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, restored.getImageV3Loaders());
        assertEquals(List.of(130), sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(source.getInfo("db").size(), sections.get(0).getNumEntries());
        return restored;
    }

    @Test
    public void testBinaryContentAndUrlHaveDifferentMeanings() throws Exception {
        SmallFile content = file(Long.MAX_VALUE, "kafka", "ca", new byte[] {0, -1, 13, 10, 32});
        SmallFile url = new SmallFile(10L, "pulsar", "ca", 2L, "https://example.invalid/file?version=2",
                100, "ab01", false);
        for (SmallFile source : List.of(content, url)) {
            SmallFile restored = SmallFile.fromProto(SmallFilePB.parseFrom(source.toProto().toByteArray()));
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(source.content, restored.content);
            assertEquals(source.isContent, restored.isContent);
            assertArrayEquals(source.getContentBytes(), restored.getContentBytes());
        }
        assertNull(SmallFile.fromProto(url.toProto()).getContentBytes());
    }

    @Test
    public void testConversionPreservesMissingAndExplicitEmptyFields() {
        SmallFile absent = SmallFile.fromProto(SmallFilePB.getDefaultInstance());
        assertNull(absent.catalog);
        assertNull(absent.name);
        assertNull(absent.content);
        assertNull(absent.md5);
        assertEquals(0, absent.id);
        assertFalse(absent.isContent);
        SmallFile empty = SmallFile.fromProto(SmallFilePB.newBuilder().setCatalog("").setName("")
                .setContent("").setMd5("").setIsContent(true).setSize(0).build());
        assertEquals("", empty.catalog);
        assertEquals("", empty.name);
        assertEquals("", empty.md5);
        assertArrayEquals(new byte[0], empty.getContentBytes());
    }

    @Test
    public void testConversionDoesNotNormalizeOrValidateStoredPayload() {
        SmallFile source = file(1, "kafka", "ca", new byte[] {1});
        source.md5 = "ABCDEF";
        source.content = "legacy-invalid-base64";
        SmallFile restored = SmallFile.fromProto(source.toProto());
        assertEquals("ABCDEF", restored.md5);
        assertEquals(source.content, restored.content);
        assertThrows(IllegalArgumentException.class, restored::getContentBytes);
    }

    @Test
    public void testUnknownProtoFieldsAreIgnored() throws Exception {
        SmallFile source = file(1, "kafka", "ca", new byte[] {7});
        SmallFilePB pb = source.toProto().toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(100).build()).build()).build();
        GsonEquivalence.assertEquivalent(source, SmallFile.fromProto(SmallFilePB.parseFrom(pb.toByteArray())));
    }

    @Test
    public void testBothIndexesAndShowRowsMatchV2() throws Exception {
        SmallFile otherDb = file(3, "kafka", "ca", new byte[] {3});
        otherDb.dbId = 11;
        SmallFileMgr source = manager(file(1, "kafka", "ca", new byte[] {1}),
                file(2, "pulsar", "ca", new byte[] {2}), otherDb);
        SmallFileMgr baseline = v2(source);
        // Use the raw helper because one file belongs to a different DB than the SHOW fixture.
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        SmallFileMgr restored = new SmallFileMgr();
        assertEquals(3, LoadImageTestSupport.load(path, restored.getImageV3Loaders()).get(0).getNumEntries());
        for (long id : List.of(1L, 2L, 3L)) {
            SmallFile actual = restored.getSmallFile(id);
            GsonEquivalence.assertEquivalent(baseline.getSmallFile(id), actual);
            assertSame(actual, restored.getSmallFile(actual.dbId, actual.catalog, actual.name, true));
        }
        assertEquals(baseline.getInfo("db"), restored.getInfo("db"));
        assertTrue(restored.containsFile(11, "kafka", "ca"));

        Mockito.verifyNoInteractions(global.getEditLog());
        Mockito.verify(global, Mockito.never()).getNextId();
    }

    @Test
    public void testUrlContentRequirementIsEnforcedAfterLoad() throws Exception {
        SmallFile source = new SmallFile(10L, "kafka", "url", 1L, "https://example.invalid/file", 99, "ab", false);
        SmallFileMgr restored = v3(manager(source));
        assertEquals(source.content, restored.getSmallFile(10, "kafka", "url", false).content);
        DdlException failure = assertThrows(DdlException.class, () -> restored.getSmallFile(10, "kafka", "url", true));
        assertTrue(failure.getMessage().contains("not with content"));
        assertThrows(DdlException.class, () -> restored.getSmallFile(10, "kafka", "missing", false));
    }

    @Test
    public void testJournalRemovalAndCreationAfterImageUpdateBothIndexes() throws Exception {
        SmallFileMgr restored = v3(manager(file(1, "kafka", "ca", new byte[] {1})));
        restored.replayRemoveFile(new RemoveSmallFileLog(11, "kafka", "ca"));
        assertNotNull(restored.getSmallFile(1));
        restored.replayRemoveFile(new RemoveSmallFileLog(10, "kafka", "ca"));
        assertNull(restored.getSmallFile(1));
        assertFalse(restored.containsFile(10, "kafka", "ca"));
        restored.replayCreateFile(file(2, "kafka", "ca", new byte[] {2}));
        assertArrayEquals(new byte[] {2}, restored.getSmallFile(10, "kafka", "ca", true).getContentBytes());
        assertSame(restored.getSmallFile(2), restored.getSmallFile(10, "kafka", "ca", true));

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testMaterializeAndRepairContentOnlyWhenRequested() throws Exception {
        String originalDirectory = Config.small_file_dir;
        Config.small_file_dir = tempDir.resolve("small-files").toString();
        try {
            byte[] content = new byte[] {0, 1, 2, -1};
            SmallFileMgr restored = v3(manager(file(1, "kafka", "ca", content)));
            assertFalse(Files.exists(Path.of(Config.small_file_dir)));
            Path output = Path.of(restored.saveToFile(10, "kafka", "ca"));
            assertArrayEquals(content, Files.readAllBytes(output));
            assertEquals(output.toString(), restored.saveToFile(10, "kafka", "ca"));
            Files.write(output, new byte[] {9});
            restored.saveToFile(10, "kafka", "ca");
            assertArrayEquals(content, Files.readAllBytes(output));
        } finally {
            Config.small_file_dir = originalDirectory;
        }
    }

    @Test
    public void testEmptyManagerAndMissingCatalogError() throws Exception {
        assertTrue(v3(new SmallFileMgr()).getInfo("db").isEmpty());

        Path file = LoadImageTestSupport.entries(tempDir, SectionId.SMALL_FILE_MGR,
                SmallFileMgrPB.getDefaultInstance(), List.of(SmallFilePB.newBuilder().setId(1).setName("ca").build()));
        ImageFormatException failure = assertThrows(ImageFormatException.class,
                () -> LoadImageTestSupport.load(file, new SmallFileMgr().getImageV3Loaders()));
        assertTrue(failure.getMessage().contains("SMALL_FILE_MGR"));
    }

    @Test
    public void testRoutineLoadResolvesFilesFromTheLaterImageSection() throws Exception {
        KafkaRoutineLoadJob kafka = RoutineLoadJobProtoTest.kafka(1);
        PulsarRoutineLoadJob pulsar = RoutineLoadJobProtoTest.pulsar(2);
        setField(kafka, "customProperties", new HashMap<>(Map.of("ssl.ca.location", "FILE:ca")));
        setField(pulsar, "customProperties", new HashMap<>(Map.of("ssl.ca.location", "FILE:ca")));
        RoutineLoadMgr routine = RoutineLoadMgrImageV3Test.manager(List.of(kafka, pulsar));
        SmallFile kafkaFile = file(91, "kafka", "ca", new byte[] {1});
        SmallFile pulsarFile = file(92, "pulsar", "ca", new byte[] {2});
        SmallFileMgr files = manager(kafkaFile, pulsarFile);
        WarehouseManager warehouses = Mockito.mock(WarehouseManager.class);
        Mockito.when(global.getWarehouseMgr()).thenReturn(warehouses);
        Mockito.when(warehouses.acquireComputeResource(Mockito.any())).thenReturn(WarehouseManager.DEFAULT_RESOURCE);
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            routine.save(writer);
            files.save(writer);
        });
        RoutineLoadMgr restoredRoutine = new RoutineLoadMgr();
        SmallFileMgr restoredFiles = new SmallFileMgr();
        Mockito.when(global.getSmallFileMgr()).thenReturn(restoredFiles);
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>(restoredRoutine.getImageV3Loaders());
        loaders.addAll(restoredFiles.getImageV3Loaders());
        LoadImageTestSupport.load(path, loaders);
        KafkaRoutineLoadJob restoredKafka = (KafkaRoutineLoadJob) restoredRoutine.getJob(1);
        PulsarRoutineLoadJob restoredPulsar = (PulsarRoutineLoadJob) restoredRoutine.getJob(2);
        assertTrue(restoredKafka.getConvertedCustomProperties().isEmpty());
        assertTrue(restoredPulsar.getConvertedCustomProperties().isEmpty());
        restoredKafka.prepare();
        restoredPulsar.prepare();
        assertEquals("FILE:91:" + kafkaFile.md5, restoredKafka.getConvertedCustomProperties().get("ssl.ca.location"));
        assertEquals("FILE:92:" + pulsarFile.md5, restoredPulsar.getConvertedCustomProperties().get("ssl.ca.location"));

    }
}
