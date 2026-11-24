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
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.load.DeleteMgr;
import com.starrocks.persist.UninstallPluginLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.DigitalVersionPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.PluginInfoPB;
import com.starrocks.persist.proto.PluginMgrPB;
import com.starrocks.plugin.DynamicPluginLoader;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.plugin.PluginLoader;
import com.starrocks.plugin.PluginLoader.PluginStatus;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PluginMgrImageV3Test {
    @TempDir
    Path tempDir;
    private String originalDirectory;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        originalDirectory = Config.plugin_dir;
        Config.plugin_dir = tempDir.resolve("plugins").toString();
        new MockUp<FrontendOptions>() {
            @Mock
            public String getLocalHostAddress() {
                return "127.0.0.1";
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.plugin_dir = originalDirectory;
    }

    static PluginInfo info(String name, PluginType type) {
        PluginInfo info = new PluginInfo(name, type, "description", new DigitalVersion(110099),
                new DigitalVersion(17000000), "example.AuditPlugin", "", "https://example.invalid/plugin.zip");
        info.setProperties(new HashMap<>(Map.of("endpoint", "https://example.invalid/audit", "batch_size", "20")));
        return info;
    }

    private static PluginMgr manager(PluginInfo... infos) throws IOException {
        PluginMgr mgr = new PluginMgr();
        // The shared fixture marks this as a checkpoint thread; reload performs no external work.
        for (PluginInfo info : infos) {
            mgr.replayLoadDynamicPlugin(info);
        }
        return mgr;
    }

    private static PluginLoader loader(PluginMgr manager, String name, PluginType type) {
        Map<String, PluginLoader>[] loaders = getField(manager, "plugins");
        return loaders[type.ordinal()].get(name);
    }

    private PluginMgr v2(PluginMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        PluginMgr restored = new PluginMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restored.load(reader);
        reader.close();
        return restored;
    }

    private PluginMgr v3(PluginMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        PluginMgr restored = new PluginMgr();
        assertEquals(140, LoadImageTestSupport.load(path, restored.getImageV3Loaders()).get(0).getType());
        return restored;
    }

    @Test
    public void testAllPluginMetadataAndTypesRoundTrip() throws Exception {
        for (PluginType type : PluginType.values()) {
            PluginInfo source = info(type.name(), type);
            PluginInfo restored = PluginInfo.fromProto(PluginInfoPB.parseFrom(source.toProto().toByteArray()));
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(type, restored.getType());
            assertEquals(source.getSource(), restored.getSource());
            assertEquals(source.getVersion(), restored.getVersion());
            assertEquals(source.getJavaVersion(), restored.getJavaVersion());
            restored.getProperties().put("new-property", "value");
            assertFalse(source.getProperties().containsKey("new-property"));
        }
    }

    @Test
    public void testVersionIdAndComponentsStayIndependent() {
        DigitalVersion source = GsonUtils.GSON.fromJson(
                "{\"id\":110099,\"major\":7,\"minor\":8,\"revision\":9}", DigitalVersion.class);
        DigitalVersion restored = DigitalVersion.fromProto(source.toProto());
        assertEquals(source, restored);
        assertEquals("7.8.9", restored.toString());
        assertEquals(110099, restored.id);
        assertTrue(restored.after(new DigitalVersion(110000)));
        assertEquals(0, restored.compareTo(source));
        DigitalVersion missingComponents = DigitalVersion.fromProto(DigitalVersionPB.newBuilder().setId(110099).build());
        assertEquals("0.0.0", missingComponents.toString());
        assertEquals(110099, missingComponents.id);
    }

    @Test
    public void testVersionByteRangeAndUnknownFields() throws Exception {
        for (int value : List.of(Byte.MIN_VALUE + 0, Byte.MAX_VALUE + 0)) {
            DigitalVersionPB pb = DigitalVersionPB.newBuilder().setMajor(value).setMinor(value).setRevision(value).build();
            assertEquals(value, DigitalVersion.fromProto(pb).major);
        }
        assertThrows(IllegalArgumentException.class,
                () -> DigitalVersion.fromProto(DigitalVersionPB.newBuilder().setMajor(128).build()));
        assertThrows(IllegalArgumentException.class,
                () -> DigitalVersion.fromProto(DigitalVersionPB.newBuilder().setMinor(-129).build()));
        assertThrows(IllegalArgumentException.class,
                () -> DigitalVersion.fromProto(DigitalVersionPB.newBuilder().setRevision(256).build()));
        PluginInfo source = info("audit", PluginType.AUDIT);
        PluginInfoPB extended = source.toProto().toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(7).build()).build()).build();
        GsonEquivalence.assertEquivalent(source, PluginInfo.fromProto(PluginInfoPB.parseFrom(extended.toByteArray())));
    }

    @Test
    public void testDefaultsAndExplicitEmptyProperties() {
        PluginInfo absent = PluginInfo.fromProto(PluginInfoPB.getDefaultInstance());
        GsonEquivalence.assertEquivalent(new PluginInfo(), absent);
        assertNull(absent.getType());
        assertNull(absent.getVersion());
        assertTrue(absent.getProperties().isEmpty());
        PluginInfo empty = new PluginInfo("");
        empty.setProperties(new HashMap<>());
        PluginInfo restored = PluginInfo.fromProto(empty.toProto());
        assertEquals("", restored.getName());
        assertTrue(restored.getProperties().isEmpty());
        restored.getProperties().put("a", "b");
        assertEquals("b", restored.getProperties().get("a"));
    }

    @Test
    public void testOnlyDynamicPluginsAreSavedAndCheckpointLoadDoesNotInstall() throws Exception {
        PluginMgr source = manager(info("Audit", PluginType.AUDIT), info("Importer", PluginType.IMPORT));
        Plugin builtin = Mockito.mock(Plugin.class);
        assertTrue(source.registerBuiltinPlugin(new PluginInfo("__builtin_test", PluginType.AUDIT, "builtin"), builtin));
        PluginMgr baseline = v2(source);
        PluginMgr restored = v3(source);
        assertEquals(baseline.getPluginShowInfos(), restored.getPluginShowInfos());
        assertEquals(2, restored.getAllDynamicPluginInfo().size());
        assertNull(loader(restored, "__builtin_test", PluginType.AUDIT));
        assertEquals(PluginStatus.INSTALLED, loader(restored, "audit", PluginType.AUDIT).getStatus());
        assertNull(restored.getActivePlugin("AUDIT", PluginType.AUDIT));
        assertFalse(Files.exists(Path.of(Config.plugin_dir)));

        Mockito.verifyNoInteractions(global.getEditLog(), builtin);
    }

    @Test
    public void testReplayReloadRunsOncePerEntryAndExposesActivePlugin() throws Exception {
        PluginMgr source = manager(info("Audit", PluginType.AUDIT), info("Importer", PluginType.IMPORT));
        AtomicInteger reloads = new AtomicInteger();
        new MockUp<DynamicPluginLoader>() {
            @Mock
            public void reload(Invocation invocation) {
                DynamicPluginLoader loader = invocation.getInvokedInstance();
                assertEquals(PluginStatus.INSTALLING, loader.getStatus());
                Plugin plugin = Mockito.mock(Plugin.class);
                setField(loader, "plugin", plugin);
                reloads.incrementAndGet();
            }
        };
        PluginMgr restored = v3(source);
        assertEquals(PluginStatus.INSTALLED, loader(restored, "audit", PluginType.AUDIT).getStatus());
        assertNotNull(restored.getActivePlugin("audit", PluginType.AUDIT));
        assertEquals(1, restored.getActivePluginList(PluginType.IMPORT).size());
        assertSame(loader(restored, "Importer", PluginType.IMPORT).getPlugin(),
                restored.getActivePluginList(PluginType.IMPORT).get(0));

        assertEquals(2 * 1, reloads.get());
        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testReloadFailureKeepsMetadataAndNameReservation() throws Exception {
        PluginMgr source = manager(info("Audit", PluginType.AUDIT));
        new MockUp<DynamicPluginLoader>() {
            @Mock
            public void reload() throws IOException {
                throw new IOException("test reload failure");
            }
        };
        PluginMgr baseline = v2(source);
        PluginMgr restored = v3(source);
        assertEquals(PluginStatus.ERROR, loader(restored, "audit", PluginType.AUDIT).getStatus());
        assertEquals(baseline.getPluginShowInfos(), restored.getPluginShowInfos());
        assertNull(restored.getActivePlugin("audit", PluginType.AUDIT));
        Set<String> names = getField(restored, "dynamicPluginNames");
        assertTrue(names.contains("AUDIT"));
        assertEquals(1, restored.getAllDynamicPluginInfo().size());
        // Failed plugins remain in future checkpoints and can be removed by a later journal record.
        PluginMgr second = v3(restored);
        assertEquals(1, second.getAllDynamicPluginInfo().size());
        second.replayUninstallPlugin(new UninstallPluginLog("Audit"));
        assertTrue(second.getAllDynamicPluginInfo().isEmpty());

    }

    @Test
    public void testDuplicateNamesUseTheExistingCaseInsensitiveReplayRule() throws Exception {
        PluginInfo first = info("Audit", PluginType.AUDIT);
        PluginInfo duplicate = info("AUDIT", PluginType.AUDIT);
        duplicate.setProperties(Map.of("second", "discarded"));
        AtomicInteger reloads = new AtomicInteger();
        new MockUp<DynamicPluginLoader>() {
            @Mock
            public void reload() {
                reloads.incrementAndGet();
            }
        };
        Path file = LoadImageTestSupport.entries(tempDir, SectionId.PLUGIN_MGR, PluginMgrPB.getDefaultInstance(),
                List.of(first.toProto(), duplicate.toProto()));
        PluginMgr restored = new PluginMgr();
        LoadImageTestSupport.load(file, restored.getImageV3Loaders());
        assertEquals(1, reloads.get());
        assertEquals(1, restored.getAllDynamicPluginInfo().size());
        assertEquals(first.getProperties(), restored.getAllDynamicPluginInfo().get(0).getProperties());
        assertEquals(PluginStatus.INSTALLED, loader(restored, "audit", PluginType.AUDIT).getStatus());
    }

    @Test
    public void testLoadKeepsPreviouslyRegisteredBuiltinPlugins() throws Exception {
        PluginMgr restored = new PluginMgr();
        Plugin builtin = Mockito.mock(Plugin.class);
        restored.registerBuiltinPlugin(new PluginInfo("__builtin_audit", PluginType.AUDIT, "builtin"), builtin);
        Path file = LoadImageTestSupport.entries(tempDir, SectionId.PLUGIN_MGR, PluginMgrPB.getDefaultInstance(),
                List.of(info("audit", PluginType.AUDIT).toProto()));
        LoadImageTestSupport.load(file, restored.getImageV3Loaders());
        assertSame(builtin, restored.getActivePlugin("__builtin_audit", PluginType.AUDIT));
        assertEquals(1, restored.getAllDynamicPluginInfo().size());
    }

    @Test
    public void testEmptyAndInvalidMetadata() throws Exception {
        assertTrue(v3(new PluginMgr()).getAllDynamicPluginInfo().isEmpty());

        PluginInfoPB valid = info("audit", PluginType.AUDIT).toProto();
        for (PluginInfoPB invalid : List.of(valid.toBuilder().clearName().build(), valid.toBuilder().clearType().build(),
                valid.toBuilder().setTypeValue(999).build(), valid.toBuilder().setVersion(
                        DigitalVersionPB.newBuilder().setRevision(1000)).build())) {
            Path file = LoadImageTestSupport.entries(tempDir, SectionId.PLUGIN_MGR, PluginMgrPB.getDefaultInstance(),
                    List.of(invalid));
            PluginMgr restored = new PluginMgr();
            ImageFormatException failure = assertThrows(ImageFormatException.class,
                    () -> LoadImageTestSupport.load(file, restored.getImageV3Loaders()));
            assertTrue(failure.getMessage().contains("PLUGIN_MGR"));
            assertTrue(restored.getAllDynamicPluginInfo().isEmpty());
        }
    }
    @Test
    public void testConsecutiveSectionsFollowV2SaveOrder() throws Exception {
        SmallFileMgr smallFiles = new SmallFileMgr();
        smallFiles.replayCreateFile(SmallFileMgrImageV3Test.file(7, "kafka", "ca", new byte[] {7}));
        PluginMgr plugins = manager(info("audit", PluginType.AUDIT));
        DeleteMgr deletes = new DeleteMgr();
        DeleteMgrImageV3Test.history(deletes).put(10L, List.of(DeleteMgrImageV3Test.info(10, "table", 1234)));
        Path file = LoadImageTestSupport.save(tempDir, writer -> {
            smallFiles.save(writer);
            plugins.save(writer);
            deletes.save(writer);
        });
        SmallFileMgr restoredFiles = new SmallFileMgr();
        PluginMgr restoredPlugins = new PluginMgr();
        DeleteMgr restoredDeletes = new DeleteMgr();
        List<Pair<SectionId, SectionLoader>> loaders =
                new ArrayList<>(restoredFiles.getImageV3Loaders());
        loaders.addAll(restoredPlugins.getImageV3Loaders());
        loaders.addAll(restoredDeletes.getImageV3Loaders());
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaders);
        assertEquals(List.of(130, 140, 150), sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(List.of(1L, 1L, 1L), sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertEquals(7, restoredFiles.getSmallFile(10, "kafka", "ca", true).id);
        assertEquals("audit", restoredPlugins.getAllDynamicPluginInfo().get(0).getName());
        assertEquals(1, restoredDeletes.getDeleteInfoCount());
        assertEquals("table", DeleteMgrImageV3Test.history(restoredDeletes).get(10L).get(0).getTableName());

        Mockito.verifyNoInteractions(global.getEditLog());
    }

}
