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

package com.starrocks.server;

import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.SyncMaterializedViewRecoveryException;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GtidInfo;
import com.starrocks.persist.ImageFiles;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.ImageLoader;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.NextIdLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.image.ImageModule;
import com.starrocks.persist.image.ImageReaderV3;
import com.starrocks.persist.image.SectionId;
import com.starrocks.persist.proto.GlobalStatePB;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.GtidGenerator;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV3;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GlobalImageV3Test {
    @TempDir
    Path temporary;
    private String oldMeta;
    private String oldMode;
    private boolean oldDisableV2;
    private MockedStatic<GlobalStateMgr> globals;
    private TestState current;

    @BeforeAll
    static void extensions() {
        ExtensionManager.getInstance().loadExtensionsFromClassPath("target/classes");
    }

    private static class TestState extends GlobalStateMgr {
        int postLoads;
        boolean failV3;
        RuntimeException recoveryFailure;

        @Override
        public void processMvRelatedMeta() {
            postLoads++;
            if (recoveryFailure != null) {
                throw recoveryFailure;
            }
            super.processMvRelatedMeta();
        }

        @Override
        public void saveImageV3(File file, long journalId) throws IOException {
            if (failV3) {
                throw new IOException("injected v3 saver failure");
            }
            super.saveImageV3(file, journalId);
        }
    }

    @BeforeEach
    public void setUp() {
        oldMeta = Config.meta_dir;
        oldMode = Config.run_mode;
        oldDisableV2 = Config.disable_image_v2_write;
        Config.meta_dir = temporary.toString();
        Config.run_mode = "shared_nothing";
        current = new TestState();
        globals = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        globals.when(GlobalStateMgr::getCurrentState).thenAnswer(invocation -> current);
        globals.when(GlobalStateMgr::getServingState).thenAnswer(invocation -> current);
        globals.when(GlobalStateMgr::isCheckpointThread).thenReturn(true);
        current.setEditLog(mock(EditLog.class));
    }

    @AfterEach
    public void tearDown() {
        globals.close();
        Config.meta_dir = oldMeta;
        Config.run_mode = oldMode;
        Config.disable_image_v2_write = oldDisableV2;
        RunMode.detectRunMode();
    }

    private Path root() {
        return temporary.resolve("image");
    }

    private TestState fresh() {
        current = new TestState();
        current.setEditLog(mock(EditLog.class));
        return current;
    }

    private void populate(long journalId) {
        current.initDefaultWarehouse();
        current.setReplayedJournalIdForTest(journalId);
        current.setNextId(100000);
        current.getGtidGenerator().init(GtidGenerator.encode(1_700_000_005_000L, GtidGenerator.MAX_SEQUENCE));
        Database database = new Database(10023, "image_db");
        current.getLocalMetastore().replayCreateDb(database);
        List<Column> columns = List.of(new Column("k", IntegerType.BIGINT));
        HashDistributionInfo distribution = new HashDistributionInfo(1, columns);
        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(10025, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setReplicationNum(10025, (short) 1);
        boolean lake = RunMode.isSharedDataMode();
        OlapTable table = lake
                ? new LakeTable(10024, "image_table", columns, KeysType.DUP_KEYS, partitionInfo, distribution)
                : new OlapTable(10024, "image_table", columns, KeysType.DUP_KEYS, partitionInfo, distribution);
        MaterializedIndex index = new MaterializedIndex(10027, 10027, MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        TabletMeta metadata = new TabletMeta(10023, 10024, 10026, 10027, TStorageMedium.HDD, lake);
        if (lake) {
            index.addTablet(new LakeTablet(10028), metadata);
            ((LakeTable) table).setStorageInfo(FilePathInfo.newBuilder().setFullPath("s3://bucket/image").build(),
                    new DataCacheInfo(true, false));
        } else {
            LocalTablet tablet = new LocalTablet(10028);
            tablet.addReplica(new Replica(10029, 10030, Replica.ReplicaState.NORMAL, 5, 0));
            index.addTablet(tablet, metadata);
        }
        table.setBaseIndexMetaId(10027);
        table.setIndexMeta(10027, "image_table", columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        Partition partition = new Partition(10025, 10026, "image_table", index, distribution);
        partition.getDefaultPhysicalPartition().updateVisibleVersion(5);
        partition.getDefaultPhysicalPartition().setNextVersion(6);
        partition.getDefaultPhysicalPartition().getVacuumState().advance(2, 3, 4, true, 5, Set.of(10027L));
        partition.getDefaultPhysicalPartition().getVacuumState().setMinRetainedVersion(2);
        partition.getDefaultPhysicalPartition().setLastSuccVacuumVersion(2);
        table.addPartition(partition);
        database.registerTableUnlocked(table);
        current.getSqlBlackList().put(7, Pattern.compile("forbidden"));
        current.getSqlDigestBlackList().put("digest");
    }

    @ParameterizedTest
    @ValueSource(strings = {"shared_nothing", "shared_data"})
    public void fullRegistryRoundTripAndHeader(String mode) throws Exception {
        Config.run_mode = mode;
        fresh();
        populate(20);
        List<ImageModule> modules = current.getImageV3Modules();
        assertEquals(39, modules.size()); // Global header and all persisted community managers.
        List<Integer> registered = modules.stream().flatMap(module -> module.loaders().stream())
                .map(loader -> loader.first.getValue()).toList();
        Set<Integer> expectedSections = new HashSet<>();
        for (Class<?> owner : List.of(SectionId.class)) {
            for (Field field : owner.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) && field.getType() == SectionId.class) {
                    int id = ((SectionId) field.get(null)).getValue();
                    if (id != SectionId.INVALID.getValue()) {
                        expectedSections.add(id);
                    }
                }
            }
        }
        assertEquals(expectedSections, new HashSet<>(registered));
        assertEquals(expectedSections.size(), registered.size());
        assertEquals(SectionId.GLOBAL_STATE.getValue(), registered.get(0));
        {
            Path directory = root().resolve("v3");
            Files.createDirectories(directory);
            Path file = directory.resolve("image.20");
            current.saveImageV3(file.toFile(), 20);
            TestState source = current;
            // Independent V2 oracle from the same live state, without loading V3 first.
            Path v2Root = temporary.resolve("oracle").resolve("image");
            Path v2Directory = v2Root.resolve("v2");
            Files.createDirectories(v2Directory);
            source.saveImage(new ImageWriter(v2Directory.toString(), 20),
                    v2Directory.resolve("image.20").toFile());
            TestState oracle = fresh();
            String currentMeta = Config.meta_dir;
            try {
                Config.meta_dir = temporary.resolve("oracle").toString();
                oracle.loadImage();
                oracle.initDefaultWarehouse();
            } finally {
                Config.meta_dir = currentMeta;
            }
            assertEquals(source.getGtidGenerator().getBatchEndGtid(), oracle.getGtidGenerator().getBatchEndGtid());
            assertNotNull(oracle.getWarehouseMgr().getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_ID));
            assertNotNull(oracle.getLocalMetastore().getDb(10023).getTable(10024));
            PhysicalPartition oraclePartition = ((OlapTable) oracle.getLocalMetastore().getDb(10023).getTable(10024))
                    .getPartition(10025).getDefaultPhysicalPartition();
            assertEquals(2, oraclePartition.getLastSuccVacuumVersion());
            assertEquals(4, oraclePartition.getVacuumState().getNextProposeStartVersion());
            assertEquals(Set.of("digest"), oracle.getSqlDigestBlackList().getDigests());
            try (ImageReaderV3 reader = new ImageReaderV3(file)) {
                assertEquals(registered, reader.sections().stream().map(section -> section.getType()).toList());
            }
            TestState restored = fresh();
            restored.loadImage();
            restored.initDefaultWarehouse();
            assertEquals(20, restored.getImageJournalId());
            assertEquals(20, restored.getReplayedJournalId());
            assertEquals(source.getGtidGenerator().getBatchEndGtid(), restored.getGtidGenerator().getBatchEndGtid());
            assertEquals(oracle.getGtidGenerator().lastGtid(), restored.getGtidGenerator().lastGtid());
            assertNotNull(restored.getLocalMetastore().getDb(10023));
            OlapTable table = (OlapTable) restored.getLocalMetastore().getDb(10023).getTable(10024);
            assertNotNull(table);
            assertEquals(mode.equals("shared_data"), table instanceof LakeTable);
            PhysicalPartition partition = table.getPartition(10025).getDefaultPhysicalPartition();
            assertEquals(5, partition.getVisibleVersion());
            assertEquals(6, partition.getNextVersion());
            assertEquals(2, partition.getLastSuccVacuumVersion());
            assertEquals(2, partition.getVacuumState().getMinRetainedVersion());
            assertEquals(2, partition.getVacuumState().getToDeleteLow());
            assertEquals(3, partition.getVacuumState().getToDeleteHigh());
            assertEquals(4, partition.getVacuumState().getNextProposeStartVersion());
            assertEquals(5, partition.getVacuumState().getPassStartVersion());
            assertEquals(Set.of(10027L), partition.getVacuumState().getPassStartIndexIds());
            assertEquals(10028, partition.getLatestBaseIndex().getTablets().get(0).getId());
            assertNotNull(restored.getWarehouseMgr().getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID));
            assertEquals(1, restored.postLoads);
            assertTrue(restored.getNextId() >= 100000);
            assertEquals(Set.of("digest"), restored.getSqlDigestBlackList().getDigests());
            globals.when(GlobalStateMgr::isCheckpointThread).thenReturn(false);
            TestState serving = fresh();
            serving.loadImage();
            assertEquals(10024, serving.getTabletInvertedIndex().getTabletMeta(10028).getTableId());
            assertEquals(1, serving.postLoads);
            globals.when(GlobalStateMgr::isCheckpointThread).thenReturn(true);
            current = restored;
        }
    }

    @Test
    public void v2LoadThenV3AndTailJournalReplay() throws Exception {
        populate(30);
        Path directory = root().resolve("v2");
        Files.createDirectories(directory);
        current.saveImage(new ImageWriter(directory.toString(), 30), directory.resolve("image.30").toFile());
        TestState fromV2 = fresh();
        fromV2.loadImage();
        fromV2.initDefaultWarehouse();
        assertNotNull(fromV2.getLocalMetastore().getDb(10023));
        fromV2.saveImage(false);
        TestState restored = fresh();
        restored.loadImage();
        restored.initDefaultWarehouse();
        Journal journal = mock(Journal.class);
        JournalCursor cursor = mock(JournalCursor.class);
        long gtidBound = GtidGenerator.encode(1_700_000_100_000L, GtidGenerator.MAX_SEQUENCE);
        when(journal.read(31, 32)).thenReturn(cursor);
        when(cursor.next()).thenReturn(new JournalEntity(OperationType.OP_SAVE_NEXTID_V2, new NextIdLog(200000)),
                new JournalEntity(OperationType.OP_SAVE_GTID, new GtidInfo(gtidBound)), null);
        restored.setJournal(journal);
        restored.setEditLog(new EditLog(null));
        restored.replayJournal(32);
        verify(journal).read(31, 32);
        verify(cursor).close();
        assertEquals(32, restored.getReplayedJournalId());
        assertEquals(gtidBound, restored.getGtidGenerator().getBatchEndGtid());
        assertEquals(30, restored.getImageJournalId());
        restored.setEditLog(mock(EditLog.class));
        assertTrue(restored.getNextId() > 200000);
        assertEquals(1, restored.postLoads);
    }

    @Test
    public void disableAndReenableV2AtTheSameJournalId() throws Exception {
        populate(40);
        Config.disable_image_v2_write = true;
        current.saveImage();
        assertTrue(ImageFiles.isComplete(root().toString(), 40, false));
        assertFalse(Files.exists(root().resolve("v2/image.40")));
        byte[] v3 = Files.readAllBytes(root().resolve("v3/image.40"));
        Files.write(root().resolve("v3/image.ckpt"), new byte[] {0});
        Config.disable_image_v2_write = false;
        current.saveImage();
        assertTrue(ImageFiles.isComplete(root().toString(), 40, true));
        assertArrayEquals(v3, Files.readAllBytes(root().resolve("v3/image.40")));
        Files.delete(root().resolve("v3/image.40"));
        TestState restored = fresh();
        restored.loadImage();
        assertNotNull(restored.getLocalMetastore().getDb(10023));
        assertEquals(40, restored.getReplayedJournalId());
    }

    @Test
    public void failedSecondWriterPublishesNeitherNewImage() throws Exception {
        populate(50);
        current.saveImage(true);
        current.setReplayedJournalIdForTest(51);
        current.failV3 = true;
        assertThrows(IOException.class, () -> current.saveImage(true));
        assertTrue(ImageFiles.isComplete(root().toString(), 50, true));
        assertFalse(Files.exists(root().resolve("v2/image.51")));
        assertFalse(Files.exists(root().resolve("v3/image.51")));
        assertTrue(Files.isRegularFile(root().resolve("v2/image.ckpt")));
        assertEquals(50, new ImageLoader(root().toString()).getImageJournalId());
        current.failV3 = false;
        current.saveImage(true);
        assertTrue(ImageFiles.isComplete(root().toString(), 51, true));
        assertFalse(Files.exists(root().resolve("v2/image.ckpt")));
        assertFalse(Files.exists(root().resolve("v3/image.ckpt")));
    }

    @Test
    public void corruptPreferredV3DoesNotSilentlyLoadV2() throws Exception {
        populate(60);
        current.saveImage(true);
        Files.write(root().resolve("v3/image.60"), new byte[] {0, 1});
        TestState restored = fresh();
        assertThrows(IOException.class, restored::loadImage);
        assertEquals(0, restored.postLoads);
        assertEquals(0, restored.getImageJournalId());
        assertEquals(ImageFormatVersion.v3, new ImageLoader(root().toString()).getImageFormatVersion());
    }

    @Test
    public void syncMvRecoveryFailureDoesNotPublishImageWatermark() throws Exception {
        populate(65);
        current.saveImage(true);
        TestState restored = fresh();
        restored.recoveryFailure = mock(SyncMaterializedViewRecoveryException.class);
        assertSame(restored.recoveryFailure, assertThrows(SyncMaterializedViewRecoveryException.class, restored::loadImage));
        assertEquals(1, restored.postLoads);
        assertEquals(0, restored.getImageJournalId());
    }

    @Test
    public void missingGlobalStateAndFieldsFailBeforeBusinessModules() throws Exception {
        writeV3(root(), 70, null);
        assertThrows(IOException.class, current::loadImage);
        writeV3(root(), 70,
                GlobalStatePB.getDefaultInstance());
        assertThrows(IOException.class, current::loadImage);
        assertEquals(0, current.postLoads);
        assertEquals(0, current.getImageJournalId());
    }

    @Test
    public void callerInitializesDefaultWarehouseAfterLoadingWithoutAnImage() throws Exception {
        current.loadImage();
        assertNull(current.getWarehouseMgr().getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_ID));
        assertEquals(0, current.postLoads);
        assertEquals(0, current.getImageJournalId());
        current.initDefaultWarehouse();
        assertNotNull(current.getWarehouseMgr().getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID));
    }

    @ParameterizedTest
    @ValueSource(strings = {"shared_nothing", "shared_data"})
    public void warehouseInitializationAfterImageSupportsRecovery(String mode) throws Exception {
        Config.run_mode = mode;
        fresh();
        populate(90);
        Database database = current.getLocalMetastore().getDb(10023);
        OlapTable table = (OlapTable) database.getTable(10024);
        current.getStreamLoadMgr().addLoadTask(new StreamLoadTask(501, database, table, "ordinary",
                "user", "127.0.0.1", 120000, 1, 0, System.currentTimeMillis(), WarehouseComputeResource.of(0)));
        current.getStreamLoadMgr().addLoadTask(new StreamLoadMultiStmtTask(502, database, "multi", "user",
                "127.0.0.1", 120000, System.currentTimeMillis(), WarehouseComputeResource.of(0)));
        TestState source = current;
        for (int i = 0; i < 3; i++) {
            current = source;
            long journalId = 90 + i;
            current.setReplayedJournalIdForTest(journalId);
            ImageFormatVersion format = i == 0 ? ImageFormatVersion.v2 : ImageFormatVersion.v3;
            Path directory = root().resolve(format.name());
            Files.createDirectories(directory);
            File file = directory.resolve("image." + journalId).toFile();
            if (format == ImageFormatVersion.v2) {
                current.saveImage(new ImageWriter(directory.toString(), journalId), file);
            } else {
                current.saveImageV3(file, journalId);
            }
            TestState restored = fresh();
            assertNull(restored.getWarehouseMgr().getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_ID));
            restored.loadImage();
            restored.initDefaultWarehouse();
            assertNotNull(restored.getWarehouseMgr().getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_ID));
            assertEquals(journalId, restored.getImageJournalId());
            assertEquals(journalId, restored.getReplayedJournalId());
            assertNotNull(restored.getLocalMetastore().getDb(10023).getTable(10024));
            assertEquals(Set.of("digest"), restored.getSqlDigestBlackList().getDigests());
            assertEquals(1, restored.getStreamLoadMgr().getTask("image_db", "ordinary", true).size());
            assertEquals(1, restored.getStreamLoadMgr().getTask("image_db", "multi", true).size());
            assertNotNull(restored.getGlobalTransactionMgr().getCallbackFactory().getCallback(501));
            assertNotNull(restored.getGlobalTransactionMgr().getCallbackFactory().getCallback(502));
            assertEquals(1, restored.postLoads);
            restored.initDefaultWarehouse();
            assertNotNull(restored.getWarehouseMgr().getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID));
            Journal journal = mock(Journal.class);
            JournalCursor cursor = mock(JournalCursor.class);
            when(journal.read(journalId + 1, journalId + 1)).thenReturn(cursor);
            when(cursor.next()).thenReturn(new JournalEntity(OperationType.OP_SAVE_NEXTID_V2,
                    new NextIdLog(200000)), null);
            restored.setJournal(journal);
            restored.setEditLog(new EditLog(null));
            restored.replayJournal(journalId + 1);
            assertEquals(journalId + 1, restored.getReplayedJournalId());
            assertEquals(journalId, restored.getImageJournalId());
            verify(cursor).close();
        }
    }

    @Test
    public void dumpV3CanBeLoadedAndReportsWriterFailure() throws Exception {
        populate(60);
        Journal journal = mock(Journal.class);
        when(journal.getMaxJournalId()).thenReturn(60L);
        current.setJournal(journal);
        String dump = current.dumpImage();
        assertNotNull(dump);
        assertEquals("image.v3.60", Path.of(dump).getFileName().toString());
        ImageFiles.validate(Path.of(dump), ImageFormatVersion.v3, 60);
        Files.createDirectories(root().resolve("v3"));
        Files.copy(Path.of(dump), root().resolve("v3/image.60"));
        TestState loaded = fresh();
        loaded.loadImage();
        assertNotNull(loaded.getLocalMetastore().getDb(10023).getTable(10024));
        loaded.failV3 = true;
        loaded.setJournal(journal);
        assertNull(loaded.dumpImage());
    }

    @Test
    public void lateCheckpointCompletionKeepsNewerDurableWatermark() throws Exception {
        populate(70);
        current.saveImage(false);
        current.setReplayedJournalIdForTest(71);
        current.saveImage(false);
        current.setImageJournalId(71);
        current.setImageJournalId(70);
        assertEquals(71, current.getImageJournalId());
        assertEquals(71, new ImageLoader(root().toString()).getImageJournalId());
    }

}
