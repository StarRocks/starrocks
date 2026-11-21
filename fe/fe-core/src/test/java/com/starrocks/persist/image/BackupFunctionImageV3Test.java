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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupHandler;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.BackupJobInfo;
import com.starrocks.backup.BackupMeta;
import com.starrocks.backup.RestoreJob;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.common.Config;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.backup.LakeBackupJob;
import com.starrocks.lake.backup.LakeRestoreJob;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.persist.proto.AbstractJobPB;
import com.starrocks.persist.proto.BackupFunctionListPB;
import com.starrocks.persist.proto.BackupJobCorePB;
import com.starrocks.persist.proto.BackupMgrPB;
import com.starrocks.persist.proto.FunctionPB;
import com.starrocks.persist.proto.RepositoryMgrPB;
import com.starrocks.persist.proto.RestoreJobCorePB;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackupFunctionImageV3Test {
    private enum Location {
        BACKUP_FUNCTIONS, BACKUP_META, RESTORE_META
    }

    private static boolean previousIgnoreUnknownSubtype;

    @TempDir
    Path tempDir;

    @BeforeAll
    public static void beforeClass() {
        previousIgnoreUnknownSubtype = Config.metadata_ignore_unknown_subtype;
        // V2 installs collection filtering when GsonUtils initializes, as on FE startup.
        Config.metadata_ignore_unknown_subtype = true;
        ExtensionManager.getInstance().loadExtensionsFromClassPath("target/classes");
    }

    @AfterEach
    public void afterTest() {
        Config.metadata_ignore_unknown_subtype = true;
    }

    @AfterAll
    public static void afterClass() {
        Config.metadata_ignore_unknown_subtype = previousIgnoreUnknownSubtype;
    }

    private static Stream<Arguments> locations() {
        return Stream.of(Location.values()).flatMap(location -> Stream.of(false, true)
                .map(lake -> Arguments.of(location, lake)));
    }

    private static Stream<Arguments> imageCases() {
        List<Arguments> cases = new ArrayList<>();
        for (Location location : Location.values()) {
            for (boolean lake : List.of(false, true)) {
                cases.add(Arguments.of(location, lake));


            }
        }
        return cases.stream();
    }

    private static Function function(int index) {
        return new ScalarFunction(800 + index, new FunctionName("backup_db", "fn_" + index),
                List.of(IntegerType.BIGINT), IntegerType.BIGINT, false);
    }

    private static AbstractJob source(Location location, boolean lake, List<Function> functions) {
        BackupMeta meta = new BackupMeta(List.of(BackupMetadataProtoTest.lakeTable()));
        meta.setCatalogs(List.of(BackupJobProtoTest.catalog()));
        AbstractJob job;
        if (location == Location.RESTORE_META) {
            meta.setFunctions(functions);
            BackupJobInfo info = new BackupJobInfo();
            info.dbName = "backup_db";
            job = lake ? new LakeRestoreJob("restore", "ts", 101, "restored_db", info, false, 1,
                    600_000, null, 77, meta, null)
                    : new RestoreJob("restore", "ts", 101, "restored_db", info, false, 1,
                    600_000, null, 77, meta, null);
        } else {
            BackupJob backup = lake ? new LakeBackupJob("backup", 101, "backup_db", List.of(), 600_000, null, 77)
                    : new BackupJob("backup", 101, "backup_db", List.of(), 600_000, null, 77);
            if (location == Location.BACKUP_FUNCTIONS) {
                backup.setBackupFunctions(functions);
            } else {
                meta.setFunctions(functions);
            }
            setField(backup, "backupMeta", meta);
            job = backup;
        }
        setField(job, "jobId", 7101L);
        return job;
    }

    private static BackupFunctionListPB.Builder functions(AbstractJobPB.Builder pb, Location location, boolean lake) {
        if (location == Location.RESTORE_META) {
            RestoreJobCorePB.Builder core = lake ? pb.getLakeRestoreJobBuilder().getCoreBuilder()
                    : pb.getRestoreJobBuilder().getCoreBuilder();
            return core.getBackupMetaBuilder().getFunctionsBuilder();
        }
        BackupJobCorePB.Builder core = lake ? pb.getLakeBackupJobBuilder().getCoreBuilder()
                : pb.getBackupJobBuilder().getCoreBuilder();
        return location == Location.BACKUP_FUNCTIONS ? core.getBackupFunctionsBuilder()
                : core.getBackupMetaBuilder().getFunctionsBuilder();
    }

    private static List<Function> functions(AbstractJob job, Location location) {
        if (location == Location.BACKUP_FUNCTIONS) {
            return getField(job, "backupFunctions");
        }
        return ((BackupMeta) getField(job, "backupMeta")).getFunctions();
    }

    @ParameterizedTest
    @MethodSource("imageCases")
    public void testSkipUnknownFunctionsMatchesV2(Location location, boolean lake) throws Exception {
        List<Function> original = List.of(function(0), function(1), function(2), function(3), function(4));
        AbstractJob source = source(location, lake, original);
        JsonObject json = GsonUtils.GSON.toJsonTree(source, AbstractJob.class).getAsJsonObject();
        JsonArray jsonFunctions = location == Location.BACKUP_FUNCTIONS ? json.getAsJsonArray("backupFunctions")
                : json.getAsJsonObject("backupMeta").getAsJsonArray("functions");
        AbstractJobPB.Builder builder = source.toProto().toBuilder();
        BackupFunctionListPB.Builder protoFunctions = functions(builder, location, lake);
        // Unknown functions before, between, and after known functions must not truncate the collection.
        for (int index : List.of(0, 2, 4)) {
            jsonFunctions.get(index).getAsJsonObject().addProperty("clazz", "FutureFunction");
            protoFunctions.getValuesBuilder(index).setKindValue(99999);
        }
        AbstractJobPB pb = builder.build();
        BackupJob following = new BackupJob("following", 102, "other_db", List.of(), 600_000, null, 77);

        Path file = tempDir.resolve("functions.image");
        try (ImageWriterV3 writer = new ImageWriterV3(Files.newOutputStream(file), 1)) {
            try (SectionWriter section = writer.beginSection(SectionId.BACKUP_MGR)) {
                section.writeManager(BackupMgrPB.newBuilder().setRepoMgr(RepositoryMgrPB.getDefaultInstance()).build());
                section.writeEntry(pb);
                section.writeEntry(following.toProto());
            }
            writer.finish();
        }

        Config.metadata_ignore_unknown_subtype = false;
        assertThrows(SubtypeNotFoundException.class, () -> AbstractJob.fromProto(pb));
        BackupHandler strict = new BackupHandler();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            ImageFormatException failure = assertThrows(ImageFormatException.class,
                    () -> reader.load(strict.getImageV3Loaders()));
            assertInstanceOf(SubtypeNotFoundException.class, failure.getCause());
        }
        assertNull(strict.getJob(101));

        Config.metadata_ignore_unknown_subtype = true;
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        SRMetaBlockWriter v2Writer = image.getImageWriter().getBlockWriter(SRMetaBlockID.BACKUP_MGR, 4);
        v2Writer.writeJson(new BackupHandler());
        v2Writer.writeInt(2);
        v2Writer.writeJson(json);
        v2Writer.writeJson(following);
        v2Writer.close();
        BackupHandler v2 = new BackupHandler();
        v2.loadBackupHandlerV2(image.getMetaBlockReader());
        BackupHandler v3 = new BackupHandler();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(v3.getImageV3Loaders());
        }
        List<FunctionPB> expected = List.of(original.get(1).toProto(), original.get(3).toProto());
        for (BackupHandler loaded : List.of(v2, v3)) {
            AbstractJob job = loaded.getJob(101);
            assertNotNull(job);
            assertEquals(source.getClass(), job.getClass());
            assertEquals(source.getJobId(), job.getJobId());
            assertEquals(source.getLabel(), job.getLabel());
            assertEquals(expected, functions(job, location).stream().map(Function::toProto).toList());
            BackupMeta meta = getField(job, "backupMeta");
            assertEquals(1, meta.getTables().size());
            LakeTable table = assertInstanceOf(LakeTable.class, meta.getTable(70001L));
            assertEquals(1, table.getPartitions().size());
            assertEquals(70004, table.getPartition(70003L).getDefaultPhysicalPartition().getId());
            assertEquals(17, table.getPartition(70003L).getDefaultPhysicalPartition().getVisibleVersion());
            assertEquals("saved_catalog", meta.getCatalogs().get(0).getName());
            BackupJobProtoTest.assertJobEquivalent(following, loaded.getJob(102));
        }
        // Apply the existing table oracle to the backup-owned table's legacy partition fields.
        Set<String> ignoredFields = CatalogEquivalenceRules.LEGACY_PARTITION_FIELDS.stream()
                .map(rule -> rule.replace("^\\$", "^\\$\\.backupMeta\\.tblNameMap\\.lake_backup_table"))
                .collect(Collectors.toSet());
        GsonEquivalence.assertEquivalent(v2.getJob(101), v3.getJob(101), ignoredFields, Set.of());
        assertEquals(original, functions(source, location));
    }

    @ParameterizedTest
    @MethodSource("locations")
    public void testAllFunctionsUnknownKeepsJobAndWarns(Location location, boolean lake) {
        AbstractJobPB.Builder builder = source(location, lake, List.of(function(0))).toProto().toBuilder();
        functions(builder, location, lake).getValuesBuilder(0).setKindValue(99999);
        List<LogEvent> events = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("backup-function-warning", null, null) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        Class<?> owner = location == Location.BACKUP_FUNCTIONS ? BackupJob.class : BackupMeta.class;
        org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(owner);
        Level oldLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        Configurator.setLevel(owner.getName(), Level.WARN);
        try {
            AbstractJob loaded = AbstractJob.fromProto(builder.build());
            assertTrue(functions(loaded, location).isEmpty());
            assertEquals(7101, loaded.getJobId());
            assertEquals(1, events.size());
            LogEvent event = events.get(0);
            assertEquals(Level.WARN, event.getLevel());
            String message = event.getMessage().getFormattedMessage();
            assertTrue(message.contains("function fn_0"));
            assertTrue(message.contains("99999"));
            if (location == Location.BACKUP_FUNCTIONS) {
                assertTrue(message.contains("backup job backup (7101)"));
            } else {
                assertTrue(message.contains("backup metadata"));
            }
            assertInstanceOf(SubtypeNotFoundException.class, event.getThrown());
        } finally {
            Configurator.setLevel(owner.getName(), oldLevel);
            logger.removeAppender(appender);
            appender.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("locations")
    public void testMalformedKnownFunctionIsNotIgnored(Location location, boolean lake) {
        AbstractJobPB.Builder builder = source(location, lake, List.of(function(0))).toProto().toBuilder();
        FunctionPB known = functions(builder, location, lake).getValues(0);
        for (FunctionPB malformed : List.of(FunctionPB.getDefaultInstance(),
                known.toBuilder().setCore(known.getCore().toBuilder().setBinaryTypeValue(99999)).build())) {
            functions(builder, location, lake).setValues(0, malformed);
            assertThrows(IllegalArgumentException.class, () -> AbstractJob.fromProto(builder.build()));
        }
    }
}
