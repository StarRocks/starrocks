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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.DefaultGsonBuilderFactory;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.DatabasePB;
import com.starrocks.persist.proto.FunctionPB;
import com.starrocks.persist.proto.LocalMetastorePB;
import com.starrocks.persist.proto.PartitionEntryPB;
import com.starrocks.persist.proto.TableEntryPB;
import com.starrocks.persist.proto.TypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

public class DatabaseImageV3Test {
    @TempDir
    Path tempDir;
    private boolean originalIgnore;

    @BeforeEach
    public void saveConfig() {
        originalIgnore = Config.metadata_ignore_unknown_subtype;
    }

    @AfterEach
    public void restoreConfig() {
        Config.metadata_ignore_unknown_subtype = originalIgnore;
    }

    private static ScalarFunction function(Type argument) {
        return ScalarFunction.createUdf(new FunctionName("udf_db", "udf"), new Type[] {argument}, IntegerType.INT,
                false, TFunctionBinaryType.SRJAR, "hdfs://host/udf.jar", "com.example.Udf", null, null, null);
    }

    private static DatabasePB database(List<FunctionPB> functions) {
        return new Database(10001, "udf_db").toProto().toBuilder()
                .setDataQuotaBytes(12345).setReplicaQuotaSize(17).addAllFunctions(functions).build();
    }

    static Stream<FunctionPB> unknownFunctions() {
        FunctionPB known = function(IntegerType.INT).toProto();
        TypePB unknownType = TypePB.newBuilder().setKindValue(999).build();
        return Stream.of(known.toBuilder().setKindValue(999).build(),
                known.toBuilder().setCore(known.getCore().toBuilder().setRetType(unknownType)).build(),
                known.toBuilder().setCore(known.getCore().toBuilder().setArgTypes(0, unknownType)).build());
    }

    @ParameterizedTest
    @MethodSource("unknownFunctions")
    public void testSkipOnlyTheUnknownFunction(FunctionPB unknown) {
        FunctionPB before = function(IntegerType.INT).toProto();
        FunctionPB after = function(IntegerType.BIGINT).toProto();
        DatabasePB pb = database(List.of(before, unknown, after));

        Config.metadata_ignore_unknown_subtype = false;
        assertThrows(SubtypeNotFoundException.class, () -> Database.fromProto(pb));

        Config.metadata_ignore_unknown_subtype = true;
        Database loaded = Database.fromProto(pb);
        assertDatabase(loaded);
        assertEquals(List.of(before, after), loaded.getFunctionsByName("udf").stream().map(Function::toProto).toList());
        assertEquals(2, loaded.getFunctions().size());
    }

    @Test
    public void testUnknownFunctionMatchesV2DatabaseRecovery() {
        Config.metadata_ignore_unknown_subtype = true;
        // V2 installs its collection filtering when the Gson builder is created.
        Gson gson = new DefaultGsonBuilderFactory().create().create();
        JsonObject json = gson.toJsonTree(new Database(10001, "udf_db")).getAsJsonObject();
        json.addProperty("d", 12345);
        json.addProperty("r", 17);
        JsonArray overloads = new JsonArray();
        overloads.add(gson.toJsonTree(function(IntegerType.INT), Function.class));
        JsonObject unknown = new JsonObject();
        unknown.addProperty("clazz", "FutureFunction");
        overloads.add(unknown);
        overloads.add(gson.toJsonTree(function(IntegerType.BIGINT), Function.class));
        json.getAsJsonObject("f").add("udf", overloads);

        Database v2 = gson.fromJson(json, Database.class);
        Database v3 = Database.fromProto(database(List.of(function(IntegerType.INT).toProto(),
                FunctionPB.newBuilder().setKindValue(999).build(), function(IntegerType.BIGINT).toProto())));
        assertDatabase(v2);
        assertDatabase(v3);
        assertEquals(2, v2.getFunctionsByName("udf").size());
        assertEquals(v2.toProto(), v3.toProto());
    }

    @Test
    public void testMalformedKnownFunctionIsNotIgnored() {
        Config.metadata_ignore_unknown_subtype = true;
        FunctionPB known = function(IntegerType.INT).toProto();
        for (FunctionPB malformed : List.of(FunctionPB.getDefaultInstance(),
                known.toBuilder().setCore(known.getCore().toBuilder().setBinaryTypeValue(999)).build())) {
            assertThrows(IllegalArgumentException.class, () -> Database.fromProto(database(List.of(malformed))));
        }
    }

    @Test
    public void testWarningIdentifiesTheSkippedFunctionAndDatabase() {
        Config.metadata_ignore_unknown_subtype = true;
        List<LogEvent> events = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("database-function-warning", null, null) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        org.apache.logging.log4j.core.Logger logger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Database.class);
        Level oldLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        Configurator.setLevel(Database.class.getName(), Level.WARN);
        try {
            Database loaded = Database.fromProto(database(List.of(
                    function(IntegerType.INT).toProto().toBuilder().setKindValue(999).build())));
            assertDatabase(loaded);
            assertTrue(loaded.getFunctions().isEmpty());
            assertEquals(1, events.size());
            LogEvent event = events.get(0);
            assertEquals(Level.WARN, event.getLevel());
            String message = event.getMessage().getFormattedMessage();
            assertTrue(message.contains("function udf"));
            assertTrue(message.contains("udf_db"));
            assertTrue(message.contains("10001"));
            assertTrue(message.contains("999"));
            assertInstanceOf(SubtypeNotFoundException.class, event.getThrown());
        } finally {
            Configurator.setLevel(Database.class.getName(), oldLevel);
            logger.removeAppender(appender);
            appender.stop();
        }
    }

    @Test
    public void testImageRetainsDatabasesTablesAndPartitions()
            throws Exception {
        Config.metadata_ignore_unknown_subtype = true;
        Path file = writeImage();
        LocalMetastore loaded = newMetastore();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(loaded.getImageV3Loaders());
        }
        Database db = loaded.getDb(10001);
        assertDatabase(db);
        assertSame(db, loaded.getDb("udf_db"));
        assertEquals(List.of("udf(INT)", "udf(BIGINT)"),
                db.getFunctionsByName("udf").stream().map(Function::signatureString).toList());
        OlapTable table = assertInstanceOf(OlapTable.class, db.getTable(20001));
        assertSame(table, db.getTable("t"));
        assertEquals(1, table.getPartitions().size());
        assertEquals(30001, table.getPartition("p").getId());
        assertEquals(40001, table.getPartition("p").getDefaultPhysicalPartition().getId());
        Database empty = loaded.getDb(10002);
        assertNotNull(empty);
        assertSame(empty, loaded.getDb("empty_db"));
        assertTrue(empty.getFunctions().isEmpty());
        assertNotNull(loaded.getDb("next_db"));

        Config.metadata_ignore_unknown_subtype = false;
        LocalMetastore strict = newMetastore();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            ImageFormatException failure = assertThrows(ImageFormatException.class,
                    () -> reader.load(strict.getImageV3Loaders()));
            assertInstanceOf(SubtypeNotFoundException.class, failure.getCause());
        }
        assertNull(strict.getDb(10001));
    }

    private static void assertDatabase(Database db) {
        assertNotNull(db);
        assertEquals(10001, db.getId());
        assertEquals("udf_db", db.getFullName());
        assertEquals(12345, db.getDataQuota());
        assertEquals(17, db.getReplicaQuota());
    }

    private static LocalMetastore newMetastore() {
        GlobalStateMgr owner = mock(GlobalStateMgr.class, RETURNS_DEEP_STUBS);
        return new LocalMetastore(owner, owner.getRecycleBin(), owner.getColocateTableIndex());
    }

    private Path writeImage() throws Exception {
        Column column = new Column("k", IntegerType.INT);
        SinglePartitionInfo info = new SinglePartitionInfo();
        HashDistributionInfo distribution = new HashDistributionInfo(1, List.of(column));
        OlapTable table = new OlapTable(20001, "t", List.of(column), KeysType.DUP_KEYS, info, distribution);
        table.setBaseIndexMetaId(20001);
        table.setIndexMeta(20001, "t", List.of(column), 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        Partition partition = new Partition(30001, 40001, "p",
                new MaterializedIndex(20001, MaterializedIndex.IndexState.NORMAL), distribution);
        info.addPartition(partition.getId(), DataProperty.DEFAULT_DATA_PROPERTY, (short) 1);
        table.addPartition(partition);
        FunctionPB unknown = FunctionPB.newBuilder().setKindValue(999).build();

        Path file = tempDir.resolve("functions.image");
        // One entry per sub-range also exercises parallel database conversion.
        try (ImageWriterV3 writer = new ImageWriterV3(Files.newOutputStream(file), 7)) {
            try (SectionWriter section = writer.beginSection(SectionId.LOCAL_METASTORE_DB)) {
                section.writeManager(LocalMetastorePB.getDefaultInstance());
                section.writeEntry(database(List.of(function(IntegerType.INT).toProto(), unknown,
                        function(IntegerType.BIGINT).toProto())));
                section.writeEntry(new Database(10002, "empty_db").toProto().toBuilder().addFunctions(unknown).build());
                section.writeEntry(new Database(10003, "next_db").toProto());
            }
            try (SectionWriter section = writer.beginSection(SectionId.LOCAL_METASTORE_TABLE)) {
                section.writeEntry(TableEntryPB.newBuilder().setDbId(10001).setTable(table.toProto()).build());
            }
            try (SectionWriter section = writer.beginSection(SectionId.LOCAL_METASTORE_PARTITION)) {
                section.writeEntry(PartitionEntryPB.newBuilder().setTableId(20001)
                        .setPartition(partition.toProto()).build());
            }
            writer.finish();
        }
        return file;
    }
}
