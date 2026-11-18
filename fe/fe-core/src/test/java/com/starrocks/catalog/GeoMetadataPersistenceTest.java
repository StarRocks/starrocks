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

package com.starrocks.catalog;

import com.google.gson.stream.JsonReader;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.EditLogDeserializer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.transaction.MockedMetadataMgr;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import static com.starrocks.persist.OperationType.OP_ADD_FUNCTION_V2;
import static com.starrocks.persist.OperationType.OP_CREATE_TABLE_V2;
import static com.starrocks.persist.OperationType.OP_MODIFY_VIEW_DEF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeoMetadataPersistenceTest {
    private static final String DB = "geo_metadata_persistence";
    private static final long DB_ID = 900001;
    private static final List<String> CRS = List.of("", "EPSG:3857", "EPSG:4326", "custom:local");
    private GlobalStateMgr state;
    private LocalMetastore originalMetastore;
    private MetadataMgr originalMetadataMgr;
    private boolean originalEnableUdf;
    private ConnectContext context;
    private StarRocksAssert sql;

    @BeforeEach
    void setUp() {
        UtFrameUtils.setUpForPersistTest();
        state = GlobalStateMgr.getCurrentState();
        originalMetastore = state.getLocalMetastore();
        originalMetadataMgr = state.getMetadataMgr();
        originalEnableUdf = Config.enable_udf;
        Config.enable_udf = true;
        context = UtFrameUtils.createDefaultCtx();
        context.setDatabase(DB);
        sql = new StarRocksAssert(context);
        resetOwners();
    }

    @AfterEach
    void tearDown() {
        clearGlobalFunctions();
        state.setLocalMetastore(originalMetastore);
        state.setMetadataMgr(originalMetadataMgr);
        Config.enable_udf = originalEnableUdf;
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    void testOwnerImagesAndJournal() throws Exception {
        StringBuilder statements = new StringBuilder();
        ByteArrayOutputStream creates = new ByteArrayOutputStream();
        ByteArrayOutputStream alters = new ByteArrayOutputStream();
        for (int i = 0; i < CRS.size(); ++i) {
            execute("CREATE VIEW v" + i + " AS " + viewQuery(i, false), statements);
            appendJournal(creates, OP_CREATE_TABLE_V2, new CreateTableInfo(DB, view(i), ""));
            for (boolean global : List.of(false, true)) {
                String name = functionName(i, global);
                execute("CREATE " + (global ? "GLOBAL " : "") + "FUNCTION " + name
                        + "(wkt STRING) RETURNS " + expression(i, false, "wkt"), statements);
                appendJournal(creates, OP_ADD_FUNCTION_V2, function(i, global));
            }
        }
        byte[] localImage = saveLocalImage();
        byte[] globalImage = saveGlobalImage();
        for (int i = 0; i < CRS.size(); ++i) {
            String verb = i % 2 == 0 ? "CREATE OR REPLACE VIEW " : "ALTER VIEW ";
            execute(verb + "v" + i + " AS " + viewQuery(i, true), statements);
            View view = view(i);
            appendJournal(alters, OP_MODIFY_VIEW_DEF, new AlterViewInfo(DB_ID, view.getId(), view.getInlineViewDef(),
                    view.getFullSchema(), view.getSqlMode(), view.getComment(), view.getDDLViewDef()));
        }
        // Run this method on the pinned pre-fix writer to produce complete, unmodified owner fixtures.
        // Export precedes the preservation assertion, which fails on that writer.
        String fixtureDir = System.getProperty("geo.metadata.fixture.dir");
        if (fixtureDir != null) {
            Path path = Files.createDirectories(Path.of(fixtureDir));
            Files.write(path.resolve("local-meta.image.json"), localImage);
            Files.write(path.resolve("global-functions.image.json"), globalImage);
            Files.write(path.resolve("create.journal"), creates.toByteArray());
            Files.write(path.resolve("alter.journal"), alters.toByteArray());
            Files.writeString(path.resolve("fixtures.sql"), statements);
        }
        loadImages(localImage, globalImage);
        assertOwners(false, false);
        checkpointAndReload(false, false);
        resetOwners();
        replay(creates.toByteArray());
        assertOwners(false, false);
        replay(alters.toByteArray());
        assertOwners(false, true);
        checkpointAndReload(false, true);
    }

    @Test
    void testLegacyOwnerImagesAndJournal() throws Exception {
        loadImages(fixture("local-meta.image.json"), fixture("global-functions.image.json"));
        assertOwners(true, false);
        checkpointAndReload(true, false);
        resetOwners();
        replay(fixture("create.journal"));
        assertOwners(true, false);
        replay(fixture("alter.journal"));
        assertOwners(true, true);
        checkpointAndReload(true, true);
    }

    private void resetOwners() {
        clearGlobalFunctions();
        LocalMetastore metastore = new LocalMetastore(state, state.getRecycleBin(), state.getColocateTableIndex());
        state.setLocalMetastore(metastore);
        state.setMetadataMgr(new MockedMetadataMgr(metastore, state.getConnectorMgr()));
        metastore.unprotectCreateDb(new Database(DB_ID, DB));
    }

    private void clearGlobalFunctions() {
        for (Function function : state.getGlobalFunctionMgr().getFunctions()) {
            if (function.getFunctionName().getFunction().startsWith("geo_metadata_global_")) {
                state.getGlobalFunctionMgr().replayDropFunction(new FunctionSearchDesc(
                        function.getFunctionName(), function.getArgs(), function.hasVarArgs()));
            }
        }
    }

    private void execute(String statement, StringBuilder statements) throws Exception {
        sql.ddl(statement);
        statements.append(statement).append(";\n");
    }

    private static String expression(int i, boolean altered, String argument) {
        if (i == 0) {
            return "ST_GeogFromText(" + argument + ")";
        }
        String crs = altered ? (i == 1 ? "EPSG:4326" : "EPSG:3857") : CRS.get(i);
        return "ST_GeomFromText(" + argument + ", '" + crs + "')";
    }

    private static String viewQuery(int i, boolean altered) {
        return "SELECT " + expression(i, altered, "'POINT (1 2)'") + " AS g, CAST(7 AS DECIMAL(12,3)) AS n";
    }

    private View view(int i) {
        return (View) state.getLocalMetastore().getTable(DB, "v" + i);
    }

    private static String functionName(int i, boolean global) {
        return global ? "geo_metadata_global_" + i : "f" + i;
    }

    private Function function(int i, boolean global) {
        List<Function> functions = global ? state.getGlobalFunctionMgr().getFunctions()
                : state.getLocalMetastore().getDb(DB).getFunctions();
        return functions.stream().filter(f -> f.getFunctionName().getFunction().equals(functionName(i, global)))
                .findFirst().orElseThrow();
    }

    private Type queryType(String query) throws Exception {
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(query, context);
        return statement.getQueryRelation().getScope().getRelationFields().getFieldByIndex(0).getType();
    }

    private void assertOwners(boolean legacy, boolean altered) throws Exception {
        for (int i = 0; i < CRS.size(); ++i) {
            Type expected = queryType(viewQuery(i, altered));
            View view = view(i);
            assertNotNull(view);
            assertCatalogType(expected, view.getColumn("g").getType(), legacy);
            Type decimal = queryType("SELECT CAST(7 AS DECIMAL(12,3))");
            assertEquals(decimal, view.getColumn("n").getType());
            assertEquals(expected, queryType("SELECT g FROM v" + i));
            assertCatalogType(expected, view.getColumn("g").getType(), legacy);
            assertEquals("g", sql.show("DESCRIBE v" + i).get(0).get(0));
            assertEquals(expected.toSql().toLowerCase(Locale.ROOT),
                    sql.show("DESCRIBE v" + i).get(0).get(1).toLowerCase(Locale.ROOT));
            assertTrue(sql.show("SHOW CREATE VIEW v" + i).get(0).get(1).contains(view.getDDLViewDef()));
            for (boolean global : List.of(false, true)) {
                Function function = function(i, global);
                Type returnType = queryType("SELECT " + expression(i, false, "'POINT (1 2)'"));
                assertCatalogType(returnType, function.getReturnType(), legacy);
                assertEquals(returnType, queryType("SELECT " + functionName(i, global) + "('POINT (1 2)')"));
                assertCatalogType(returnType, function.getReturnType(), legacy);
                List<List<String>> shown = sql.show("SHOW CREATE " + (global ? "GLOBAL " : "") + "FUNCTION "
                        + functionName(i, global) + "(STRING)");
                assertEquals(1, shown.size());
                assertTrue(shown.get(0).toString().contains(((SqlFunction) function).getSql()));
            }
        }
    }

    private static void assertCatalogType(Type expected, Type actual, boolean legacy) {
        assertEquals(expected.getPrimitiveType(), actual.getPrimitiveType());
        if (legacy) {
            assertNull(((ScalarType) actual).getGeoDescriptor());
        } else {
            assertEquals(expected, actual);
            GeoTypeDescriptor descriptor = ((ScalarType) actual).getGeoDescriptor();
            assertNotNull(descriptor);
            descriptor.validate(actual.getPrimitiveType());
        }
    }

    private byte[] saveLocalImage() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        state.getLocalMetastore().save(image.getImageWriter());
        return image.getDataInputStream().readAllBytes();
    }

    private byte[] saveGlobalImage() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        state.getGlobalFunctionMgr().save(image.getImageWriter());
        return image.getDataInputStream().readAllBytes();
    }

    private void loadImages(byte[] localImage, byte[] globalImage) throws Exception {
        resetOwners();
        try (JsonReader input = jsonReader(localImage)) {
            SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(input);
            state.getLocalMetastore().load(reader);
            reader.close();
        }
        try (JsonReader input = jsonReader(globalImage)) {
            SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(input);
            state.getGlobalFunctionMgr().load(reader);
            reader.close();
        }
    }

    private static JsonReader jsonReader(byte[] bytes) {
        return new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8));
    }

    private void checkpointAndReload(boolean legacy, boolean altered) throws Exception {
        byte[] localImage = saveLocalImage();
        byte[] globalImage = saveGlobalImage();
        for (byte[] bytes : List.of(localImage, globalImage)) {
            String json = new String(bytes, StandardCharsets.UTF_8);
            assertFalse(json.contains("\"geo\":null"));
            assertEquals(!legacy, json.contains("\"geo\":"));
        }
        loadImages(localImage, globalImage);
        assertOwners(legacy, altered);
    }

    private static void appendJournal(ByteArrayOutputStream bytes, short opcode, Writable value) throws Exception {
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeShort(opcode);
        Text.writeString(output, GsonUtils.GSON.toJson(value));
    }

    private void replay(byte[] bytes) throws Exception {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            while (input.available() > 0) {
                short opcode = input.readShort();
                Writable value = EditLogDeserializer.deserialize(opcode, input);
                state.getEditLog().loadJournal(state, new JournalEntity(opcode, value));
            }
        }
    }

    private static byte[] fixture(String name) throws Exception {
        try (InputStream input = GeoMetadataPersistenceTest.class.getResourceAsStream("/geo-metadata/legacy/" + name)) {
            assertNotNull(input, name);
            return input.readAllBytes();
        }
    }
}
