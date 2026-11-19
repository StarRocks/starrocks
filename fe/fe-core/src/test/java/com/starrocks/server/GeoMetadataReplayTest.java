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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.View;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJournalCursor;
import com.starrocks.journal.bdbje.CloseSafeDatabase;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.GeoType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GeoMetadataReplayTest {
    private static final long BAD_JOURNAL_ID = 41;
    private static final long NEXT_JOURNAL_ID = BAD_JOURNAL_ID + 1;

    @TempDir
    Path directory;

    private String previousMetaDir;
    private String previousPluginDir;
    private boolean previousIgnore;
    private boolean previousRecovery;
    private String previousSkipIds;
    private GlobalStateMgr state;
    private MockedStatic<GlobalStateMgr> globalState;

    @BeforeEach
    void setUp() {
        previousMetaDir = Config.meta_dir;
        previousPluginDir = Config.plugin_dir;
        previousIgnore = Config.metadata_journal_ignore_replay_failure;
        previousRecovery = Config.metadata_enable_recovery_mode;
        previousSkipIds = Config.metadata_journal_skip_bad_journal_ids;
        Config.meta_dir = directory.resolve("meta").toString();
        Config.plugin_dir = directory.resolve("plugins").toString();
        Config.metadata_enable_recovery_mode = false;
        Config.metadata_journal_skip_bad_journal_ids = "";
        state = new GlobalStateMgr(new NodeMgr());
        state.setFrontendNodeType(FrontendNodeType.FOLLOWER);
        state.setEditLog(new EditLog(null));
        state.setReplayedJournalIdForTest(BAD_JOURNAL_ID - 1);
        globalState = mockStatic(GlobalStateMgr.class);
        globalState.when(GlobalStateMgr::getCurrentState).thenReturn(state);
        globalState.when(GlobalStateMgr::getServingState).thenReturn(state);
    }

    @AfterEach
    void tearDown() {
        globalState.close();
        Config.meta_dir = previousMetaDir;
        Config.plugin_dir = previousPluginDir;
        Config.metadata_journal_ignore_replay_failure = previousIgnore;
        Config.metadata_enable_recovery_mode = previousRecovery;
        Config.metadata_journal_skip_bad_journal_ids = previousSkipIds;
    }

    static Stream<Arguments> replayCases() {
        return Stream.of(PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY).flatMap(kind ->
                Stream.of(OperationType.OP_MODIFY_VIEW_DEF, OperationType.OP_ADD_FUNCTION_V2,
                        OperationType.OP_CREATE_TABLE_V2).flatMap(op ->
                        Stream.of(true, false).map(ignore -> Arguments.of(kind, op, ignore))));
    }

    @ParameterizedTest
    @MethodSource("replayCases")
    void testMalformedDescriptorHonorsOpcodePolicy(PrimitiveType kind, short op, boolean ignore) throws Exception {
        Config.metadata_journal_ignore_replay_failure = ignore;
        checkReplay(kind, op, ignore && op != OperationType.OP_CREATE_TABLE_V2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExplicitSkipIdOverridesNonIgnorableCreate(boolean matchingId) throws Exception {
        Config.metadata_journal_ignore_replay_failure = false;
        Config.metadata_journal_skip_bad_journal_ids = Long.toString(matchingId ? BAD_JOURNAL_ID : NEXT_JOURNAL_ID);
        checkReplay(PrimitiveType.GEOMETRY, OperationType.OP_CREATE_TABLE_V2, matchingId);
    }

    @Test
    void testRecoveryModeOverridesNonIgnorableCreate() throws Exception {
        Config.metadata_journal_ignore_replay_failure = false;
        Config.metadata_enable_recovery_mode = true;
        checkReplay(PrimitiveType.GEOGRAPHY, OperationType.OP_CREATE_TABLE_V2, true);
    }

    private void checkReplay(PrimitiveType kind, short op, boolean skipped) throws Exception {
        GeoTypeDescriptor descriptor = kind == PrimitiveType.GEOMETRY ? GeoTypeDescriptor.geometry("EPSG:3857")
                : new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                        GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                        "OGC:CRS84", 4326);
        ScalarType type = new GeoType(kind, descriptor);
        Database db = new Database(10001, "geo_replay");
        state.getLocalMetastore().unprotectCreateDb(db);
        String constructor = kind == PrimitiveType.GEOMETRY
                ? "ST_GeomFromText(wkt, 'EPSG:3857')" : "ST_GeogFromText(wkt)";
        String originalSql = "SELECT " + constructor.replace("wkt", "'POINT (1 2)'") + " AS g";
        View view = new View(10002, "existing", List.of(new Column("g", type, true)));
        view.setInlineViewDefWithSqlMode(originalSql, 0);
        view.setOriginalViewDef(originalSql);
        view.setComment("old view comment");
        view.setSecurity(true);
        db.registerTableUnlocked(view);
        SqlFunction function = new SqlFunction(new FunctionName(db.getFullName(), "geo_fn"),
                new Type[] {VarcharType.VARCHAR}, type, new String[] {"wkt"}, constructor);
        Database.replayCreateFunctionLog(function);
        String viewBefore = GsonUtils.GSON.toJson(view);
        String functionBefore = GsonUtils.GSON.toJson(function);

        JsonObject bad = corruptRecord(op, db, view, function, type);
        View nextView = new View(10004, "following", List.of(new Column("n", IntegerType.INT, false)));
        nextView.setInlineViewDefWithSqlMode("SELECT 7 AS n", 0);
        Map<Long, byte[]> records = Map.of(BAD_JOURNAL_ID, record(op, bad), NEXT_JOURNAL_ID,
                record(OperationType.OP_CREATE_TABLE_V2,
                        GsonUtils.GSON.toJsonTree(new CreateTableInfo(db.getFullName(), nextView, null)).getAsJsonObject()));
        List<Long> reads = new ArrayList<>();
        CloseSafeDatabase journalDb = mock(CloseSafeDatabase.class);
        when(journalDb.get(isNull(), any(DatabaseEntry.class), any(DatabaseEntry.class), eq(LockMode.READ_COMMITTED)))
                .thenAnswer(invocation -> {
                    DatabaseEntry key = invocation.getArgument(1);
                    long id = TupleBinding.getPrimitiveBinding(Long.class).entryToObject(key);
                    reads.add(id);
                    DatabaseEntry data = invocation.getArgument(2);
                    data.setData(records.get(id));
                    return OperationStatus.SUCCESS;
                });
        BDBEnvironment environment = mock(BDBEnvironment.class);
        when(environment.getDatabaseNamesWithPrefix("")).thenReturn(List.of(BAD_JOURNAL_ID));
        when(environment.openDatabase(Long.toString(BAD_JOURNAL_ID))).thenReturn(journalDb);
        BDBJournalCursor cursor = spy(BDBJournalCursor.getJournalCursor(environment, BAD_JOURNAL_ID, NEXT_JOURNAL_ID));
        try {
            if (skipped) {
                state.replayJournalInner(cursor, false);
                assertEquals(NEXT_JOURNAL_ID, state.getReplayedJournalId());
                assertEquals(List.of(BAD_JOURNAL_ID, NEXT_JOURNAL_ID), reads);
                assertNotNull(db.getTable("following"));
                assertEquals(IntegerType.INT, db.getTable("following").getColumn("n").getType());
            } else {
                JournalException error = assertThrows(JournalException.class, () -> state.replayJournalInner(cursor, false));
                assertEquals(op, error.getOpCode());
                assertInstanceOf(JsonParseException.class, error.getCause());
                assertEquals(BAD_JOURNAL_ID - 1, state.getReplayedJournalId());
                assertEquals(List.of(BAD_JOURNAL_ID), reads);
                assertNull(db.getTable("following"));
            }
            verify(cursor, times(skipped ? 1 : 0)).skipNext();
            assertNull(db.getTable("bad_create"));
            assertSame(view, db.getTable("existing"));
            assertSame(function, db.getFunctions().get(0));
            assertEquals(viewBefore, GsonUtils.GSON.toJson(view));
            assertEquals(functionBefore, GsonUtils.GSON.toJson(db.getFunctions().get(0)));
        } finally {
            cursor.close();
        }
    }

    private JsonObject corruptRecord(short op, Database db, View view, SqlFunction function, ScalarType type) {
        JsonObject payload;
        JsonObject geoType;
        String replacementSql = function.getSql().replace("wkt", "'POINT (9 9)'");
        if (op == OperationType.OP_MODIFY_VIEW_DEF) {
            AlterViewInfo info = new AlterViewInfo(db.getId(), view.getId(), "SELECT " + replacementSql + " AS changed",
                    List.of(new Column("changed", type, false)), 1, "new view comment",
                    "SELECT " + replacementSql + " AS changed");
            info.setUpdateSecurity(true);
            info.setSecurity(false);
            payload = GsonUtils.GSON.toJsonTree(info).getAsJsonObject();
            geoType = payload.getAsJsonArray("newFullSchema").get(0).getAsJsonObject().getAsJsonObject("type");
        } else if (op == OperationType.OP_ADD_FUNCTION_V2) {
            SqlFunction replacement = new SqlFunction(function.getFunctionName(), function.getArgs(), type,
                    function.getArgNames(), replacementSql);
            payload = GsonUtils.GSON.toJsonTree(replacement).getAsJsonObject();
            geoType = payload.getAsJsonObject("retType");
        } else {
            View created = new View(10003, "bad_create", List.of(new Column("g", type, true)));
            payload = GsonUtils.GSON.toJsonTree(new CreateTableInfo(db.getFullName(), created, null)).getAsJsonObject();
            geoType = payload.getAsJsonObject("t").getAsJsonArray("fullSchema").get(0)
                    .getAsJsonObject().getAsJsonObject("type");
        }
        assertEquals("GeoType", geoType.get("clazz").getAsString());
        geoType.add("geo", new JsonArray());
        return payload;
    }

    private static byte[] record(short op, JsonObject payload) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeShort(op);
            Text.writeString(out, payload.toString());
        }
        return bytes.toByteArray();
    }
}
