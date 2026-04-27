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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.thrift.TDropIndexInfo;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Targeted unit coverage for the four private {@code tryBuildLake*} methods on
 * {@link SchemaChangeHandler} plus the small private static helpers
 * {@code toThriftIndex} / {@code toThriftIndexType}. These methods are
 * private so we invoke them via reflection.
 *
 * <p>The full classifier-driven dispatch in
 * {@link SchemaChangeHandler#analyzeAndCreateJob} requires running the FE-DDL
 * pipeline end-to-end and is exercised by separate integration tests; here we
 * focus on each fast-path builder in isolation.
 */
public class SchemaChangeHandlerLakeIndexFastPathTest {

    private static Method privateMethod(String name, Class<?>... params) throws NoSuchMethodException {
        Method m = SchemaChangeHandler.class.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    private static Object invoke(Method m, Object self, Object... args) throws Exception {
        try {
            return m.invoke(self, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }
            throw e;
        }
    }

    private static SchemaChangeHandler newHandler() {
        // SchemaChangeHandler ctor schedules a daemon thread, but we never
        // call .start(); the bare constructor is fine for invoking private
        // helpers reflectively.
        return new SchemaChangeHandler();
    }

    private static Database stubDb() {
        return new Database(2L, "db");
    }

    /** A minimal OlapTable mock that resolves column "c1" to a real BIGINT Column. */
    private static OlapTable stubLakeTable(String... columnNames) {
        OlapTable t = mock(OlapTable.class);
        when(t.getName()).thenReturn("t");
        when(t.getId()).thenReturn(3L);
        when(t.isOlapTableOrMaterializedView()).thenReturn(false);
        when(t.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(t.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        when(t.getIndexes()).thenReturn(new ArrayList<>());
        when(t.getCopiedIndexes()).thenReturn(new ArrayList<>());
        when(t.incAndGetMaxIndexId()).thenReturn(101L, 102L, 103L);
        when(t.getMaxIndexId()).thenReturn(100L);
        for (String name : columnNames) {
            Column col = new Column(name, IntegerType.BIGINT);
            when(t.getColumn(name)).thenReturn(col);
            when(t.getColumn(ColumnId.create(name))).thenReturn(col);
        }
        return t;
    }

    private static GlobalStateMgr stubGsm() {
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        when(gsm.getNextId()).thenReturn(50L, 51L, 52L);
        return gsm;
    }

    // ============================================================
    // tryBuildLakeAddIndexJob
    // ============================================================

    @Test
    public void testTryBuildLakeAddIndexJob_HappyPath() throws Exception {
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable("c1");
        IndexDef def = new IndexDef("ix_a", Collections.singletonList("c1"),
                IndexDef.IndexType.BITMAP, "", new HashMap<>());
        CreateIndexClause clause = new CreateIndexClause(def);

        Method m = privateMethod("tryBuildLakeAddIndexJob", Database.class, OlapTable.class, List.class);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> rmStatic = Mockito.mockStatic(RunMode.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(stubGsm());
            rmStatic.when(RunMode::getCurrentRunMode).thenReturn(RunMode.SHARED_DATA);
            Object result = invoke(m, handler, stubDb(), table,
                    Collections.<AlterClause>singletonList(clause));
            assertNotNull(result);
            assertTrue(result instanceof LakeTableAddIndexJob);
            LakeTableAddIndexJob job = (LakeTableAddIndexJob) result;
            assertEquals(1, job.getNewIndexes().size());
            assertEquals(1, job.getIndexesToAdd().size());
            verify(table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        }
    }

    @Test
    public void testTryBuildLakeAddIndexJob_ProcessAddIndexThrows_StateRevertsToNormal() throws Exception {
        SchemaChangeHandler handler = newHandler();
        // Column "missing" not registered → MetaUtils.getColumnIdsByColumnNames
        // throws SemanticException inside processAddIndex → tryBuild returns null.
        OlapTable table = stubLakeTable(); // no columns
        IndexDef def = new IndexDef("ix_a", Collections.singletonList("missing"),
                IndexDef.IndexType.BITMAP, "", new HashMap<>());
        CreateIndexClause clause = new CreateIndexClause(def);

        Method m = privateMethod("tryBuildLakeAddIndexJob", Database.class, OlapTable.class, List.class);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(stubGsm());
            Object result = invoke(m, handler, stubDb(), table,
                    Collections.<AlterClause>singletonList(clause));
            assertNull(result);
        }
        // The state should NOT be SCHEMA_CHANGE. Since the throw happened
        // before stateSet=true was ever flipped, no setState call should have
        // landed. (Verifies the M1 fix branch did not over-revert.)
        verify(table, never()).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
    }

    // ============================================================
    // tryBuildLakeDropIndexJob
    // ============================================================

    @Test
    public void testTryBuildLakeDropIndexJob_HappyPath() throws Exception {
        SchemaChangeHandler handler = newHandler();
        Index existing = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new HashMap<>());
        OlapTable table = stubLakeTable("c1");
        when(table.getIndexes()).thenReturn(new ArrayList<>(Collections.singletonList(existing)));

        DropIndexClause clause = new DropIndexClause("ix_a");
        Method m = privateMethod("tryBuildLakeDropIndexJob", Database.class, OlapTable.class, List.class);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(stubGsm());
            Object result = invoke(m, handler, stubDb(), table,
                    Collections.<AlterClause>singletonList(clause));
            assertNotNull(result);
            LakeTableDropIndexJob job = (LakeTableDropIndexJob) result;
            assertEquals(1, job.getDropIndexIds().size());
            assertEquals(101L, job.getDropIndexIds().get(0));
            assertEquals(1, job.getDropInfos().size());
            assertEquals(101L, job.getDropInfos().get(0).getIndex_id());
            assertEquals(TIndexType.BITMAP, job.getDropInfos().get(0).getIndex_type());
            verify(table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        }
    }

    @Test
    public void testTryBuildLakeDropIndexJob_MissingIndexReturnsNull() throws Exception {
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable("c1");
        // No indexes on table. DropIndexClause for "nope" → null result.
        DropIndexClause clause = new DropIndexClause("nope");
        Method m = privateMethod("tryBuildLakeDropIndexJob", Database.class, OlapTable.class, List.class);
        Object result = invoke(m, handler, stubDb(), table,
                Collections.<AlterClause>singletonList(clause));
        assertNull(result);
        verify(table, never()).setState(any());
    }

    @Test
    public void testTryBuildLakeDropIndexJob_MultiColumnIndexEmitsMultipleInfos() throws Exception {
        SchemaChangeHandler handler = newHandler();
        Index existing = new Index(101L, "ix_a",
                Arrays.asList(ColumnId.create("c1"), ColumnId.create("c2")),
                IndexDef.IndexType.NGRAMBF, "", new HashMap<>());
        OlapTable table = stubLakeTable("c1", "c2");
        when(table.getIndexes()).thenReturn(new ArrayList<>(Collections.singletonList(existing)));

        DropIndexClause clause = new DropIndexClause("ix_a");
        Method m = privateMethod("tryBuildLakeDropIndexJob", Database.class, OlapTable.class, List.class);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(stubGsm());
            Object result = invoke(m, handler, stubDb(), table,
                    Collections.<AlterClause>singletonList(clause));
            LakeTableDropIndexJob job = (LakeTableDropIndexJob) result;
            assertEquals(2, job.getDropInfos().size());
            // Same index id, distinct col_unique_ids.
            for (TDropIndexInfo info : job.getDropInfos()) {
                assertEquals(101L, info.getIndex_id());
                assertEquals(TIndexType.NGRAMBF, info.getIndex_type());
            }
        }
    }

    // ============================================================
    // tryBuildLakeAddBloomFilterJob
    // ============================================================

    @Test
    public void testTryBuildLakeAddBloomFilterJob_HappyPath() throws Exception {
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable("c1", "c2");
        when(table.getBfFpp()).thenReturn(0.05);

        Method m = privateMethod("tryBuildLakeAddBloomFilterJob", Database.class, OlapTable.class, Set.class);
        Set<String> added = new HashSet<>(Arrays.asList("c1", "c2"));
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(stubGsm());
            Object result = invoke(m, handler, stubDb(), table, added);
            assertNotNull(result);
            LakeTableAddIndexJob job = (LakeTableAddIndexJob) result;
            assertEquals(2, job.getAddBfColumns().size());
            assertEquals(2, job.getIndexesToAdd().size());
            for (TOlapTableIndex t : job.getIndexesToAdd()) {
                assertEquals(TIndexType.BLOOM_FILTER, t.getIndex_type());
                assertNotNull(t.getIndex_properties());
                assertTrue(t.getIndex_properties().containsKey("bloom_filter_fpp"));
            }
            verify(table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        }
    }

    @Test
    public void testTryBuildLakeAddBloomFilterJob_DefaultFppWhenZero() throws Exception {
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable("c1");
        when(table.getBfFpp()).thenReturn(0.0);

        Method m = privateMethod("tryBuildLakeAddBloomFilterJob", Database.class, OlapTable.class, Set.class);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(stubGsm());
            Object result = invoke(m, handler, stubDb(), table, Collections.singleton("c1"));
            LakeTableAddIndexJob job = (LakeTableAddIndexJob) result;
            String fpp = job.getIndexesToAdd().get(0).getIndex_properties().get("bloom_filter_fpp");
            assertEquals(Double.toString(FeConstants.DEFAULT_BLOOM_FILTER_FPP), fpp);
        }
    }

    @Test
    public void testTryBuildLakeAddBloomFilterJob_MissingColumnReturnsNull() throws Exception {
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable(); // no columns
        Method m = privateMethod("tryBuildLakeAddBloomFilterJob", Database.class, OlapTable.class, Set.class);
        Object result = invoke(m, handler, stubDb(), table, Collections.singleton("ghost"));
        assertNull(result);
        verify(table, never()).setState(any());
    }

    @Test
    public void testTryBuildLakeAddBloomFilterJob_GetNextIdThrowsLeavesStateUntouched() throws Exception {
        // Throw from getNextId() — that's BEFORE the setState(SCHEMA_CHANGE)
        // line, so the catch block runs with stateSet=false: NO setState call
        // should have fired (neither SCHEMA_CHANGE nor revert).
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable("c1");
        when(table.getBfFpp()).thenReturn(0.05);
        Method m = privateMethod("tryBuildLakeAddBloomFilterJob", Database.class, OlapTable.class, Set.class);
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        when(gsm.getNextId()).thenThrow(new RuntimeException("boom"));
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            Object result = invoke(m, handler, stubDb(), table, Collections.singleton("c1"));
            assertNull(result);
            verify(table, never()).setState(any());
        }
    }

    // ============================================================
    // tryBuildLakeDropBloomFilterJob
    // ============================================================

    @Test
    public void testTryBuildLakeDropBloomFilterJob_HappyPath() throws Exception {
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable("c1");
        Method m = privateMethod("tryBuildLakeDropBloomFilterJob", Database.class, OlapTable.class, Set.class);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(stubGsm());
            Object result = invoke(m, handler, stubDb(), table, Collections.singleton("c1"));
            assertNotNull(result);
            LakeTableDropIndexJob job = (LakeTableDropIndexJob) result;
            assertEquals(1, job.getDropBfColumns().size());
            assertEquals(1, job.getDropInfos().size());
            TDropIndexInfo info = job.getDropInfos().get(0);
            // Plain BF uses index_id=-1 sentinel.
            assertEquals(-1L, info.getIndex_id());
            assertEquals(TIndexType.BLOOM_FILTER, info.getIndex_type());
        }
    }

    @Test
    public void testTryBuildLakeDropBloomFilterJob_MissingColumnReturnsNull() throws Exception {
        SchemaChangeHandler handler = newHandler();
        OlapTable table = stubLakeTable(); // no columns
        Method m = privateMethod("tryBuildLakeDropBloomFilterJob", Database.class, OlapTable.class, Set.class);
        Object result = invoke(m, handler, stubDb(), table, Collections.singleton("ghost"));
        assertNull(result);
        verify(table, never()).setState(any());
    }

    // ============================================================
    // toThriftIndex / toThriftIndexType
    // ============================================================

    @Test
    public void testToThriftIndex_BitmapMapping() throws Exception {
        Method m = SchemaChangeHandler.class.getDeclaredMethod("toThriftIndex", Index.class, OlapTable.class);
        m.setAccessible(true);
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new HashMap<>());
        OlapTable table = stubLakeTable("c1");
        TOlapTableIndex t = (TOlapTableIndex) m.invoke(null, ix, table);
        assertEquals("ix_a", t.getIndex_name());
        assertEquals(101L, t.getIndex_id());
        assertEquals(TIndexType.BITMAP, t.getIndex_type());
        assertEquals(Collections.singletonList("c1"), t.getColumns());
    }

    @Test
    public void testToThriftIndex_NgrambfMapping() throws Exception {
        Method m = SchemaChangeHandler.class.getDeclaredMethod("toThriftIndex", Index.class, OlapTable.class);
        m.setAccessible(true);
        Index ix = new Index(-1L, "ng", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.NGRAMBF, "", new HashMap<>());
        OlapTable table = stubLakeTable("c1");
        TOlapTableIndex t = (TOlapTableIndex) m.invoke(null, ix, table);
        assertEquals(TIndexType.NGRAMBF, t.getIndex_type());
    }

    @Test
    public void testToThriftIndex_GinMapping() throws Exception {
        Method m = SchemaChangeHandler.class.getDeclaredMethod("toThriftIndex", Index.class, OlapTable.class);
        m.setAccessible(true);
        Index ix = new Index(101L, "g1", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.GIN, "", new HashMap<>());
        OlapTable table = stubLakeTable("c1");
        TOlapTableIndex t = (TOlapTableIndex) m.invoke(null, ix, table);
        assertEquals(TIndexType.GIN, t.getIndex_type());
    }

    @Test
    public void testToThriftIndex_VectorMapping() throws Exception {
        Method m = SchemaChangeHandler.class.getDeclaredMethod("toThriftIndex", Index.class, OlapTable.class);
        m.setAccessible(true);
        Index ix = new Index(101L, "v1", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.VECTOR, "", new HashMap<>());
        OlapTable table = stubLakeTable("c1");
        TOlapTableIndex t = (TOlapTableIndex) m.invoke(null, ix, table);
        assertEquals(TIndexType.VECTOR, t.getIndex_type());
    }

    @Test
    public void testToThriftIndex_UnknownColumnFallsBackToColumnId() throws Exception {
        Method m = SchemaChangeHandler.class.getDeclaredMethod("toThriftIndex", Index.class, OlapTable.class);
        m.setAccessible(true);
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("ghost")),
                IndexDef.IndexType.BITMAP, "", new HashMap<>());
        OlapTable table = stubLakeTable(); // no columns registered
        TOlapTableIndex t = (TOlapTableIndex) m.invoke(null, ix, table);
        assertEquals(Collections.singletonList("ghost"), t.getColumns());
    }
}
