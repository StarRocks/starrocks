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

import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the decision matrix of {@link SchemaChangeIndexFastPathClassifier}.
 * Pure unit-test using Mockito for OlapTable / Index to avoid pulling in the
 * full catalog. The classifier is intentionally stateless and small, so
 * lightweight mocking covers it well.
 */
public class SchemaChangeIndexFastPathClassifierTest {

    private static OlapTable lakeTable() {
        OlapTable t = mock(OlapTable.class);
        when(t.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        return t;
    }

    private static OlapTable nonLakeTable() {
        OlapTable t = mock(OlapTable.class);
        when(t.isCloudNativeTableOrMaterializedView()).thenReturn(false);
        return t;
    }

    private static CreateIndexClause createIndex(IndexDef.IndexType type) {
        IndexDef def = mock(IndexDef.class);
        when(def.getIndexType()).thenReturn(type);
        CreateIndexClause c = mock(CreateIndexClause.class);
        when(c.getIndexDef()).thenReturn(def);
        return c;
    }

    private static DropIndexClause dropIndex(String name) {
        DropIndexClause c = mock(DropIndexClause.class);
        when(c.getIndexName()).thenReturn(name);
        return c;
    }

    @Test
    public void testAddIndexFastPath_NotLakeTable() {
        OlapTable t = nonLakeTable();
        List<AlterClause> clauses = Collections.singletonList(createIndex(IndexDef.IndexType.BITMAP));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, clauses));
    }

    @Test
    public void testAddIndexFastPath_NullOrEmpty() {
        OlapTable t = lakeTable();
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, null));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, Collections.emptyList()));
    }

    @Test
    public void testAddIndexFastPath_BitmapAccepted() {
        OlapTable t = lakeTable();
        List<AlterClause> clauses = Collections.singletonList(createIndex(IndexDef.IndexType.BITMAP));
        assertTrue(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, clauses));
    }

    @Test
    public void testAddIndexFastPath_NgrambfAccepted() {
        OlapTable t = lakeTable();
        List<AlterClause> clauses = Collections.singletonList(createIndex(IndexDef.IndexType.NGRAMBF));
        assertTrue(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, clauses));
    }

    @Test
    public void testAddIndexFastPath_GinAccepted() {
        // Classifier accepts GIN at the type-eligibility level; the BE-side
        // builder still rejects GIN (NotSupported) so the actual run will
        // fall back. That's intentional: keeps the FE classifier table-type
        // agnostic and lets BE/FE be deployed independently.
        OlapTable t = lakeTable();
        List<AlterClause> clauses = Collections.singletonList(createIndex(IndexDef.IndexType.GIN));
        assertTrue(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, clauses));
    }

    @Test
    public void testAddIndexFastPath_VectorRejected() {
        OlapTable t = lakeTable();
        List<AlterClause> clauses = Collections.singletonList(createIndex(IndexDef.IndexType.VECTOR));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, clauses));
    }

    @Test
    public void testAddIndexFastPath_MixedClausesRejected() {
        // Mixing a CreateIndexClause with any other clause forces the
        // regular path (the fast path's contract is "only add-index").
        OlapTable t = lakeTable();
        List<AlterClause> clauses = List.of(createIndex(IndexDef.IndexType.BITMAP), mock(ModifyTablePropertiesClause.class));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, clauses));
    }

    @Test
    public void testDropIndexFastPath_NotLakeTable() {
        OlapTable t = nonLakeTable();
        Index ix = mock(Index.class);
        when(ix.getIndexName()).thenReturn("x");
        when(ix.getIndexType()).thenReturn(IndexDef.IndexType.BITMAP);
        when(t.getIndexes()).thenReturn(Collections.singletonList(ix));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t,
                Collections.singletonList(dropIndex("x"))));
    }

    @Test
    public void testDropIndexFastPath_BitmapAccepted() {
        OlapTable t = lakeTable();
        Index ix = mock(Index.class);
        when(ix.getIndexName()).thenReturn("idx_a");
        when(ix.getIndexType()).thenReturn(IndexDef.IndexType.BITMAP);
        when(t.getIndexes()).thenReturn(Collections.singletonList(ix));
        assertTrue(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t,
                Collections.singletonList(dropIndex("idx_a"))));
    }

    @Test
    public void testDropIndexFastPath_VectorRejected() {
        OlapTable t = lakeTable();
        Index ix = mock(Index.class);
        when(ix.getIndexName()).thenReturn("idx_v");
        when(ix.getIndexType()).thenReturn(IndexDef.IndexType.VECTOR);
        when(t.getIndexes()).thenReturn(Collections.singletonList(ix));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t,
                Collections.singletonList(dropIndex("idx_v"))));
    }

    @Test
    public void testDropIndexFastPath_UnknownNameRejected() {
        OlapTable t = lakeTable();
        when(t.getIndexes()).thenReturn(Collections.emptyList());
        // Analyzer should have failed earlier; classifier stays defensive.
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t,
                Collections.singletonList(dropIndex("missing"))));
    }

    @Test
    public void testIsSupportedIndexType() {
        assertTrue(SchemaChangeIndexFastPathClassifier.isSupportedIndexType(IndexDef.IndexType.BITMAP));
        assertTrue(SchemaChangeIndexFastPathClassifier.isSupportedIndexType(IndexDef.IndexType.NGRAMBF));
        assertTrue(SchemaChangeIndexFastPathClassifier.isSupportedIndexType(IndexDef.IndexType.GIN));
        assertFalse(SchemaChangeIndexFastPathClassifier.isSupportedIndexType(IndexDef.IndexType.VECTOR));
        assertFalse(SchemaChangeIndexFastPathClassifier.isSupportedIndexType(null));
    }

    // -----------------------------------------------------------------
    // Drop-index branches: empty list, null clauses, missing-Index branch
    // for findIndexByName.
    // -----------------------------------------------------------------

    @Test
    public void testDropIndexFastPath_NullOrEmpty() {
        OlapTable t = lakeTable();
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t, null));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t, Collections.emptyList()));
    }

    @Test
    public void testDropIndexFastPath_NonDropClauseRejected() {
        // A clause list that isn't pure DropIndexClause must reject (mixed alter).
        OlapTable t = lakeTable();
        List<AlterClause> mixed = List.of(mock(ModifyTablePropertiesClause.class));
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t, mixed));
    }

    @Test
    public void testDropIndexFastPath_NullIndexesListReturnsFalse() {
        OlapTable t = lakeTable();
        when(t.getIndexes()).thenReturn(null);
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t,
                Collections.singletonList(dropIndex("anything"))));
    }

    @Test
    public void testAddIndexFastPath_NullIndexDefRejected() {
        OlapTable t = lakeTable();
        CreateIndexClause c = mock(CreateIndexClause.class);
        when(c.getIndexDef()).thenReturn(null);
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, List.of(c)));
    }

    @Test
    public void testAddIndexFastPath_ConfigDisabled() {
        boolean prev = Config.enable_lake_add_index_fast_path;
        Config.enable_lake_add_index_fast_path = false;
        try {
            OlapTable t = lakeTable();
            List<AlterClause> clauses = Collections.singletonList(createIndex(IndexDef.IndexType.BITMAP));
            assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(t, clauses));
        } finally {
            Config.enable_lake_add_index_fast_path = prev;
        }
    }

    @Test
    public void testDropIndexFastPath_ConfigDisabled() {
        boolean prev = Config.enable_lake_add_index_fast_path;
        Config.enable_lake_add_index_fast_path = false;
        try {
            OlapTable t = lakeTable();
            assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(t,
                    Collections.singletonList(dropIndex("idx"))));
        } finally {
            Config.enable_lake_add_index_fast_path = prev;
        }
    }

    @Test
    public void testAddIndexFastPath_NullTableRejected() {
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath(null,
                Collections.singletonList(createIndex(IndexDef.IndexType.BITMAP))));
    }

    @Test
    public void testDropIndexFastPath_NullTableRejected() {
        assertFalse(SchemaChangeIndexFastPathClassifier.shouldUseDropIndexFastPath(null,
                Collections.singletonList(dropIndex("idx"))));
    }

    // -----------------------------------------------------------------
    // BloomFilterDelta accessors / properties.
    // -----------------------------------------------------------------

    @Test
    public void testBfDelta_PureAddPureDropPredicates() {
        // Drive every branch of BloomFilterDelta accessors via the public
        // classifyBloomFilterChange that constructs them.
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(Collections.emptySet());
        SchemaChangeIndexFastPathClassifier.BloomFilterDelta added =
                SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t,
                        List.of(bfPropClause(Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1"))));
        assertNotNull(added);
        assertTrue(added.isPureAdd());
        assertFalse(added.isPureDrop());
        assertEquals(java.util.Set.of("c1"), added.added);
        assertEquals(Collections.emptySet(), added.dropped);

        OlapTable t2 = lakeTable();
        when(t2.getBfFpp()).thenReturn(0.05);
        when(t2.getBfColumnNames()).thenReturn(java.util.Set.of("c1"));
        SchemaChangeIndexFastPathClassifier.BloomFilterDelta dropped =
                SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t2,
                        List.of(bfPropClause(Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, ""))));
        assertNotNull(dropped);
        assertFalse(dropped.isPureAdd());
        assertTrue(dropped.isPureDrop());
        assertEquals(java.util.Set.of("c1"), dropped.dropped);
    }

    // -----------------------------------------------------------------
    // classifyBloomFilterChange: gating branches.
    // -----------------------------------------------------------------

    @Test
    public void testBfClassify_NotLakeRejected() {
        OlapTable t = nonLakeTable();
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t,
                List.of(bfPropClause(Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1")))));
    }

    @Test
    public void testBfClassify_NullTableRejected() {
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(null,
                List.of(bfPropClause(Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1")))));
    }

    @Test
    public void testBfClassify_NullOrEmptyClauses() {
        OlapTable t = lakeTable();
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, null));
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, Collections.emptyList()));
    }

    @Test
    public void testBfClassify_MultipleClausesRejected() {
        OlapTable t = lakeTable();
        // Size != 1 returns null (only support a single property change at a time).
        List<AlterClause> two = List.of(
                bfPropClause(Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1")),
                bfPropClause(Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c2")));
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, two));
    }

    @Test
    public void testBfClassify_NonPropertyClauseRejected() {
        OlapTable t = lakeTable();
        List<AlterClause> clauses = List.of(createIndex(IndexDef.IndexType.BITMAP));
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, clauses));
    }

    @Test
    public void testBfClassify_NullOrEmptyPropertyMapRejected() {
        OlapTable t = lakeTable();
        ModifyTablePropertiesClause emptyMap = mock(ModifyTablePropertiesClause.class);
        when(emptyMap.getProperties()).thenReturn(new HashMap<>());
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(emptyMap)));

        ModifyTablePropertiesClause nullMap = mock(ModifyTablePropertiesClause.class);
        when(nullMap.getProperties()).thenReturn(null);
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(nullMap)));
    }

    @Test
    public void testBfClassify_UnrelatedPropertyRejected() {
        OlapTable t = lakeTable();
        // A property other than bf_columns / bf_fpp forces the legacy path.
        Map<String, String> props = new HashMap<>();
        props.put("replication_num", "3");
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props))));
    }

    @Test
    public void testBfClassify_FppChangeRejected() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(Collections.emptySet());
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1");
        // Different fpp → reject.
        props.put(PropertyAnalyzer.PROPERTIES_BF_FPP, "0.01");
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props))));
    }

    @Test
    public void testBfClassify_MalformedFppRejected() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(Collections.emptySet());
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1");
        props.put(PropertyAnalyzer.PROPERTIES_BF_FPP, "not-a-number");
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props))));
    }

    @Test
    public void testBfClassify_FppMatchAccepted() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(Collections.emptySet());
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1");
        // Echoed fpp matching the current value is fine (within 1e-9).
        props.put(PropertyAnalyzer.PROPERTIES_BF_FPP, "0.05");
        SchemaChangeIndexFastPathClassifier.BloomFilterDelta delta =
                SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props)));
        assertNotNull(delta);
        assertTrue(delta.isPureAdd());
    }

    @Test
    public void testBfClassify_MissingBfColumnsKeyRejected() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        // Only fpp present, no bf_columns → reject (pure-fpp alters not eligible).
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_BF_FPP, "0.05");
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props))));
    }

    @Test
    public void testBfClassify_NoOpReturnsNull() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(java.util.Set.of("c1"));
        // Same set → null (lets legacy path raise the canonical "no change" error).
        Map<String, String> props = Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1");
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props))));
    }

    @Test
    public void testBfClassify_MixedAddAndDropRejected() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(java.util.Set.of("c1"));
        // Adds c2, drops c1 → mixed → reject.
        Map<String, String> props = Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c2");
        assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props))));
    }

    @Test
    public void testBfClassify_MultiAddAccepted() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(Collections.emptySet());
        Map<String, String> props = Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1, c2 ,C3");
        SchemaChangeIndexFastPathClassifier.BloomFilterDelta delta =
                SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props)));
        assertNotNull(delta);
        assertTrue(delta.isPureAdd());
        // Names normalized to lowercase, trimmed.
        assertEquals(java.util.Set.of("c1", "c2", "c3"), delta.added);
    }

    @Test
    public void testBfClassify_NullBfColumnNamesTreatedAsEmpty() {
        OlapTable t = lakeTable();
        when(t.getBfFpp()).thenReturn(0.05);
        when(t.getBfColumnNames()).thenReturn(null);
        Map<String, String> props = Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1");
        SchemaChangeIndexFastPathClassifier.BloomFilterDelta delta =
                SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props)));
        assertNotNull(delta);
        assertTrue(delta.isPureAdd());
    }

    @Test
    public void testBfClassify_ConfigDisabled() {
        boolean prev = Config.enable_lake_add_index_fast_path;
        Config.enable_lake_add_index_fast_path = false;
        try {
            OlapTable t = lakeTable();
            when(t.getBfFpp()).thenReturn(0.05);
            when(t.getBfColumnNames()).thenReturn(Collections.emptySet());
            Map<String, String> props = Map.of(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "c1");
            assertNull(SchemaChangeIndexFastPathClassifier.classifyBloomFilterChange(t, List.of(bfPropClause(props))));
        } finally {
            Config.enable_lake_add_index_fast_path = prev;
        }
    }

    private static ModifyTablePropertiesClause bfPropClause(Map<String, String> props) {
        ModifyTablePropertiesClause c = mock(ModifyTablePropertiesClause.class);
        when(c.getProperties()).thenReturn(new HashMap<>(props));
        return c;
    }
}
