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
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
