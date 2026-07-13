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

package com.starrocks.http.rest.context;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins the per-entity edge extraction that {@link ContextBulkImportAction} feeds into
 * {@code ContextWriteExecutor.upsertBatch}. Before this wiring, bulk-import passed
 * {@code perEntityEdges=null} and silently ignored every entity's {@code edges} array — graph
 * import via bulk-import was impossible. The list must be index-aligned with the entities (so
 * upsertBatch attaches each entity's edges to the right row) and {@code null} when no entity
 * carries edges (the cheap "no edges anywhere" fast path).
 */
public class ContextBulkImportActionEdgesTest {

    private static Map<String, Object> entity(String key, Object edges) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("entity_key", key);
        e.put("entity_type", "page");
        e.put("content", "body of " + key);
        if (edges != null) {
            e.put("edges", edges);
        }
        return e;
    }

    private static Map<String, Object> edge(String field, Object value) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(field, value);
        return m;
    }

    @Test
    public void testNoEntityHasEdgesReturnsNull() {
        List<Map<String, Object>> entities = Arrays.asList(
                entity("A", null), entity("B", null));
        Assertions.assertNull(ContextBulkImportAction.extractPerEntityEdges(entities),
                "no edges anywhere must collapse to null so upsertBatch takes the fast path");
    }

    @Test
    public void testForwardRefEdgeIsParsedAndIndexAligned() {
        // A (index 0) references B by key; B (index 1) has no edges. The forward reference (B may
        // not exist yet) is preserved as a StringLiteral key — upsertBatch stores it lazily.
        List<Map<String, Object>> entities = Arrays.asList(
                entity("A", Arrays.asList(edge("dst_entity_key", "B"))),
                entity("B", null));
        List<List<Expr>> per = ContextBulkImportAction.extractPerEntityEdges(entities);
        Assertions.assertNotNull(per);
        Assertions.assertEquals(2, per.size());
        // index 0 -> A's edge to B
        Assertions.assertNotNull(per.get(0));
        Assertions.assertEquals(1, per.get(0).size());
        Assertions.assertTrue(per.get(0).get(0) instanceof StringLiteral);
        Assertions.assertEquals("B", ((StringLiteral) per.get(0).get(0)).getValue());
        // index 1 -> B has no edges
        Assertions.assertNull(per.get(1));
    }

    @Test
    public void testNumericAndKeyEdgesBothParse() {
        List<Map<String, Object>> entities = Arrays.asList(
                entity("A", Arrays.asList(edge("dst_entity_id", 42L), edge("dst_entity_key", "C"))));
        List<List<Expr>> per = ContextBulkImportAction.extractPerEntityEdges(entities);
        Assertions.assertNotNull(per);
        List<Expr> a = per.get(0);
        Assertions.assertEquals(2, a.size());
        Assertions.assertTrue(a.get(0) instanceof IntLiteral);
        Assertions.assertEquals(42L, ((IntLiteral) a.get(0)).getLongValue());
        Assertions.assertTrue(a.get(1) instanceof StringLiteral);
        Assertions.assertEquals("C", ((StringLiteral) a.get(1)).getValue());
    }

    @Test
    public void testEmptyEdgesArrayDoesNotCountAsEdges() {
        List<Map<String, Object>> entities = Arrays.asList(entity("A", new ArrayList<>()));
        Assertions.assertNull(ContextBulkImportAction.extractPerEntityEdges(entities));
    }
}
