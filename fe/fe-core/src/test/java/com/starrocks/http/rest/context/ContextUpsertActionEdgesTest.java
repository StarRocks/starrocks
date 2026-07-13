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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins the JSON-to-{@link Expr} conversion used by {@link ContextUpsertAction} when materializing
 * the {@code edges} array into the {@code EDGES (...)} clause that {@code ContextWriteExecutor}
 * consumes. Exercises the package-private {@code toEdgeExprs} helper directly.
 */
public class ContextUpsertActionEdgesTest {

    @Test
    public void testNullInputReturnsNull() {
        Assertions.assertNull(ContextUpsertAction.toEdgeExprs(null));
        Assertions.assertNull(ContextUpsertAction.toEdgeExprs(new ArrayList<>()));
    }

    @Test
    public void testEntityKeyStringMapsToStringLiteral() {
        List<Map<String, Object>> in = Arrays.asList(
                edge("dst_entity_key", "financial::client"),
                edge("dst_entity_key", "financial::district"));
        List<Expr> out = ContextUpsertAction.toEdgeExprs(in);
        Assertions.assertEquals(2, out.size());
        Assertions.assertTrue(out.get(0) instanceof StringLiteral);
        Assertions.assertEquals("financial::client", ((StringLiteral) out.get(0)).getValue());
        Assertions.assertEquals("financial::district", ((StringLiteral) out.get(1)).getValue());
    }

    @Test
    public void testEntityIdNumberMapsToIntLiteral() {
        List<Map<String, Object>> in = Arrays.asList(
                edge("dst_entity_id", 42L),
                edge("dst_entity_id", 1001));
        List<Expr> out = ContextUpsertAction.toEdgeExprs(in);
        Assertions.assertEquals(2, out.size());
        Assertions.assertTrue(out.get(0) instanceof IntLiteral);
        Assertions.assertEquals(42L, ((IntLiteral) out.get(0)).getLongValue());
        Assertions.assertEquals(1001L, ((IntLiteral) out.get(1)).getLongValue());
    }

    @Test
    public void testDstShorthandDetectsType() {
        List<Map<String, Object>> in = Arrays.asList(
                edge("dst", "financial::client"),
                edge("dst", 42));
        List<Expr> out = ContextUpsertAction.toEdgeExprs(in);
        Assertions.assertEquals(2, out.size());
        Assertions.assertTrue(out.get(0) instanceof StringLiteral);
        Assertions.assertEquals("financial::client", ((StringLiteral) out.get(0)).getValue());
        Assertions.assertTrue(out.get(1) instanceof IntLiteral);
        Assertions.assertEquals(42L, ((IntLiteral) out.get(1)).getLongValue());
    }

    @Test
    public void testMalformedEntriesDropped() {
        Map<String, Object> empty = new LinkedHashMap<>();
        Map<String, Object> noDst = new LinkedHashMap<>();
        noDst.put("ref_kind", "foreign_key");      // dst missing entirely
        Map<String, Object> blank = new LinkedHashMap<>();
        blank.put("dst_entity_key", "");
        List<Map<String, Object>> in = Arrays.asList(empty, noDst, blank, null,
                edge("dst_entity_key", "ok"));
        List<Expr> out = ContextUpsertAction.toEdgeExprs(in);
        Assertions.assertNotNull(out);
        Assertions.assertEquals(1, out.size(), "only the well-formed entry should survive");
        Assertions.assertEquals("ok", ((StringLiteral) out.get(0)).getValue());
    }

    @Test
    public void testRefKindAndLabelIgnoredButTolerated() {
        // ref_kind / ref_label are accepted by the JSON surface (forward-compat) but the writer
        // currently lands all explicit edges under ref_kind='explicit'. The presence of these
        // fields must not affect parsing.
        Map<String, Object> edge = edge("dst_entity_key", "financial::client");
        edge.put("ref_kind", "foreign_key");
        edge.put("ref_label", "account.client_id");
        List<Expr> out = ContextUpsertAction.toEdgeExprs(Arrays.asList(edge));
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("financial::client", ((StringLiteral) out.get(0)).getValue());
    }

    @Test
    public void testEntirelyMalformedListReturnsNull() {
        List<Map<String, Object>> in = Arrays.asList(new HashMap<>(), null);
        Assertions.assertNull(ContextUpsertAction.toEdgeExprs(in),
                "after all entries drop, return null so the writer skips the EDGES clause");
    }

    private static Map<String, Object> edge(String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(key, value);
        return m;
    }
}
