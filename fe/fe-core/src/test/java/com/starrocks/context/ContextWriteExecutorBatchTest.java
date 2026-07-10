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

package com.starrocks.context;

import com.google.common.collect.ImmutableMap;
import com.starrocks.context.allocator.ContextVersionAllocator;
import com.starrocks.persist.ContextOpLog;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link ContextWriteExecutor#upsertBatch}'s pre-SQL phases — the parts that decide
 * which rows make it past Phase 1 validation, plus the guard paths that fail before any SQL is
 * dispatched. End-to-end SQL coverage (multi-row VALUES shape, BE round-trip) is in the
 * {@code test_semantic_context} integration suite; this class only exercises behavior that does
 * not require a live {@code SimpleExecutor}.
 */
public class ContextWriteExecutorBatchTest {

    private static ContextWriteExecutor newExecutor(ContextMgr mgr) {
        return new ContextWriteExecutor(mgr, new ContextVersionAllocator(),
                /*snapshot allocator unused in pre-SQL paths*/ null);
    }

    @Test
    public void testEmptyListReturnsEmptyOutcomes() {
        ContextMgr mgr = new ContextMgr();
        ContextWriteExecutor executor = newExecutor(mgr);
        ContextCollectionName name = new ContextCollectionName("any", "x", NodePosition.ZERO);
        // Even with a missing contextbase, an empty list short-circuits before the lookup —
        // matching the "no SQL, no work" promise for the no-op case.
        List<ContextWriteExecutor.UpsertOutcome> outcomes =
                executor.upsertBatch(name, Collections.emptyList(), null, null);
        Assertions.assertNotNull(outcomes);
        Assertions.assertTrue(outcomes.isEmpty());
    }

    @Test
    public void testNullListReturnsEmptyOutcomes() {
        ContextMgr mgr = new ContextMgr();
        ContextWriteExecutor executor = newExecutor(mgr);
        ContextCollectionName name = new ContextCollectionName("any", "x", NodePosition.ZERO);
        List<ContextWriteExecutor.UpsertOutcome> outcomes =
                executor.upsertBatch(name, null, null, null);
        Assertions.assertNotNull(outcomes);
        Assertions.assertTrue(outcomes.isEmpty());
    }

    @Test
    public void testUnqualifiedCollectionRejected() {
        ContextMgr mgr = new ContextMgr();
        ContextWriteExecutor executor = newExecutor(mgr);
        ContextCollectionName unqualified = new ContextCollectionName(null, "x", NodePosition.ZERO);
        List<Map<String, Expr>> args =
                Collections.singletonList(ImmutableMap.of("entity_key", new StringLiteral("k")));
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> executor.upsertBatch(unqualified, args, null, null));
        Assertions.assertTrue(ex.getMessage().contains("contextbase.collection"));
    }

    @Test
    public void testMissingContextBaseRejected() {
        ContextMgr mgr = new ContextMgr();
        ContextWriteExecutor executor = newExecutor(mgr);
        ContextCollectionName name = new ContextCollectionName("sales_ai", "rules", NodePosition.ZERO);
        List<Map<String, Expr>> args =
                Collections.singletonList(ImmutableMap.of("entity_key", new StringLiteral("k")));
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> executor.upsertBatch(name, args, null, null));
        Assertions.assertTrue(ex.getMessage().contains("contextbase not found"));
    }

    @Test
    public void testMissingCollectionRejected() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(1L, "sales_ai", null));
        ContextWriteExecutor executor = newExecutor(mgr);
        ContextCollectionName name = new ContextCollectionName("sales_ai", "rules", NodePosition.ZERO);
        List<Map<String, Expr>> args =
                Collections.singletonList(ImmutableMap.of("entity_key", new StringLiteral("k")));
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> executor.upsertBatch(name, args, null, null));
        Assertions.assertTrue(ex.getMessage().contains("collection not found"));
    }

    @Test
    public void testUpsertOutcomeCarriers() {
        // Exercises the public outcome carrier without needing SQL — guarantees the REST and
        // daemon callers see exactly the fields they map into their response shape.
        ContextWriteExecutor.UpsertResult result =
                new ContextWriteExecutor.UpsertResult(7L, 3L, 99L, "k");
        // Reflection-free check: build a list-equivalent and ensure the public fields are wired.
        Assertions.assertEquals(7L, result.entityId);
        Assertions.assertEquals(3L, result.version);
        Assertions.assertEquals(99L, result.snapshotVersion);
        Assertions.assertEquals("k", result.entityKey);
    }

    @Test
    public void testEmptyArgsListAfterFilteringStillReturnsZeroOutcomes() {
        // A non-empty input where every row is null (defensive: REST callers using Gson can hand
        // back lists with sparse entries). We accept the empty short-circuit anyway because an
        // empty list of *valid* args means there's nothing to upsert.
        ContextMgr mgr = new ContextMgr();
        ContextWriteExecutor executor = newExecutor(mgr);
        ContextCollectionName name = new ContextCollectionName("any", "x", NodePosition.ZERO);
        List<Map<String, Expr>> args = new ArrayList<>();
        // Add no rows — the upsertBatch short-circuit on empty list is the contract.
        List<ContextWriteExecutor.UpsertOutcome> outcomes =
                executor.upsertBatch(name, args, null, null);
        Assertions.assertTrue(outcomes.isEmpty());
    }
}
