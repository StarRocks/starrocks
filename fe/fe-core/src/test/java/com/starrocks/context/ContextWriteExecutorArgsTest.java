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
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Validates the shape of the {@link ContextWriteExecutor} wiring without hitting the internal DB.
 * Exercises the guard paths (missing contextbase / missing collection) that fail before any INSERT
 * is dispatched; the SimpleExecutor call path is covered end-to-end by the M3 integration test
 * that runs inside {@code UtFrameUtils}.
 */
public class ContextWriteExecutorArgsTest {

    @Test
    public void testUnqualifiedCollectionRejected() {
        ContextMgr mgr = new ContextMgr();
        ContextWriteExecutor executor = new ContextWriteExecutor(
                mgr, new ContextVersionAllocator(), null /* snapshot allocator unused in guard path */);
        ContextCollectionName unqualified = new ContextCollectionName(null, "pipeline_rules", NodePosition.ZERO);
        Map<String, Expr> args = ImmutableMap.of("entity_key", new StringLiteral("x"));
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> executor.upsert(unqualified, args, null));
        Assertions.assertTrue(ex.getMessage().contains("contextbase.collection"));
    }

    @Test
    public void testMissingContextBaseRejected() {
        ContextMgr mgr = new ContextMgr();
        ContextWriteExecutor executor = new ContextWriteExecutor(
                mgr, new ContextVersionAllocator(), null);
        ContextCollectionName name = new ContextCollectionName("sales_ai", "pipeline_rules", NodePosition.ZERO);
        Map<String, Expr> args = ImmutableMap.of("entity_key", new StringLiteral("x"));
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> executor.upsert(name, args, null));
        Assertions.assertTrue(ex.getMessage().contains("contextbase not found"));
    }

    @Test
    public void testMissingCollectionRejected() {
        ContextMgr mgr = new ContextMgr();
        // Populate the contextbase via the replay path to avoid triggering the edit-log write.
        mgr.replayCreateContextBase(com.starrocks.persist.ContextOpLog.forContextBase(1L, "sales_ai", null));
        ContextWriteExecutor executor = new ContextWriteExecutor(
                mgr, new ContextVersionAllocator(), null);
        ContextCollectionName name = new ContextCollectionName("sales_ai", "pipeline_rules", NodePosition.ZERO);
        Map<String, Expr> args = ImmutableMap.of("entity_key", new StringLiteral("x"));
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> executor.upsert(name, args, null));
        Assertions.assertTrue(ex.getMessage().contains("collection not found"));
    }
}
