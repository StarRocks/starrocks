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

package com.starrocks.context.retrieval;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests the input-validation and short-circuit paths of {@link ReferenceExpander} that do not
 * require hitting the internal tables. The happy path (actual SQL probes against
 * {@code context_entity_refs}) is covered by the REST-level integration test once the internal DB
 * is materialized in a UT frame.
 */
public class ReferenceExpanderFrontierTest {

    @Test
    public void testEmptySeedsReturnsEmpty() {
        ReferenceExpander expander = new ReferenceExpander();
        ReferenceExpander.Request req = new ReferenceExpander.Request();
        req.seeds = null;
        ReferenceExpander.Result result = expander.expand(req);
        Assertions.assertTrue(result.rows.isEmpty());
        Assertions.assertFalse(result.truncated);

        req.seeds = java.util.Collections.emptyList();
        result = expander.expand(req);
        Assertions.assertTrue(result.rows.isEmpty());
    }

    @Test
    public void testSeedsEchoedAsHopZeroWhenDepthIsZero() {
        ReferenceExpander expander = new ReferenceExpander();
        ReferenceExpander.Request req = new ReferenceExpander.Request();
        req.seeds = Arrays.asList(10L, 20L, 30L);
        req.depth = 0;
        ReferenceExpander.Result result = expander.expand(req);
        Assertions.assertEquals(3, result.rows.size());
        for (ReferenceExpander.ExpansionRow row : result.rows) {
            Assertions.assertEquals(0, row.hop);
            Assertions.assertEquals(1.0, row.pathScore, 1e-9);
        }
    }

    @Test
    public void testDirectionEnumValues() {
        // Defensive: ensure the three documented directions are all enum values; this catches the
        // class of regressions where a rename would silently drop a direction.
        Assertions.assertEquals(3, ReferenceExpander.Direction.values().length);
        Assertions.assertNotNull(ReferenceExpander.Direction.valueOf("FORWARD"));
        Assertions.assertNotNull(ReferenceExpander.Direction.valueOf("BACKWARD"));
        Assertions.assertNotNull(ReferenceExpander.Direction.valueOf("BOTH"));
    }
}
