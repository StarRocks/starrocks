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

import com.google.gson.JsonArray;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards two invariants of the graph_expand hop SQL:
 * <ol>
 *   <li>the per-hop frontier cap applies to the INITIAL (seed) frontier, so the first SQL hop
 *       cannot carry an unbounded IN-list;</li>
 *   <li>the active-ref MAX(snapshot_version) subquery is resolved per ordinal <em>before</em> the
 *       backward destination filter is applied — otherwise a REFERENCE_RESYNC that moved an edge
 *       would let a stale layer win.</li>
 * </ol>
 */
public class ReferenceExpanderTest {

    private static final class CapturingExpander extends ReferenceExpander {
        final List<String> sqls = new ArrayList<>();

        @Override
        JsonArray runQuery(String sql) {
            sqls.add(sql);
            return new JsonArray();
        }
    }

    private static String innerMaxSubquery(String sql) {
        int from = sql.indexOf("MAX(snapshot_version)");
        int to = sql.indexOf("GROUP BY", from);
        assertTrue(from >= 0 && to > from, "expected an active-ref MAX subquery: " + sql);
        return sql.substring(from, to);
    }

    @Test
    public void testForwardInnerBoundBySrc() {
        String sql = new ReferenceExpander().buildHopSql(
                Arrays.asList(1L, 2L), ReferenceExpander.Direction.FORWARD, null, 42L, null, null, -1L);
        // Forward frontier is on src (the GROUP BY key) → safe to bound the MAX subquery by it.
        assertTrue(innerMaxSubquery(sql).contains("src_entity_id IN (1,2)"),
                "forward inner MAX should be bounded by the src frontier: " + sql);
        assertTrue(sql.contains("r.src_entity_id IN (1,2)"), "outer forward filter: " + sql);
    }

    @Test
    public void testBackwardResolvesActiveRefBeforeDstFilter() {
        String sql = new ReferenceExpander().buildHopSql(
                Arrays.asList(9L), ReferenceExpander.Direction.BACKWARD, null, 42L, null, null, -1L);
        // The active-ref MAX must NOT be filtered by dst (that would pick a stale post-resync layer).
        assertFalse(innerMaxSubquery(sql).contains("dst_entity_id"),
                "backward inner MAX must not filter by dst: " + sql);
        // The dst frontier filter is applied by the OUTER query, on the already-resolved active rows.
        assertTrue(sql.contains("r.dst_entity_id IN (9)"), "outer backward filter: " + sql);
    }

    @Test
    public void testBackwardWithFenceInnerIsFenceOnly() {
        String sql = new ReferenceExpander().buildHopSql(
                Arrays.asList(9L), ReferenceExpander.Direction.BACKWARD, null, 42L, null, null, 7777L);
        String inner = innerMaxSubquery(sql);
        assertTrue(inner.contains("snapshot_version <= 7777"), "backward inner MAX fenced: " + sql);
        assertFalse(inner.contains("dst_entity_id"), "backward inner MAX still must not filter by dst: " + sql);
    }

    @Test
    public void testSeedFrontierCapEnforcedOnFirstHop() {
        CapturingExpander exec = new CapturingExpander();
        ReferenceExpander.Request req = new ReferenceExpander.Request();
        req.seeds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        req.maxFrontier = 2;
        req.depth = 1;
        req.contextBaseId = 42L;
        ReferenceExpander.Result result = exec.expand(req);
        assertTrue(result.truncated, "supplying more seeds than maxFrontier must mark the result truncated");
        assertFalse(exec.sqls.isEmpty(), "the first hop must issue a query");
        String firstHop = exec.sqls.get(0);
        // Only the first maxFrontier=2 seeds reach the first-hop IN-list.
        assertTrue(firstHop.contains("IN (1,2)"), "first-hop frontier capped to 2: " + firstHop);
        assertFalse(firstHop.contains("IN (1,2,3"), "over-cap seeds must not reach the first hop: " + firstHop);
    }
}
