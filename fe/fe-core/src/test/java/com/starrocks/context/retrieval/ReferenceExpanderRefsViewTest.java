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
import java.util.Collections;
import java.util.List;

/**
 * Pins the refs-FROM-clause shape that {@link ReferenceExpander#buildHopSql} emits. The fix
 * for the REFERENCE_RESYNC history-rewrite bug:
 *
 * <ul>
 *   <li>added {@code snapshot_version} to the refs primary key so a resync appends a new
 *       (src_entity_id, src_version, ord, snapshot_version) row instead of overwriting the
 *       prior one;</li>
 *   <li>made the read path select the latest snapshot per (src_entity_id, src_version, ord)
 *       so a steady-state read sees the most recent resync;</li>
 *   <li>made the as-of read path fence the inner MAX by {@code snapshot_version <= fence} so
 *       historical reads see the historical edge set, not the latest rewrite.</li>
 * </ul>
 *
 * <p>These tests verify both shapes appear in the emitted SQL. A regression would either lose
 * the inner aggregation (returning every resync as a duplicate edge) or lose the fence
 * predicate (returning the latest edge to historical readers — the original bug).
 */
public class ReferenceExpanderRefsViewTest {

    @Test
    public void steadyStateSqlAggregatesLatestSnapshotPerEdge() {
        ReferenceExpander expander = new ReferenceExpander();
        List<Long> frontier = Arrays.asList(1L, 2L, 3L);
        String sql = expander.buildHopSql(frontier, ReferenceExpander.Direction.FORWARD,
                Collections.emptyList(), /*contextBaseId=*/null, /*collectionId=*/null,
                /*collectionIds=*/null, /*snapshotFence=*/-1L);
        Assertions.assertTrue(sql.contains("MAX(snapshot_version) AS sv"),
                "the latest-snapshot subquery is the keystone of the resync fix: " + sql);
        Assertions.assertTrue(sql.contains("GROUP BY src_entity_id, src_version, ord"),
                "must group at the edge-identity grain so a single (src,version,ord) yields one row: " + sql);
        // No fence predicate when snapshotFence < 0 — steady-state read takes the absolute MAX.
        Assertions.assertFalse(sql.contains("snapshot_version <="),
                "steady-state read must not add a snapshot-version filter: " + sql);
    }

    @Test
    public void fencedSqlBoundsLatestSnapshotByFence() {
        ReferenceExpander expander = new ReferenceExpander();
        List<Long> frontier = Arrays.asList(42L);
        long fence = 9999L;
        String sql = expander.buildHopSql(frontier, ReferenceExpander.Direction.FORWARD,
                Collections.emptyList(), /*contextBaseId=*/null, /*collectionId=*/null,
                /*collectionIds=*/null, fence);
        Assertions.assertTrue(sql.contains("MAX(snapshot_version) AS sv"),
                "fenced read also aggregates at the edge-identity grain: " + sql);
        Assertions.assertTrue(sql.contains("snapshot_version <= " + fence),
                "fenced read must restrict the inner MAX to snapshots within the fence: " + sql);
    }

    @Test
    public void scopedSqlAddsKeyFallbackForUnresolvedEdges() {
        // With a contextbase scope, unresolved forward-ref edges (dst_entity_id=0) must resolve
        // via dst_entity_key -> heads, exposed under dst_entity_id so downstream logic is unchanged.
        ReferenceExpander expander = new ReferenceExpander();
        List<Long> frontier = Arrays.asList(1L, 2L);
        String sql = expander.buildHopSql(frontier, ReferenceExpander.Direction.FORWARD,
                Collections.emptyList(), /*contextBaseId=*/7L, /*collectionId=*/null,
                /*collectionIds=*/null, /*snapshotFence=*/-1L);
        Assertions.assertTrue(sql.contains("CASE WHEN r0.dst_entity_id > 0"),
                "scoped read must compute an effective dst id with a key fallback: " + sql);
        Assertions.assertTrue(sql.contains("kh.entity_key = r0.dst_entity_key"),
                "the fallback must join heads on the stored dst_entity_key: " + sql);
        Assertions.assertTrue(sql.contains("kh.contextbase_id = 7"),
                "the key fallback must be scoped to the requested contextbase: " + sql);
        Assertions.assertTrue(sql.contains("AS dst_entity_id"),
                "the resolved id must be exposed under dst_entity_id so downstream logic is unchanged: " + sql);
    }

    @Test
    public void unscopedSqlHasNoKeyFallback() {
        // No contextbase scope -> cannot resolve a key unambiguously; only pre-resolved edges
        // traverse, and the projection stays the raw dst_entity_id (no regression).
        ReferenceExpander expander = new ReferenceExpander();
        String sql = expander.buildHopSql(Arrays.asList(1L), ReferenceExpander.Direction.FORWARD,
                Collections.emptyList(), /*contextBaseId=*/null, null, null, -1L);
        Assertions.assertFalse(sql.contains("CASE WHEN r0.dst_entity_id"), sql);
        Assertions.assertFalse(sql.contains("dst_entity_key"), sql);
    }

    @Test
    public void backwardAndBothDirectionsAlsoUseTheLatestSnapshotView() {
        ReferenceExpander expander = new ReferenceExpander();
        List<Long> frontier = Arrays.asList(1L);
        String backward = expander.buildHopSql(frontier, ReferenceExpander.Direction.BACKWARD,
                Collections.emptyList(), null, null, null, -1L);
        Assertions.assertTrue(backward.contains("MAX(snapshot_version) AS sv"), backward);
        String both = expander.buildHopSql(frontier, ReferenceExpander.Direction.BOTH,
                Collections.emptyList(), null, null, null, -1L);
        // BOTH does a UNION ALL of FORWARD + BACKWARD; both halves should each carry the view.
        int occurrences = 0;
        int from = 0;
        while ((from = both.indexOf("MAX(snapshot_version) AS sv", from)) != -1) {
            occurrences++;
            from++;
        }
        Assertions.assertEquals(2, occurrences,
                "BOTH-direction SQL must apply the latest-snapshot view in both halves: " + both);
    }
}
