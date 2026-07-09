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

package com.starrocks.context.retrieval.rerank.support;

import com.starrocks.context.retrieval.ReferenceExpander;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads 1-hop undirected adjacency for a fixed node set, restricted to edges whose other endpoint
 * is also in the set. Built on top of {@link ReferenceExpander#scan} so the FE-side scope filtering,
 * snapshot fence handling, and ref-kind filtering stay consistent with the rest of search().
 *
 * <p>Why undirected: foreign keys are directed on the SQL schema (account.client_id → client.id),
 * but for table retrieval the relevance signal is symmetric — if {@code account} is selected,
 * {@code client} is a likely co-pick, and vice versa. We collapse the two directions into a
 * single neighbor set.
 *
 * <p>Why pool-only: edges that walk out of the candidate pool can never influence the greedy
 * selection (we never pick something outside the pool), so dropping them keeps the in-memory
 * adjacency map small and prevents leaking neighbor ids the caller has no privilege on.
 */
public final class AdjacencyLoader {

    private AdjacencyLoader() {
    }

    /**
     * Returns {@code nodeId → Set<neighborId>} where every neighbor is also in {@code nodeIds}.
     * Self-loops are dropped. Empty input or empty result set yields an empty map.
     */
    public static Map<Long, Set<Long>> loadUndirected1Hop(
            ReferenceExpander expander,
            Set<Long> nodeIds,
            long contextBaseId,
            List<Long> collectionIds,
            long snapshotFence,
            Collection<String> edgeTypes) {
        return loadUndirected1Hop(expander, nodeIds, contextBaseId, null, collectionIds,
                snapshotFence, edgeTypes);
    }

    /**
     * Multi-contextbase variant. {@code contextBaseIds} confines adjacency to the union of the
     * requested bases when {@code contextBaseId <= 0} (the single-base id wins when positive).
     */
    public static Map<Long, Set<Long>> loadUndirected1Hop(
            ReferenceExpander expander,
            Set<Long> nodeIds,
            long contextBaseId,
            List<Long> contextBaseIds,
            List<Long> collectionIds,
            long snapshotFence,
            Collection<String> edgeTypes) {
        if (nodeIds == null || nodeIds.isEmpty() || expander == null) {
            return Collections.emptyMap();
        }
        Map<Long, Set<Long>> adj = new HashMap<>();
        Long cbId = contextBaseId > 0 ? contextBaseId : null;
        boolean multiBase = contextBaseIds != null && !contextBaseIds.isEmpty();

        // Two passes — FORWARD (src ∈ nodes → dst) and BACKWARD (dst ∈ nodes → src). The scan
        // method returns pairs in the shape (queried_endpoint, other_endpoint) regardless of
        // direction; we only add edges where the other endpoint is also in the pool. The single-base
        // path keeps calling the original scan signature so it stays the only contextbase predicate.
        addHits(adj, scan(expander, nodeIds, ReferenceExpander.Direction.FORWARD,
                cbId, contextBaseIds, multiBase, collectionIds, snapshotFence, edgeTypes), nodeIds);
        addHits(adj, scan(expander, nodeIds, ReferenceExpander.Direction.BACKWARD,
                cbId, contextBaseIds, multiBase, collectionIds, snapshotFence, edgeTypes), nodeIds);
        return adj;
    }

    private static List<long[]> scan(ReferenceExpander expander, Set<Long> nodeIds,
                                     ReferenceExpander.Direction direction, Long cbId,
                                     List<Long> contextBaseIds, boolean multiBase,
                                     List<Long> collectionIds, long snapshotFence,
                                     Collection<String> edgeTypes) {
        if (multiBase) {
            return expander.scan(nodeIds, direction, cbId, contextBaseIds, /*collectionId*/ null,
                    collectionIds, snapshotFence, edgeTypes);
        }
        return expander.scan(nodeIds, direction, cbId, /*collectionId*/ null,
                collectionIds, snapshotFence, edgeTypes);
    }

    private static void addHits(Map<Long, Set<Long>> adj, List<long[]> hits, Set<Long> nodeIds) {
        if (hits == null || hits.isEmpty()) {
            return;
        }
        for (long[] pair : hits) {
            long a = pair[0];
            long b = pair[1];
            if (a == b) {
                // Self-loops carry no greedy signal — Python build_fk_graph also skips them.
                continue;
            }
            if (!nodeIds.contains(a) || !nodeIds.contains(b)) {
                continue;
            }
            adj.computeIfAbsent(a, k -> new HashSet<>()).add(b);
            adj.computeIfAbsent(b, k -> new HashSet<>()).add(a);
        }
    }
}
