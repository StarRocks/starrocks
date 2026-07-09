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

package com.starrocks.context.retrieval.rerank;

import com.starrocks.common.Config;
import com.starrocks.context.retrieval.ContextSearchExecutor;
import com.starrocks.context.retrieval.rerank.support.AdjacencyLoader;
import com.starrocks.context.retrieval.rerank.support.BaseScore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

/**
 * Iterative greedy reranker — direct port of the FK-greedy algorithm validated on
 * BIRD / SpiderUnion in AgentBase-bench. Selects candidates one at a time, scoring each by
 * {@code utility(c) = base_score(c) + (β if c is FK-linked to any already-selected entity)},
 * then expands the frontier with the picked entity's 1-hop graph neighbors before the next pick.
 *
 * <p>Why this rather than additive: a single-pass weighted sum can't model the
 * "is FK-connected to what I just picked" signal — that's a state-dependent bonus. Empirically
 * (handoff doc §2), this gives +24pp AllHit@10 on Spider 3+ table joins over pure vector,
 * while leaving single-table queries virtually untouched.
 *
 * <p>Three design decisions guard against the failure modes the upstream Python implementation
 * found the hard way:
 * <ul>
 *   <li>The first pick has an empty frontier, so β never fires — rank-1 is always the
 *       base-score top-1. Single-table queries stay safe.</li>
 *   <li>β defaults to 0.05 — small enough that obviously more relevant non-FK candidates still
 *       beat FK neighbors, only cosine-close ties get flipped. Hub-table reshuffling is avoided.</li>
 *   <li>The frontier signal is binary ("is c linked to any selected?") and not weighted by hop
 *       count, neighbor count, or neighbor score — those formulae let hub tables dominate.</li>
 * </ul>
 *
 * <p>Strategy-specific options on {@code request.strategyOptions}:
 * <ul>
 *   <li>{@code "beta"} (number) — overrides {@link Config#context_search_default_graph_beta}</li>
 *   <li>{@code "base_score"} (string) — {@code "vector_only"} (default) or {@code "weighted_vt"}</li>
 * </ul>
 *
 * <p>{@link #needsGraphExpandedPool} returns {@code false}: this strategy doesn't use the
 * graph BFS results merged into the pool. It loads its own 1-hop adjacency restricted to the
 * pool members, which is cheaper and avoids polluting the pool with low-relevance neighbors.
 */
public final class VectorAnchorGreedyRerankStrategy implements RerankStrategy {

    public static final String NAME = "vector_anchor_greedy";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean needsGraphExpandedPool() {
        return false;
    }

    @Override
    public List<ContextSearchExecutor.Candidate> rerank(RerankContext ctx) {
        ContextSearchExecutor.Request req = ctx.request;
        List<ContextSearchExecutor.Candidate> pool = ctx.pool;
        if (pool == null || pool.isEmpty()) {
            return Collections.emptyList();
        }

        double beta = optionDouble(req.strategyOptions, "beta",
                Config.context_search_default_graph_beta);
        BaseScore.Mode mode = BaseScore.Mode.parse(
                optionString(req.strategyOptions, "base_score", "vector_only"));

        // Snapshot pool ids in insertion order. LinkedHashSet keeps the order stable for
        // tiebreaking when two candidates have identical utility.
        Set<Long> poolIds = pool.stream()
                .map(c -> c.entityId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<Long, Set<Long>> adj = AdjacencyLoader.loadUndirected1Hop(
                ctx.refExpander, poolIds, ctx.contextBaseId, ctx.contextBaseIds, ctx.collectionIds,
                ctx.snapshotFence, req.edgeTypes);

        ToDoubleFunction<ContextSearchExecutor.Candidate> baseScore =
                c -> BaseScore.compute(c, req, mode);

        // Greedy loop. We never sort the remaining set globally — instead, we scan in the
        // original pool insertion order on each iteration. This is O(K * N) for K = max_results
        // and N = pool size, which is bounded (N ≤ max_results * 3 by VectorSearchExecutor's
        // fuse-pool multiplier on line 276 of ContextSearchExecutor); for typical max_results=10
        // this is ~300 scalar comparisons per iteration.
        List<ContextSearchExecutor.Candidate> remaining = new ArrayList<>(pool);
        Set<Long> selectedIds = new HashSet<>();
        Set<Long> frontier = new HashSet<>();
        int target = Math.min(req.maxResults, pool.size());
        List<ContextSearchExecutor.Candidate> out = new ArrayList<>(target);

        while (out.size() < target && !remaining.isEmpty()) {
            ContextSearchExecutor.Candidate best = null;
            double bestUtility = -Double.MAX_VALUE;
            int bestIdx = -1;
            for (int i = 0; i < remaining.size(); i++) {
                ContextSearchExecutor.Candidate c = remaining.get(i);
                double utility = baseScore.applyAsDouble(c)
                        + (frontier.contains(c.entityId) ? beta : 0.0);
                if (utility > bestUtility) {
                    bestUtility = utility;
                    best = c;
                    bestIdx = i;
                }
            }
            if (best == null) {
                break;
            }
            best.finalScore = bestUtility;
            out.add(best);
            remaining.remove(bestIdx);
            selectedIds.add(best.entityId);
            for (Long nb : adj.getOrDefault(best.entityId, Collections.emptySet())) {
                if (poolIds.contains(nb) && !selectedIds.contains(nb)) {
                    frontier.add(nb);
                }
            }
        }

        if (ctx.explain != null) {
            int undirectedEdges = adj.values().stream().mapToInt(Set::size).sum() / 2;
            ctx.explain.put("rerank_strategy", NAME);
            ctx.explain.put("rerank_beta", beta);
            ctx.explain.put("rerank_base_score", mode.name().toLowerCase(Locale.ROOT));
            ctx.explain.put("rerank_pool_size", pool.size());
            ctx.explain.put("rerank_adjacency_edges", undirectedEdges);
        }
        return out;
    }

    private static double optionDouble(Map<String, Object> opts, String key, double fallback) {
        if (opts == null) {
            return fallback;
        }
        Object v = opts.get(key);
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        if (v instanceof String) {
            try {
                return Double.parseDouble((String) v);
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    private static String optionString(Map<String, Object> opts, String key, String fallback) {
        if (opts == null) {
            return fallback;
        }
        Object v = opts.get(key);
        return v instanceof String ? (String) v : fallback;
    }
}
