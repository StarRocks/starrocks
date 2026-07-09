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

import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.policy.CollectionTypePolicy;
import com.starrocks.context.retrieval.ContextSearchExecutor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reciprocal Rank Fusion (RRF). Fuses the text / vector / graph channels by each candidate's
 * <em>rank position</em> within a channel rather than its raw score:
 *
 * <pre>final = Σ_channel  weight_channel / (k + rank_channel(candidate))</pre>
 *
 * <p>Why rank, not score: the three channels are on incomparable, partly-miscalibrated scales — the
 * text channel is a BM25-lite/hit-count magnitude, the vector channel a cosine clustered in a narrow
 * band, the graph channel a hop-decay path score capped at 0.5. Additive fusion lets whichever
 * channel has the largest numeric range dominate (a spurious keyword hit at text_score≈1.0 buries a
 * strong vector match), and a graph-only candidate's tiny absolute path score can never out-add a
 * text/vector hit. RRF is invariant to all of that: a candidate ranked #1 in any channel contributes
 * {@code w/(k+1)} regardless of the channel's absolute scale, so each channel gets a fair, bounded
 * vote. RRF is the best choice when every channel carries real signal; when one channel is noise
 * (e.g. the text channel on natural-language queries) its equal-weight rank vote can amplify that
 * noise, which is why the cluster default is {@code vector_anchor_greedy} (vector-anchored) rather
 * than RRF. {@link AdditiveRerankStrategy} stays available for magnitude-weighted fusion once
 * channel scores are calibrated.
 *
 * <p>Channels a candidate did not appear in (score ≤ 0) contribute nothing for that candidate.
 * {@code k} defaults to {@link #DEFAULT_RRF_K} (60, the industry-standard value) and is
 * per-request overridable via {@code strategy_options.rrf_k}. The graph channel ranks on the
 * synthesis-demoted graph contribution and synthesis entities take the
 * {@link ContextSearchExecutor#SYNTHESIS_FINAL_SCORE_FACTOR} final tiebreak, mirroring the additive
 * strategy's two-layer synthesis handling.
 *
 * <p>Returns {@code true} from {@link #needsGraphExpandedPool} because it ranks {@code graphScore},
 * populated by the BFS reference expansion in {@link ContextSearchExecutor#search}.
 */
public final class RrfRerankStrategy implements RerankStrategy {

    public static final String NAME = "rrf";

    /** RRF constant k: final = Σ_channel weight / (k + rank). 60 is the industry-standard value.
     *  Per-request override via {@code strategy_options.rrf_k}. */
    public static final int DEFAULT_RRF_K = 60;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean needsGraphExpandedPool() {
        return true;
    }

    @Override
    public List<ContextSearchExecutor.Candidate> rerank(RerankContext ctx) {
        ContextSearchExecutor.Request req = ctx.request;
        List<ContextSearchExecutor.Candidate> pool = ctx.pool;
        int k = resolveK(req.strategyOptions);

        // Per-channel rank maps (1-indexed). A candidate absent from a channel (score <= 0) is not
        // present in that channel's map and contributes nothing from it.
        Map<Long, Integer> textRank = rankByChannel(pool, c -> c.textScore);
        Map<Long, Integer> vectorRank = rankByChannel(pool, c -> c.vectorScore);
        Map<Long, Integer> graphRank = rankByChannel(pool, c -> graphContribution(ctx, c));

        for (ContextSearchExecutor.Candidate c : pool) {
            double score = 0.0;
            Integer tr = textRank.get(c.entityId);
            if (tr != null) {
                score += req.textWeight / (k + tr);
            }
            Integer vr = vectorRank.get(c.entityId);
            if (vr != null) {
                score += req.vectorWeight / (k + vr);
            }
            Integer gr = graphRank.get(c.entityId);
            if (gr != null) {
                score += req.graphWeight / (k + gr);
            }
            if (isSynthesis(ctx, c)) {
                score *= ContextSearchExecutor.SYNTHESIS_FINAL_SCORE_FACTOR;
            }
            c.finalScore = score;
        }

        // Stable order: final score desc, entityId asc as a deterministic tiebreak.
        pool.sort(Comparator
                .comparingDouble((ContextSearchExecutor.Candidate c) -> c.finalScore).reversed()
                .thenComparingLong(c -> c.entityId));

        if (ctx.explain != null) {
            ctx.explain.put("rerank_strategy", NAME);
            ctx.explain.put("rrf_k", k);
        }
        return pool.size() <= req.maxResults
                ? pool
                : new ArrayList<>(pool.subList(0, req.maxResults));
    }

    /** Synthesis-demoted graph contribution used as the graph-channel ranking key. */
    private static double graphContribution(RerankContext ctx, ContextSearchExecutor.Candidate c) {
        return isSynthesis(ctx, c)
                ? c.graphScore * ContextSearchExecutor.SYNTHESIS_GRAPH_SCORE_FACTOR
                : c.graphScore;
    }

    private static boolean isSynthesis(RerankContext ctx, ContextSearchExecutor.Candidate c) {
        ContextReadExecutor.EntityMeta meta = ctx.metaByEntity == null
                ? null : ctx.metaByEntity.get(c.entityId);
        return meta != null && CollectionTypePolicy.isSynthesisType(meta.entityType);
    }

    /**
     * Build a 1-indexed rank map for one channel: sort the candidates with positive channel score
     * descending (entityId asc as deterministic tiebreak) and assign ranks 1, 2, 3, …. Candidates
     * with a non-positive score are omitted so they earn no contribution from this channel.
     */
    private static Map<Long, Integer> rankByChannel(
            List<ContextSearchExecutor.Candidate> pool,
            java.util.function.ToDoubleFunction<ContextSearchExecutor.Candidate> channel) {
        List<ContextSearchExecutor.Candidate> participants = new ArrayList<>();
        for (ContextSearchExecutor.Candidate c : pool) {
            if (channel.applyAsDouble(c) > 0.0) {
                participants.add(c);
            }
        }
        participants.sort(Comparator
                .comparingDouble(channel).reversed()
                .thenComparingLong(c -> c.entityId));
        Map<Long, Integer> ranks = new HashMap<>(participants.size() * 2);
        for (int i = 0; i < participants.size(); i++) {
            ranks.put(participants.get(i).entityId, i + 1);
        }
        return ranks;
    }

    private static int resolveK(Map<String, Object> opts) {
        int fallback = DEFAULT_RRF_K;
        if (opts == null) {
            return fallback;
        }
        Object v = opts.get("rrf_k");
        if (v instanceof Number) {
            int parsed = ((Number) v).intValue();
            return parsed > 0 ? parsed : fallback;
        }
        if (v instanceof String) {
            try {
                int parsed = Integer.parseInt(((String) v).trim());
                return parsed > 0 ? parsed : fallback;
            } catch (NumberFormatException ignore) {
                return fallback;
            }
        }
        return fallback;
    }
}
