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
import java.util.List;

/**
 * The legacy linear-combination ranker. Computes
 * {@code final = textWeight·textScore + vectorWeight·vectorScore + graphWeight·graphScore} for
 * every candidate, applying the two-layer synthesis demotion documented in the architecture doc
 * §10.5, then sorts descending and takes the top {@code maxResults}.
 *
 * <p>This is the default strategy — backwards-compatible behavior for all existing callers that
 * don't pass {@code graph_strategy} in the search payload.
 *
 * <p>Returns {@code true} from {@link #needsGraphExpandedPool} because the formula reads
 * {@code graphScore}, which is populated by the BFS reference expansion in
 * {@link ContextSearchExecutor#search}.
 */
public final class AdditiveRerankStrategy implements RerankStrategy {

    public static final String NAME = "additive";

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
        for (ContextSearchExecutor.Candidate c : pool) {
            ContextReadExecutor.EntityMeta meta = ctx.metaByEntity == null
                    ? null : ctx.metaByEntity.get(c.entityId);
            boolean isSynthesis = meta != null
                    && CollectionTypePolicy.isSynthesisType(meta.entityType);
            double graphContribution = isSynthesis
                    ? c.graphScore * ContextSearchExecutor.SYNTHESIS_GRAPH_SCORE_FACTOR
                    : c.graphScore;
            c.finalScore = req.textWeight * c.textScore
                    + req.vectorWeight * c.vectorScore
                    + req.graphWeight * graphContribution;
            if (isSynthesis) {
                c.finalScore *= ContextSearchExecutor.SYNTHESIS_FINAL_SCORE_FACTOR;
            }
        }
        pool.sort((a, b) -> Double.compare(b.finalScore, a.finalScore));
        if (ctx.explain != null) {
            ctx.explain.put("rerank_strategy", NAME);
        }
        return pool.size() <= req.maxResults
                ? pool
                : new ArrayList<>(pool.subList(0, req.maxResults));
    }
}
