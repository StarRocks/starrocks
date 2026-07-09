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
import com.starrocks.context.retrieval.ContextSearchExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Direct unit tests for {@link AdditiveRerankStrategy} — the linear-combination ranker.
 * Equivalent behavior was previously tested end-to-end via {@link ContextSearchExecutor}; these
 * narrower tests pin the contract after the refactor moved the logic into a separate class.
 */
public class AdditiveRerankStrategyTest {

    @Test
    public void testLinearCombinationOrdersByFinalScore() {
        // Candidate 1: text=1.0, vector=0.5; Candidate 2: text=0.3, vector=0.9
        // weights = {text:0.5, vector:0.5} → final_1 = 0.75, final_2 = 0.60 → 1 outranks 2.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 1.0, 0.5, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.3, 0.9, 0.0);
        ContextSearchExecutor.Request req = req(0.5, 0.5, 0.0, 10);

        List<ContextSearchExecutor.Candidate> top =
                new AdditiveRerankStrategy().rerank(ctx(req, c1, c2));
        Assertions.assertEquals(2, top.size());
        Assertions.assertEquals(1L, top.get(0).entityId);
        Assertions.assertEquals(0.75, top.get(0).finalScore, 1e-9);
        Assertions.assertEquals(2L, top.get(1).entityId);
        Assertions.assertEquals(0.60, top.get(1).finalScore, 1e-9);
    }

    @Test
    public void testGraphContributionIncludedWhenWeightNonZero() {
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.0, 1.0);
        ContextSearchExecutor.Request req = req(0.0, 0.0, 1.0, 10);
        List<ContextSearchExecutor.Candidate> top =
                new AdditiveRerankStrategy().rerank(ctx(req, c1));
        Assertions.assertEquals(1.0, top.get(0).finalScore, 1e-9);
    }

    @Test
    public void testSynthesisEntityIsDemoted() {
        // Two entities with the same raw scores; the derived_page must rank lower because of
        // SYNTHESIS_GRAPH_SCORE_FACTOR (0.5) × SYNTHESIS_FINAL_SCORE_FACTOR (0.9) = 0.45 vs 1.0.
        ContextSearchExecutor.Candidate leaf = candidate(1L, 0.0, 0.0, 1.0);
        ContextSearchExecutor.Candidate synth = candidate(2L, 0.0, 0.0, 1.0);
        ContextSearchExecutor.Request req = req(0.0, 0.0, 1.0, 10);

        Map<Long, ContextReadExecutor.EntityMeta> meta = new HashMap<>();
        meta.put(1L, metaWith("leaf", "page"));
        meta.put(2L, metaWith("synth", "derived_page"));

        List<ContextSearchExecutor.Candidate> top =
                new AdditiveRerankStrategy().rerank(ctxWithMeta(req, meta, leaf, synth));
        Assertions.assertEquals(1L, top.get(0).entityId);
        Assertions.assertEquals(1.0, top.get(0).finalScore, 1e-9);
        Assertions.assertEquals(2L, top.get(1).entityId);
        Assertions.assertEquals(0.45, top.get(1).finalScore, 1e-9);
    }

    @Test
    public void testMaxResultsTruncation() {
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.9, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.7, 0.0);
        ContextSearchExecutor.Candidate c3 = candidate(3L, 0.0, 0.5, 0.0);
        ContextSearchExecutor.Request req = req(0.0, 1.0, 0.0, 2);
        List<ContextSearchExecutor.Candidate> top =
                new AdditiveRerankStrategy().rerank(ctx(req, c1, c2, c3));
        Assertions.assertEquals(2, top.size());
        Assertions.assertEquals(1L, top.get(0).entityId);
        Assertions.assertEquals(2L, top.get(1).entityId);
    }

    @Test
    public void testEmptyPoolReturnsEmpty() {
        ContextSearchExecutor.Request req = req(0.5, 0.3, 0.2, 10);
        List<ContextSearchExecutor.Candidate> top =
                new AdditiveRerankStrategy().rerank(ctx(req));
        Assertions.assertTrue(top.isEmpty());
    }

    @Test
    public void testExplainBreadcrumbWritten() {
        ContextSearchExecutor.Candidate c1 = candidate(1L, 1.0, 0.0, 0.0);
        ContextSearchExecutor.Request req = req(1.0, 0.0, 0.0, 10);
        Map<String, Object> explain = new LinkedHashMap<>();
        RerankContext rctx = baseBuilder(req, Collections.emptyMap(), explain, c1).build();
        new AdditiveRerankStrategy().rerank(rctx);
        Assertions.assertEquals(AdditiveRerankStrategy.NAME, explain.get("rerank_strategy"));
    }

    // ---------- helpers ----------

    private static ContextSearchExecutor.Candidate candidate(long id, double text, double vec, double graph) {
        return new ContextSearchExecutor.Candidate(id, text, vec, graph, 0, new ArrayList<>(), null);
    }

    private static ContextSearchExecutor.Request req(double textW, double vecW, double graphW, int maxResults) {
        ContextSearchExecutor.Request r = new ContextSearchExecutor.Request();
        r.textWeight = textW;
        r.vectorWeight = vecW;
        r.graphWeight = graphW;
        r.maxResults = maxResults;
        return r;
    }

    private static ContextReadExecutor.EntityMeta metaWith(String key, String type) {
        return new ContextReadExecutor.EntityMeta(0L, key, type, 1L, 0L, "", 1.0, null, null);
    }

    private static RerankContext ctx(ContextSearchExecutor.Request req,
                                     ContextSearchExecutor.Candidate... candidates) {
        return baseBuilder(req, Collections.emptyMap(), new LinkedHashMap<>(), candidates).build();
    }

    private static RerankContext ctxWithMeta(ContextSearchExecutor.Request req,
                                             Map<Long, ContextReadExecutor.EntityMeta> meta,
                                             ContextSearchExecutor.Candidate... candidates) {
        return baseBuilder(req, meta, new LinkedHashMap<>(), candidates).build();
    }

    private static RerankContext.Builder baseBuilder(ContextSearchExecutor.Request req,
                                                     Map<Long, ContextReadExecutor.EntityMeta> meta,
                                                     Map<String, Object> explain,
                                                     ContextSearchExecutor.Candidate... candidates) {
        List<ContextSearchExecutor.Candidate> pool = new ArrayList<>();
        Collections.addAll(pool, candidates);
        return RerankContext.builder()
                .pool(pool)
                .metaByEntity(meta)
                .request(req)
                .contextBaseId(1L)
                .collectionIds(Collections.singletonList(1L))
                .snapshotFence(-1L)
                .explain(explain);
    }
}
