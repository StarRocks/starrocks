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
 * Unit tests for {@link RrfRerankStrategy} — reciprocal-rank fusion. The headline test is the
 * scale-mismatch scenario where additive fusion is fooled by a keyword-stuffed candidate but RRF,
 * fusing by rank, lets the candidate that ranks well across channels win.
 */
public class RrfRerankStrategyTest {

    @Test
    public void testRankFusionBeatsAdditiveOnScaleMismatch() {
        // Three candidates, weights equal per channel:
        //   A: text=0.20, vector=0.82 (best vector), graph=0      → vector#1, text#2
        //   B: text=0.10, vector=0.80,               graph=0.50   → vector#2, text#3, graph#1 (bridge)
        //   C: text=1.00 (keyword-stuffed), vector=0.79, graph=0  → text#1, vector#3
        // Additive(0.5/0.3/0.2): C=0.5+0.237=0.737 wins on the spurious text spike.
        // RRF(k=60): B = 1/62+1/63+1/61 = 0.04839 wins; C = 1/63+1/61 = 0.03226 falls to last.
        ContextSearchExecutor.Candidate a = candidate(1L, 0.20, 0.82, 0.0);
        ContextSearchExecutor.Candidate b = candidate(2L, 0.10, 0.80, 0.50);
        ContextSearchExecutor.Candidate c = candidate(3L, 1.00, 0.79, 0.0);

        // Additive picks the keyword-stuffed C first (the pathology RRF fixes).
        List<ContextSearchExecutor.Candidate> additive =
                new AdditiveRerankStrategy().rerank(ctx(req(0.5, 0.3, 0.2, 10), a, b, c));
        Assertions.assertEquals(3L, additive.get(0).entityId);

        // RRF with equal channel weights picks the cross-signal bridge B first, C last.
        List<ContextSearchExecutor.Candidate> rrf =
                new RrfRerankStrategy().rerank(ctx(req(1.0, 1.0, 1.0, 10), a, b, c));
        Assertions.assertEquals(2L, rrf.get(0).entityId);
        Assertions.assertEquals(3L, rrf.get(2).entityId);
    }

    @Test
    public void testReciprocalRankScoreMath() {
        // Single channel (vector), k=60: ranks 1,2 → scores 1/61, 1/62.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.9, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.7, 0.0);
        List<ContextSearchExecutor.Candidate> top =
                new RrfRerankStrategy().rerank(ctx(req(0.0, 1.0, 0.0, 10), c1, c2));
        int k = RrfRerankStrategy.DEFAULT_RRF_K;
        Assertions.assertEquals(1L, top.get(0).entityId);
        Assertions.assertEquals(1.0 / (k + 1), top.get(0).finalScore, 1e-12);
        Assertions.assertEquals(1.0 / (k + 2), top.get(1).finalScore, 1e-12);
    }

    @Test
    public void testAbsentChannelContributesNothing() {
        // c1 only has a vector score, c2 only a text score; with equal weights and identical rank
        // (#1 in their respective single channel) they tie, broken deterministically by entityId.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.9, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.9, 0.0, 0.0);
        List<ContextSearchExecutor.Candidate> top =
                new RrfRerankStrategy().rerank(ctx(req(1.0, 1.0, 1.0, 10), c1, c2));
        int k = RrfRerankStrategy.DEFAULT_RRF_K;
        Assertions.assertEquals(1.0 / (k + 1), top.get(0).finalScore, 1e-12);
        Assertions.assertEquals(1.0 / (k + 1), top.get(1).finalScore, 1e-12);
        Assertions.assertEquals(1L, top.get(0).entityId); // entityId asc tiebreak
    }

    @Test
    public void testRrfKOverrideViaStrategyOptions() {
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.9, 0.0);
        ContextSearchExecutor.Request req = req(0.0, 1.0, 0.0, 10);
        req.strategyOptions = new HashMap<>();
        req.strategyOptions.put("rrf_k", 10);
        List<ContextSearchExecutor.Candidate> top =
                new RrfRerankStrategy().rerank(ctx(req, c1));
        Assertions.assertEquals(1.0 / (10 + 1), top.get(0).finalScore, 1e-12);
    }

    @Test
    public void testSynthesisFinalDemotion() {
        // Identical single-channel rank (#1 graph each) but entity 2 is synthesis → final ×0.9.
        ContextSearchExecutor.Candidate leaf = candidate(1L, 0.0, 0.0, 0.9);
        ContextSearchExecutor.Candidate synth = candidate(2L, 0.0, 0.0, 0.9);
        Map<Long, ContextReadExecutor.EntityMeta> meta = new HashMap<>();
        meta.put(1L, metaWith("leaf", "page"));
        meta.put(2L, metaWith("synth", "derived_page"));
        List<ContextSearchExecutor.Candidate> top = new RrfRerankStrategy()
                .rerank(ctxWithMeta(req(0.0, 0.0, 1.0, 10), meta, leaf, synth));
        // leaf ranks graph#1, synth graph#2 (graphContribution halved → lower key); plus synth ×0.9.
        Assertions.assertEquals(1L, top.get(0).entityId);
        Assertions.assertTrue(top.get(0).finalScore > top.get(1).finalScore);
    }

    @Test
    public void testExplainBreadcrumbWritten() {
        ContextSearchExecutor.Candidate c1 = candidate(1L, 1.0, 0.0, 0.0);
        Map<String, Object> explain = new LinkedHashMap<>();
        RerankContext rctx = baseBuilder(req(1.0, 0.0, 0.0, 10), Collections.emptyMap(), explain, c1).build();
        new RrfRerankStrategy().rerank(rctx);
        Assertions.assertEquals(RrfRerankStrategy.NAME, explain.get("rerank_strategy"));
        Assertions.assertEquals(RrfRerankStrategy.DEFAULT_RRF_K, explain.get("rrf_k"));
    }

    // ---------- helpers (mirror AdditiveRerankStrategyTest) ----------

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
