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

import com.starrocks.context.retrieval.ContextSearchExecutor;
import com.starrocks.context.retrieval.ReferenceExpander;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link VectorAnchorGreedyRerankStrategy}. The strategy issues 1-hop reads through
 * {@link ReferenceExpander#scan}; we stub it with a deterministic edge map keyed by direction so
 * tests pin both the greedy loop and the undirected adjacency merge.
 */
public class VectorAnchorGreedyRerankStrategyTest {

    @Test
    public void testFirstPickIsAlwaysVectorTopWhenFrontierEmpty() {
        // Rank-1 must always equal vector top-1: β never fires on the first pick because the
        // frontier starts empty. This is the safety guarantee that single-table queries lean on.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.85, 0.0);
        ContextSearchExecutor.Candidate c3 = candidate(3L, 0.0, 0.10, 0.0);
        // Even with strong edges into c3, c3 must not jump to rank-1.
        StubExpander expander = new StubExpander()
                .addForward(2L, 3L).addForward(1L, 3L);
        ContextSearchExecutor.Request req = greedyReq(0.05, 3);

        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, expander, c1, c2, c3));
        Assertions.assertEquals(1L, top.get(0).entityId, "rank-1 must be vector top-1");
    }

    @Test
    public void testFrontierFlipsCosineClosePair() {
        // Three candidates; cosine: c1=0.90, c2=0.80, c3=0.81 (cosine-close to c2).
        // FK graph: c1 <-> c3. After picking c1, c3 gets the β bonus → c3 should jump above c2.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.80, 0.0);
        ContextSearchExecutor.Candidate c3 = candidate(3L, 0.0, 0.81, 0.0);
        StubExpander expander = new StubExpander().addForward(1L, 3L);
        ContextSearchExecutor.Request req = greedyReq(0.05, 3);

        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, expander, c1, c2, c3));
        Assertions.assertEquals(1L, top.get(0).entityId);
        Assertions.assertEquals(3L, top.get(1).entityId, "FK neighbor must outrank cosine-close non-neighbor");
        Assertions.assertEquals(2L, top.get(2).entityId);
        // Rank-2 utility = 0.81 + 0.05 = 0.86
        Assertions.assertEquals(0.86, top.get(1).finalScore, 1e-9);
    }

    @Test
    public void testFrontierWontFlipObviouslyMoreRelevantNonNeighbor() {
        // Non-FK candidate beats FK candidate by more than β → it still wins.
        // c1=0.90 (picked first), c2=0.50 (non-FK), c3=0.40 (FK to c1). c2 should outrank c3
        // because 0.50 > 0.40 + 0.05 = 0.45.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.50, 0.0);
        ContextSearchExecutor.Candidate c3 = candidate(3L, 0.0, 0.40, 0.0);
        StubExpander expander = new StubExpander().addForward(1L, 3L);
        ContextSearchExecutor.Request req = greedyReq(0.05, 3);

        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, expander, c1, c2, c3));
        Assertions.assertEquals(Arrays.asList(1L, 2L, 3L),
                Arrays.asList(top.get(0).entityId, top.get(1).entityId, top.get(2).entityId));
    }

    @Test
    public void testBetaZeroDegeneratesToVectorRanking() {
        // β=0 means FK bonus never fires → output is pure vector ranking, even with FK edges.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.80, 0.0);
        ContextSearchExecutor.Candidate c3 = candidate(3L, 0.0, 0.81, 0.0);
        StubExpander expander = new StubExpander().addForward(1L, 3L);
        ContextSearchExecutor.Request req = greedyReq(0.0, 3);

        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, expander, c1, c2, c3));
        Assertions.assertEquals(Arrays.asList(1L, 3L, 2L),
                Arrays.asList(top.get(0).entityId, top.get(1).entityId, top.get(2).entityId));
    }

    @Test
    public void testUndirectedAdjacencyFromBothDirections() {
        // Edge stored as 1 -> 2 (FORWARD scan). After picking 2 first (higher cosine), the
        // BACKWARD scan of node 2 should surface node 1 as a neighbor. Without bidirectional
        // merge, the β bonus would not fire and 1's pick order could be wrong.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.70, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.80, 0.0);
        ContextSearchExecutor.Candidate c3 = candidate(3L, 0.0, 0.75, 0.0);
        // Edge 1 -> 2 only. After picking 2 first (cosine 0.80), c1 should enter frontier via
        // the backward scan (dst=2 → src=1).
        StubExpander expander = new StubExpander().addBackward(2L, 1L);
        ContextSearchExecutor.Request req = greedyReq(0.10, 3);

        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, expander, c1, c2, c3));
        Assertions.assertEquals(2L, top.get(0).entityId);
        // c1 utility = 0.70 + 0.10 = 0.80 ; c3 utility = 0.75 → c1 should win rank-2.
        Assertions.assertEquals(1L, top.get(1).entityId);
        Assertions.assertEquals(0.80, top.get(1).finalScore, 1e-9);
    }

    @Test
    public void testEdgeOutsidePoolIsIgnored() {
        // FK target 99 is not in the candidate pool → should not produce a phantom frontier entry.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.50, 0.0);
        StubExpander expander = new StubExpander().addForward(1L, 99L);
        ContextSearchExecutor.Request req = greedyReq(0.05, 2);

        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, expander, c1, c2));
        Assertions.assertEquals(Arrays.asList(1L, 2L),
                Arrays.asList(top.get(0).entityId, top.get(1).entityId));
        Assertions.assertEquals(0.90, top.get(0).finalScore, 1e-9);
        Assertions.assertEquals(0.50, top.get(1).finalScore, 1e-9);
    }

    @Test
    public void testSelfLoopIgnored() {
        // Self-loop 1 -> 1 must not put 1 into its own frontier — would self-boost forever.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        StubExpander expander = new StubExpander().addForward(1L, 1L);
        ContextSearchExecutor.Request req = greedyReq(0.05, 1);

        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, expander, c1));
        Assertions.assertEquals(0.90, top.get(0).finalScore, 1e-9);
    }

    @Test
    public void testEmptyPoolReturnsEmpty() {
        ContextSearchExecutor.Request req = greedyReq(0.05, 10);
        List<ContextSearchExecutor.Candidate> top =
                new VectorAnchorGreedyRerankStrategy().rerank(ctx(req, new StubExpander()));
        Assertions.assertTrue(top.isEmpty());
    }

    @Test
    public void testMaxResultsRespected() {
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.80, 0.0);
        ContextSearchExecutor.Candidate c3 = candidate(3L, 0.0, 0.70, 0.0);
        ContextSearchExecutor.Request req = greedyReq(0.05, 2);
        List<ContextSearchExecutor.Candidate> top = new VectorAnchorGreedyRerankStrategy()
                .rerank(ctx(req, new StubExpander(), c1, c2, c3));
        Assertions.assertEquals(2, top.size());
    }

    @Test
    public void testWeightedVtBaseScore() {
        // base_score=weighted_vt → text_score also contributes. c2 has higher text+vector sum.
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.1, 0.90, 0.0);  // 0.5*0.1 + 0.5*0.9 = 0.50
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.9, 0.20, 0.0);  // 0.5*0.9 + 0.5*0.2 = 0.55
        ContextSearchExecutor.Request req = greedyReq(0.05, 2);
        req.textWeight = 0.5;
        req.vectorWeight = 0.5;
        req.strategyOptions = new HashMap<>();
        req.strategyOptions.put("base_score", "weighted_vt");

        List<ContextSearchExecutor.Candidate> top = new VectorAnchorGreedyRerankStrategy()
                .rerank(ctx(req, new StubExpander(), c1, c2));
        Assertions.assertEquals(2L, top.get(0).entityId, "weighted_vt should pick c2 over c1");
    }

    @Test
    public void testExplainCarriesProvenance() {
        ContextSearchExecutor.Candidate c1 = candidate(1L, 0.0, 0.90, 0.0);
        ContextSearchExecutor.Candidate c2 = candidate(2L, 0.0, 0.80, 0.0);
        StubExpander expander = new StubExpander().addForward(1L, 2L);
        ContextSearchExecutor.Request req = greedyReq(0.05, 2);
        Map<String, Object> explain = new LinkedHashMap<>();
        RerankContext rctx = builder(req, expander, explain, c1, c2).build();

        new VectorAnchorGreedyRerankStrategy().rerank(rctx);
        Assertions.assertEquals(VectorAnchorGreedyRerankStrategy.NAME, explain.get("rerank_strategy"));
        Assertions.assertEquals(0.05, ((Number) explain.get("rerank_beta")).doubleValue(), 1e-9);
        Assertions.assertEquals("vector_only", explain.get("rerank_base_score"));
        Assertions.assertEquals(2, ((Number) explain.get("rerank_pool_size")).intValue());
        Assertions.assertEquals(1, ((Number) explain.get("rerank_adjacency_edges")).intValue());
    }

    // ---------- helpers ----------

    private static ContextSearchExecutor.Candidate candidate(long id, double text, double vec, double graph) {
        return new ContextSearchExecutor.Candidate(id, text, vec, graph, 0, new ArrayList<>(), null);
    }

    private static ContextSearchExecutor.Request greedyReq(double beta, int maxResults) {
        ContextSearchExecutor.Request r = new ContextSearchExecutor.Request();
        r.textWeight = 0.0;
        r.vectorWeight = 1.0;
        r.graphWeight = 0.0;
        r.maxResults = maxResults;
        r.graphStrategy = VectorAnchorGreedyRerankStrategy.NAME;
        r.strategyOptions = new HashMap<>();
        r.strategyOptions.put("beta", beta);
        return r;
    }

    private static RerankContext ctx(ContextSearchExecutor.Request req, StubExpander expander,
                                     ContextSearchExecutor.Candidate... candidates) {
        return builder(req, expander, new LinkedHashMap<>(), candidates).build();
    }

    private static RerankContext.Builder builder(ContextSearchExecutor.Request req,
                                                 StubExpander expander,
                                                 Map<String, Object> explain,
                                                 ContextSearchExecutor.Candidate... candidates) {
        List<ContextSearchExecutor.Candidate> pool = new ArrayList<>();
        Collections.addAll(pool, candidates);
        return RerankContext.builder()
                .pool(pool)
                .metaByEntity(Collections.emptyMap())
                .request(req)
                .contextBaseId(1L)
                .collectionIds(Collections.singletonList(1L))
                .snapshotFence(-1L)
                .refExpander(expander)
                .explain(explain);
    }

    /**
     * Records FORWARD and BACKWARD edges separately. Each direction returns pairs in the
     * (queried_endpoint, other_endpoint) shape that {@link ReferenceExpander#scan} promises.
     */
    static final class StubExpander extends ReferenceExpander {
        private final List<long[]> forward = new ArrayList<>();
        private final List<long[]> backward = new ArrayList<>();

        StubExpander addForward(long src, long dst) {
            forward.add(new long[] {src, dst});
            return this;
        }

        StubExpander addBackward(long dst, long src) {
            backward.add(new long[] {dst, src});
            return this;
        }

        @Override
        public List<long[]> scan(Collection<Long> nodes, Direction direction,
                                 Long contextBaseId, Long collectionId, List<Long> collectionIds,
                                 long snapshotFence, Collection<String> refKinds) {
            List<long[]> src = direction == Direction.FORWARD ? forward : backward;
            // Filter to pairs whose queried endpoint is in the supplied node set so the test
            // simulates the SQL IN-list behavior of the real implementation.
            List<long[]> out = new ArrayList<>();
            for (long[] pair : src) {
                if (nodes.contains(pair[0])) {
                    out.add(pair);
                }
            }
            return out;
        }
    }
}
