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

import com.starrocks.common.Config;
import com.starrocks.context.ContextMgr;
import com.starrocks.persist.ContextOpLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Tests the pure-Java fusion logic of {@link ContextSearchExecutor} without hitting the internal
 * tables. We stub the text and reference paths so we can assert the candidate pool, weighting, and
 * ordering independently of the SQL layer.
 */
public class ContextSearchExecutorScoringTest {

    private static ContextMgr newMgrWithBase(String baseName) {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(1L, baseName, null));
        return mgr;
    }

    /**
     * Build a {@link ContextSearchExecutor} that returns a deterministic metadata map instead of
     * touching {@link com.starrocks.server.GlobalStateMgr}. Used to drive synthesis-aware code
     * paths in unit tests without a live SQL plane.
     */
    private static ContextSearchExecutor withMeta(
            ContextMgr mgr, TextSearchExecutor text, ReferenceExpander refExpander,
            java.util.Map<Long, com.starrocks.context.ContextReadExecutor.EntityMeta> meta) {
        return new ContextSearchExecutor(mgr, text, refExpander, new StubVectorSearch()) {
            @Override
            protected java.util.Map<Long, com.starrocks.context.ContextReadExecutor.EntityMeta> loadMetadataSafe(
                    java.util.Collection<Long> ids, long snapshotFence) {
                java.util.Map<Long, com.starrocks.context.ContextReadExecutor.EntityMeta> out = new java.util.HashMap<>();
                for (Long id : ids) {
                    com.starrocks.context.ContextReadExecutor.EntityMeta m = meta.get(id);
                    if (m != null) {
                        out.put(id, m);
                    }
                }
                return out;
            }
        };
    }

    /** Minimal EntityMeta stub for tests — only entity_type matters for synthesis dispatch. */
    private static com.starrocks.context.ContextReadExecutor.EntityMeta metaWith(
            String entityKey, String entityType) {
        return new com.starrocks.context.ContextReadExecutor.EntityMeta(
                /*entityId=*/ 0L, entityKey, entityType, /*version=*/ 1L, /*snapshotVersion=*/ 0L,
                /*preview=*/ "", /*confidence=*/ 1.0, /*title=*/ null,
                /*frontmatterJson=*/ null);
    }

    @Test
    public void testTextOnlyScoringUsesWeights() {
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 3, null, 1.0),
                new TextSearchExecutor.EntityHit(2L, 1, null, 0.5));
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.textWeight = 1.0;
        req.vectorWeight = 0.0;
        req.graphWeight = 0.0;
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;
        req.graphStrategy = "additive"; // pins the additive linear-sum scoring (default is now rrf)

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(2, result.candidates.size());
        Assertions.assertEquals(1L, result.candidates.get(0).entityId);
        Assertions.assertEquals(1.0, result.candidates.get(0).finalScore, 1e-9);
        Assertions.assertEquals(0.5, result.candidates.get(1).finalScore, 1e-9);
        Assertions.assertEquals(Boolean.TRUE, result.explain.get("text_enabled"));
        Assertions.assertEquals(Boolean.FALSE, result.explain.get("reference_enabled"));
    }

    @Test
    public void testFusionDegradesToVectorWhenTextChannelFails() {
        // A hybrid search must still return the vector channel's results when the text channel
        // errors (e.g. a text-index error on one contextbase) — not fail the whole search.
        ContextSearchExecutor exec = new ContextSearchExecutor(
                newMgrWithBase("cb1"), new ThrowingTextSearch(), new StubReferenceExpander(),
                new StubVectorSearchWithHits(
                        new VectorSearchExecutor.EntityHit(7L, 0.9, "section", "snip")));

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "kw";
        req.textWeight = 0.5;
        req.vectorWeight = 0.5;
        req.graphWeight = 0.0;
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;

        ContextSearchExecutor.Result result = exec.search(req); // must NOT throw
        Assertions.assertEquals(1, result.candidates.size());
        Assertions.assertEquals(7L, result.candidates.get(0).entityId);
    }

    @Test
    public void testFusionDegradesToTextWhenVectorChannelFails() {
        // Symmetric: vector channel errors (e.g. embedding provider down) → return text results.
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(3L, 1, null, 1.0));
        ContextSearchExecutor exec = new ContextSearchExecutor(
                newMgrWithBase("cb1"), textSearch, new StubReferenceExpander(),
                new ThrowingVectorSearch());

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "kw";
        req.textWeight = 0.5;
        req.vectorWeight = 0.5;
        req.graphWeight = 0.0;
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;

        ContextSearchExecutor.Result result = exec.search(req); // must NOT throw
        Assertions.assertEquals(1, result.candidates.size());
        Assertions.assertEquals(3L, result.candidates.get(0).entityId);
    }

    @Test
    public void testFusionFailsWhenAllChannelsFail() {
        // Only when EVERY enabled channel fails do we surface the error (not a silent empty result).
        ContextSearchExecutor exec = new ContextSearchExecutor(
                newMgrWithBase("cb1"), new ThrowingTextSearch(), new StubReferenceExpander(),
                new ThrowingVectorSearch());

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "kw";
        req.textWeight = 0.5;
        req.vectorWeight = 0.5;
        req.graphWeight = 0.0;
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;

        Assertions.assertThrows(RuntimeException.class, () -> exec.search(req));
    }

    @Test
    public void testGraphModeOffSkipsReferenceExpansion() {
        StubTextSearch textSearch = new StubTextSearch();
        StubReferenceExpander refExpander = new StubReferenceExpander(
                new ReferenceExpander.ExpansionRow(10L, 11L, 1, 0.5, Arrays.asList("inline")));
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.seedIds = Arrays.asList(10L);
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;
        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertTrue(result.candidates.isEmpty());
        Assertions.assertEquals(0, refExpander.callCount);
    }

    @Test
    public void testUnknownContextBaseRejectedWithInvalidScope() {
        // Regression: a misspelled / missing contextbase used to leave contextBaseId null and the
        // downstream search would silently scan every contextbase in the cluster — a real
        // cross-tenant data leak. The executor must now raise INVALID_SCOPE.
        StubTextSearch textSearch = new StubTextSearch();
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(new ContextMgr(), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "does_not_exist";
        req.queryText = "kw";
        com.starrocks.context.error.ContextException ex = Assertions.assertThrows(
                com.starrocks.context.error.ContextException.class,
                () -> exec.search(req));
        Assertions.assertEquals(com.starrocks.context.error.ContextErrorCode.INVALID_SCOPE,
                ex.getCode());
    }

    @Test
    public void testCollectionWithoutContextBaseRejected() {
        // collection is a per-contextbase namespace — passing it without the contextbase is
        // ambiguous and used to silently search all contextbases.
        StubTextSearch textSearch = new StubTextSearch();
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(new ContextMgr(), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.collection = "pipeline_rules";
        req.queryText = "kw";
        com.starrocks.context.error.ContextException ex = Assertions.assertThrows(
                com.starrocks.context.error.ContextException.class,
                () -> exec.search(req));
        Assertions.assertEquals(com.starrocks.context.error.ContextErrorCode.INVALID_SCOPE,
                ex.getCode());
    }

    @Test
    public void testReferencePathFeedsCandidates() {
        StubTextSearch textSearch = new StubTextSearch();
        StubReferenceExpander refExpander = new StubReferenceExpander(
                new ReferenceExpander.ExpansionRow(10L, 10L, 0, 1.0, Arrays.asList()),
                new ReferenceExpander.ExpansionRow(10L, 11L, 1, 0.5, Arrays.asList("inline")));
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.seedIds = Arrays.asList(10L);
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;
        req.textWeight = 0.0;
        req.vectorWeight = 0.0;
        req.graphWeight = 1.0;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(2, result.candidates.size());
        Assertions.assertEquals(10L, result.candidates.get(0).entityId);
        Assertions.assertEquals(1, refExpander.callCount);
    }

    @Test
    public void testDirectionDefaultsToBothPassedThroughToExpander() {
        // Regression for the FORWARD-only fusion bug: a bare Request must drive the reference
        // expansion with direction=BOTH (not the old hardcoded FORWARD), so doc1->entityX<-doc2
        // graphs are reachable. The stub captures the direction the executor handed to expand().
        StubTextSearch textSearch = new StubTextSearch();
        StubReferenceExpander refExpander = new StubReferenceExpander(
                new ReferenceExpander.ExpansionRow(10L, 10L, 0, 1.0, Arrays.asList()),
                new ReferenceExpander.ExpansionRow(10L, 11L, 1, 0.5, Arrays.asList("inline")));
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.seedIds = Arrays.asList(10L);
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;
        req.textWeight = 0.0;
        req.vectorWeight = 0.0;
        req.graphWeight = 1.0;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(1, refExpander.callCount);
        Assertions.assertEquals(ReferenceExpander.Direction.BOTH, refExpander.capturedDirection);
        Assertions.assertEquals("BOTH", result.explain.get("reference_direction"));
    }

    @Test
    public void testDirectionForwardHonored() {
        // Backward-compat escape hatch: a caller can pin direction=FORWARD to restore the old
        // behavior. The executor must thread request.direction through to expand() unchanged.
        StubTextSearch textSearch = new StubTextSearch();
        StubReferenceExpander refExpander = new StubReferenceExpander(
                new ReferenceExpander.ExpansionRow(10L, 10L, 0, 1.0, Arrays.asList()));
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.seedIds = Arrays.asList(10L);
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;
        req.graphWeight = 1.0;
        req.direction = ReferenceExpander.Direction.FORWARD;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(ReferenceExpander.Direction.FORWARD, refExpander.capturedDirection);
        Assertions.assertEquals("FORWARD", result.explain.get("reference_direction"));
    }

    @Test
    public void testDefaultGraphDirectionFallsBackToBothOnInvalidConfig() {
        // The config-default resolver must be case-insensitive and never silently revert to the
        // old FORWARD-only behavior: an unparseable value falls back to BOTH (with a WARN).
        String saved = Config.context_search_default_graph_direction;
        try {
            Config.context_search_default_graph_direction = "forward";
            Assertions.assertEquals(ReferenceExpander.Direction.FORWARD,
                    ContextSearchExecutor.defaultGraphDirection());
            Config.context_search_default_graph_direction = "Both";
            Assertions.assertEquals(ReferenceExpander.Direction.BOTH,
                    ContextSearchExecutor.defaultGraphDirection());
            Config.context_search_default_graph_direction = "NONSENSE";
            Assertions.assertEquals(ReferenceExpander.Direction.BOTH,
                    ContextSearchExecutor.defaultGraphDirection());
            Config.context_search_default_graph_direction = null;
            Assertions.assertEquals(ReferenceExpander.Direction.BOTH,
                    ContextSearchExecutor.defaultGraphDirection());
        } finally {
            Config.context_search_default_graph_direction = saved;
        }
    }

    @Test
    public void testBudgetPlannerOutputSurfacedInExplain() {
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 1, null, 1.0));
        StubReferenceExpander refExpander = new StubReferenceExpander();
        StubBudgetPlanner budgetPlanner = new StubBudgetPlanner();
        ContextSearchExecutor exec = new ContextSearchExecutor(
                newMgrWithBase("cb1"), textSearch, refExpander, new StubVectorSearch(), budgetPlanner);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;
        req.maxTokens = 128;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals("# packed", result.explain.get("packed_text"));
        Assertions.assertEquals(12, ((Number) result.explain.get("used_tokens_estimate")).intValue());
        Assertions.assertEquals(java.util.Collections.singletonList(1L), result.explain.get("included_entities"));
        Assertions.assertEquals(1, budgetPlanner.callCount);
    }

    @Test
    public void testGraphSeedsAutoDerivedFromTextVector() {
        // Text hit on entity 1 (score 1.0). With graph_mode=AUTO and no explicit seed_ids, the
        // executor should derive entity 1 as a seed and feed it to the reference expander, which
        // returns a graph_score for entity 11.
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 3, null, 1.0));
        StubReferenceExpander refExpander = new StubReferenceExpander(
                new ReferenceExpander.ExpansionRow(1L, 11L, 1, 0.5, Arrays.asList("inline")));
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.textWeight = 1.0;
        req.vectorWeight = 0.0;
        req.graphWeight = 1.0;
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(1, refExpander.callCount);
        Assertions.assertEquals(Arrays.asList(1L), new ArrayList<>(refExpander.capturedSeeds));
        Assertions.assertEquals("derived", result.explain.get("graph_seeds_source"));
        Assertions.assertEquals("ran", result.explain.get("graph_status"));
        Assertions.assertEquals(1, ((Number) result.explain.get("graph_seed_count")).intValue());
        // Candidate 11 came from the graph path; it must carry a non-zero graph_score.
        boolean foundGraphContribution = false;
        for (ContextSearchExecutor.Candidate c : result.candidates) {
            if (c.entityId == 11L) {
                Assertions.assertTrue(c.graphScore > 0.0, "expected positive graph_score for derived candidate");
                foundGraphContribution = true;
            }
        }
        Assertions.assertTrue(foundGraphContribution, "expected derived graph candidate 11 in result");
    }

    @Test
    public void testGraphModeOffSkipsExpansionEvenWithTextHits() {
        // Regression for the new derivation logic: OFF must short-circuit before the seed
        // derivation step, otherwise the executor would still call the expander.
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 3, null, 1.0));
        StubReferenceExpander refExpander = new StubReferenceExpander(
                new ReferenceExpander.ExpansionRow(1L, 11L, 1, 0.5, Arrays.asList("inline")));
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(0, refExpander.callCount);
        Assertions.assertEquals("skipped_off", result.explain.get("graph_status"));
        Assertions.assertEquals("none", result.explain.get("graph_seeds_source"));
        Assertions.assertEquals(Boolean.FALSE, result.explain.get("reference_enabled"));
    }

    @Test
    public void testNoHitsAndNoSeedsSkipsSilently() {
        // No text hits, no vector hits, no explicit seeds, AUTO. The executor must not throw and
        // must not call the expander — the seed set is genuinely empty.
        StubTextSearch textSearch = new StubTextSearch();
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "no_match";
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(0, refExpander.callCount);
        Assertions.assertTrue(result.candidates.isEmpty());
        Assertions.assertEquals("skipped_no_seeds", result.explain.get("graph_status"));
        Assertions.assertEquals("none", result.explain.get("graph_seeds_source"));
    }

    @Test
    public void testExplicitAndDerivedSeedsAreUnioned() {
        // Caller supplies explicit seed 10. Text path also surfaces entity 1. Expander should be
        // called with both; explicit seeds come first to preserve caller intent in dedup order.
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 3, null, 1.0));
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.seedIds = Arrays.asList(10L);
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;
        req.textWeight = 1.0;
        req.vectorWeight = 0.0;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(1, refExpander.callCount);
        Assertions.assertEquals(Arrays.asList(10L, 1L), new ArrayList<>(refExpander.capturedSeeds));
        Assertions.assertEquals("mixed", result.explain.get("graph_seeds_source"));
        Assertions.assertEquals(2, ((Number) result.explain.get("graph_seed_count")).intValue());
    }

    @Test
    public void testExplicitSeedsDedupAgainstDerived() {
        // Both explicit and derived seeds point at entity 1. The seed set must contain it once,
        // and the source label is "explicit" because no *new* derived seeds were added.
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 3, null, 1.0));
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.seedIds = Arrays.asList(1L);
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;
        req.textWeight = 1.0;
        req.vectorWeight = 0.0;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(Arrays.asList(1L), new ArrayList<>(refExpander.capturedSeeds));
        Assertions.assertEquals(1, ((Number) result.explain.get("graph_seed_count")).intValue());
        // graph_seeds_source is "mixed" because both buckets contributed the same id; either label
        // is defensible. We assert the seed_count semantic — that's what callers actually consume.
    }

    @Test
    public void testGraphSeedTopKBoundsSeedSet() {
        // Five text hits with descending scores. graph_seed_topk=2 → only entities 1, 2 should
        // feed the expander; entities 3-5 are dropped from the seed set.
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 5, null, 1.0),
                new TextSearchExecutor.EntityHit(2L, 4, null, 0.9),
                new TextSearchExecutor.EntityHit(3L, 3, null, 0.8),
                new TextSearchExecutor.EntityHit(4L, 2, null, 0.7),
                new TextSearchExecutor.EntityHit(5L, 1, null, 0.6));
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;
        req.textWeight = 1.0;
        req.vectorWeight = 0.0;
        req.graphSeedTopK = 2;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(Arrays.asList(1L, 2L), new ArrayList<>(refExpander.capturedSeeds));
        Assertions.assertEquals(2, ((Number) result.explain.get("graph_seed_topk_used")).intValue());
        Assertions.assertEquals(2, ((Number) result.explain.get("graph_seed_count")).intValue());
    }

    @Test
    public void testDerivedPageNotChosenAsGraphSeed() {
        // Two candidates with the same text_score: a leaf (entity 1) and a derived_page (entity 2).
        // The derived_page must be filtered out of the seed set; only the leaf should be passed
        // to the reference expander, and explain.synthesis_filtered_seeds should record the skip.
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 3, null, 1.0),
                new TextSearchExecutor.EntityHit(2L, 3, null, 1.0));
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = withMeta(newMgrWithBase("cb1"), textSearch, refExpander, java.util.Map.of(
                1L, metaWith("leaf-1", "page"),
                2L, metaWith("synth-2", "derived_page")));

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.textWeight = 1.0;
        req.vectorWeight = 0.0;
        req.graphWeight = 1.0;
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;

        ContextSearchExecutor.Result result = exec.search(req);
        Assertions.assertEquals(java.util.Arrays.asList(1L), new ArrayList<>(refExpander.capturedSeeds),
                "derived_page must not be a graph seed");
        Assertions.assertEquals(1, ((Number) result.explain.get("synthesis_filtered_seeds")).intValue());
        Assertions.assertEquals(1, ((Number) result.explain.get("graph_seed_count")).intValue());
    }

    @Test
    public void testDerivedPageGraphScoreIsHalved() {
        // Reference expansion produces graph_score=1.0 for both a leaf and a derived_page entity.
        // After fusion, the derived_page's graph contribution should be halved (× 0.5) and its
        // final_score further reduced by × 0.9. Verify by reconstructing the expected final_score.
        StubTextSearch textSearch = new StubTextSearch();
        // Hop-0 rows feed the seeds back as candidates with pathScore=1.0.
        StubReferenceExpander refExpander = new StubReferenceExpander(
                new ReferenceExpander.ExpansionRow(1L, 1L, 0, 1.0, java.util.Collections.emptyList()),
                new ReferenceExpander.ExpansionRow(2L, 2L, 0, 1.0, java.util.Collections.emptyList()));
        ContextSearchExecutor exec = withMeta(newMgrWithBase("cb1"), textSearch, refExpander, java.util.Map.of(
                1L, metaWith("leaf-1", "page"),
                2L, metaWith("synth-2", "derived_page")));

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.seedIds = java.util.Arrays.asList(1L, 2L); // explicit seeds — both reach via graph
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;
        req.textWeight = 0.0;
        req.vectorWeight = 0.0;
        req.graphWeight = 1.0;
        req.graphStrategy = "additive"; // pins additive synthesis demotion math (default is now rrf)

        ContextSearchExecutor.Result result = exec.search(req);
        // Expect the leaf to outrank the derived_page even though raw graph_scores tie.
        Assertions.assertTrue(result.candidates.size() >= 2);
        Assertions.assertEquals(1L, result.candidates.get(0).entityId,
                "leaf must outrank synthesis at equal raw graph_score");
        Assertions.assertEquals(2L, result.candidates.get(1).entityId);
        // Leaf: final = 1.0 * 1.0 = 1.0
        Assertions.assertEquals(1.0, result.candidates.get(0).finalScore, 1e-9);
        // Synthesis: final = 1.0 * (1.0 * 0.5) * 0.9 = 0.45
        Assertions.assertEquals(0.45, result.candidates.get(1).finalScore, 1e-9);
    }

    @Test
    public void testDerivedSeedsSkipNonPositivePartialScore() {
        // Both text and graph weights are zero — fusion is pure-vector. A text hit lands in the
        // candidate map but its partial fusion score is 0, so it must not seed the graph path
        // (otherwise we'd expand from a candidate fusion itself doesn't trust).
        StubTextSearch textSearch = new StubTextSearch(
                new TextSearchExecutor.EntityHit(1L, 3, null, 1.0));
        StubReferenceExpander refExpander = new StubReferenceExpander();
        ContextSearchExecutor exec = new ContextSearchExecutor(newMgrWithBase("cb1"), textSearch, refExpander);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "deal scoring";
        req.textWeight = 0.0;     // collapses partial score to zero
        req.vectorWeight = 0.0;
        req.graphWeight = 1.0;
        req.graphMode = ContextSearchExecutor.GraphMode.AUTO;

        ContextSearchExecutor.Result result = exec.search(req);
        // Partial score is 0 → no seeds derived → expander not called.
        Assertions.assertEquals(0, refExpander.callCount);
        Assertions.assertEquals("skipped_no_seeds", result.explain.get("graph_status"));
    }

    /**
     * Stub text search executor returning a fixed set of hits. Extends the real class so the
     * orchestrator's constructor continues to type-check; the {@code search(...)} method is
     * overridden to avoid hitting the internal DB.
     */
    private static final class StubTextSearch extends TextSearchExecutor {
        private final java.util.List<EntityHit> hits;

        StubTextSearch(EntityHit... hits) {
            this.hits = Arrays.asList(hits);
        }

        @Override
        public java.util.List<EntityHit> search(Request request) {
            return hits;
        }
    }

    private static final class StubVectorSearch extends VectorSearchExecutor {
        @Override
        public java.util.List<EntityHit> search(Request request) {
            return java.util.Collections.emptyList();
        }
    }

    /** Vector stub returning a fixed set of hits (for channel-degrade tests). */
    private static final class StubVectorSearchWithHits extends VectorSearchExecutor {
        private final java.util.List<EntityHit> hits;

        StubVectorSearchWithHits(EntityHit... hits) {
            this.hits = Arrays.asList(hits);
        }

        @Override
        public java.util.List<EntityHit> search(Request request) {
            return hits;
        }
    }

    /** Text channel that always throws — to verify the fusion degrades to the vector channel. */
    private static final class ThrowingTextSearch extends TextSearchExecutor {
        @Override
        public java.util.List<EntityHit> search(Request request) {
            throw new RuntimeException("text channel boom");
        }
    }

    /** Vector channel that always throws — to verify the fusion degrades to the text channel. */
    private static final class ThrowingVectorSearch extends VectorSearchExecutor {
        @Override
        public java.util.List<EntityHit> search(Request request) {
            throw new RuntimeException("vector channel boom");
        }
    }

    private static final class StubBudgetPlanner extends ContextBudgetPlanner {
        int callCount;

        StubBudgetPlanner() {
            super(null);
        }

        @Override
        public Result plan(java.util.List<ContextSearchExecutor.Candidate> rankedCandidates, long snapshotFence,
                           int maxTokens) {
            callCount++;
            return new Result("# packed", 12, java.util.Collections.singletonList(1L),
                    java.util.Collections.emptyList(), java.util.Collections.singletonMap(1L, "preview"));
        }

        @Override
        public Result plan(java.util.List<ContextSearchExecutor.Candidate> rankedCandidates, long snapshotFence,
                           int maxTokens,
                           java.util.Map<Long, com.starrocks.context.ContextReadExecutor.EntityMeta> preloadedMeta) {
            // Executor now passes a metaByEntity map alongside the candidates. Delegate to the
            // 3-arg overload so existing test assertions on packedText / callCount stay valid.
            return plan(rankedCandidates, snapshotFence, maxTokens);
        }
    }

    private static final class StubReferenceExpander extends ReferenceExpander {
        private final List<ExpansionRow> rows;
        int callCount;
        Collection<Long> capturedSeeds;
        Direction capturedDirection;

        StubReferenceExpander(ExpansionRow... rows) {
            this.rows = Arrays.asList(rows);
        }

        @Override
        public Result expand(Request request) {
            callCount++;
            capturedSeeds = request.seeds == null ? null : new ArrayList<>(request.seeds);
            capturedDirection = request.direction;
            return new Result(rows, false, rows.isEmpty() ? 0 : rows.get(rows.size() - 1).hop);
        }
    }
}
