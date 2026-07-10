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
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Asserts that {@link VectorSearchExecutor#search} folds fragment hits to entity hits in SQL via
 * window functions, instead of in a Java {@code bestByEntity} LinkedHashMap. The folded SQL must
 * carry the {@code (raw_score + 1.0) / 2.0} normalization, the
 * {@code ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY score DESC)} top-fragment selection,
 * the {@code WHERE entity_rank = 1} filter, and entity-level pagination via {@code LIMIT/OFFSET}.
 *
 * <p>The previous implementation pulled fragment-level rows from BE and folded them in Java —
 * O(num_fragments) memory + sort on the FE leader. The new path returns one row per entity,
 * already paginated.
 */
public class VectorSearchExecutorFoldTest {

    @Test
    public void testScopedSearchUsesScopedAnnShape() {
        // A scoped search always puts the scope as a residual predicate on the fragments scan
        // inside the inner ANN TopN (PRE filtered-ANN); the heads JOIN sits above the LIMIT and
        // carries no scope. Whether a segment has an HNSW .vi is transparent to the FE -- the BE
        // uses the index when present and brute-forces otherwise, both correct.
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        exec.search(baseRequest());
        String sql = exec.calls.get(0);
        Assertions.assertTrue(sql.contains("f.contextbase_id = 1"), sql);
        Assertions.assertTrue(sql.contains("ORDER BY score DESC LIMIT"), sql);
        Assertions.assertFalse(sql.contains("h.contextbase_id"), sql);
    }

    @Test
    public void testUnscopedSearchKeepsAnnShape() {
        // An unscoped search references no scope column; it takes the plain ANN shape.
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        VectorSearchExecutor.Request req = baseRequest();
        req.contextBaseId = null;
        exec.search(req);
        String sql = exec.calls.get(0);
        Assertions.assertTrue(sql.contains(") ann "), sql);
        Assertions.assertFalse(sql.contains("h.contextbase_id"), sql);
        Assertions.assertFalse(sql.contains("f.contextbase_id"), sql);
    }

    @Test
    public void testFoldedSqlContainsAllWindowFunctionMarkers() {
        // The fixture provides a non-null queryEmbedding so resolveQueryEmbedding short-circuits
        // (no network/LLM call); the executor builds and runs the folded SQL directly.
        StubExecutor exec = new StubExecutor();
        exec.queueRows(); // empty result is fine — we only inspect the SQL string
        VectorSearchExecutor.Request req = baseRequest();
        exec.search(req);

        String sql = exec.calls.get(0);
        Assertions.assertTrue(sql.contains("ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY score DESC)"), sql);
        Assertions.assertTrue(sql.contains("(raw_score + 1.0) / 2.0 AS vector_score"), sql);
        Assertions.assertTrue(sql.contains("WHERE entity_rank = 1"), sql);
        Assertions.assertTrue(sql.contains("ORDER BY vector_score DESC, entity_id"), sql);
    }

    @Test
    public void testDefaultSearchesBothPreviewAndSection() {
        // Default (deepMode=false, also what the fusion path uses) must query BOTH fragment kinds
        // so long docs are reachable via their section fragments, not just a truncated preview.
        // Both kinds = every kind the writer emits, so the SQL must carry NO fragment_kind
        // predicate at all: a tautological IN as a scan residual would make the BE vector
        // pre-filter read fragment_kind over the whole scan range on every query.
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        exec.search(baseRequest());
        String sql = exec.calls.get(0);
        Assertions.assertFalse(sql.contains("f.fragment_kind IN"), sql);
        Assertions.assertFalse(sql.contains("f.fragment_kind ="), sql);
    }

    @Test
    public void testDeepModeSearchesSectionOnly() {
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        VectorSearchExecutor.Request req = baseRequest();
        req.deepMode = true;
        exec.search(req);
        String sql = exec.calls.get(0);
        Assertions.assertTrue(sql.contains("f.fragment_kind = 'section'"), sql);
        Assertions.assertFalse(sql.contains("IN ('preview'"), sql);
    }

    @Test
    public void testFragmentModePreviewForcesPreviewOnly() {
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        VectorSearchExecutor.Request req = baseRequest();
        req.fragmentMode = "preview";
        exec.search(req);
        String sql = exec.calls.get(0);
        Assertions.assertTrue(sql.contains("f.fragment_kind = 'preview'"), sql);
        Assertions.assertFalse(sql.contains("IN ('preview'"), sql);
    }

    @Test
    public void testFragmentModeOverridesDeepMode() {
        // Explicit fragmentMode wins over the -d (deepMode) option: "both" drops the
        // section-only filter deepMode would otherwise add (and emits no tautological IN).
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        VectorSearchExecutor.Request req = baseRequest();
        req.deepMode = true;
        req.fragmentMode = "both";
        exec.search(req);
        String sql = exec.calls.get(0);
        Assertions.assertFalse(sql.contains("f.fragment_kind IN"), sql);
        Assertions.assertFalse(sql.contains("f.fragment_kind ="), sql);
    }

    @Test
    public void testFoldedRowsParseDirectlyIntoEntityHits() {
        // Two folded rows from the SQL come back: one per entity, already sorted by vector_score
        // descending. The Java side must just decode them in order, no further fold or sort.
        StubExecutor exec = new StubExecutor();
        // Note: column 1 here is the *normalized* vector_score (already in [0,1]) the SQL emits,
        // not raw cosine. Java code passes it straight through.
        exec.queueRows(
                row(1L, 0.9, "preview", "snippet-one"),
                row(2L, 0.5, "section", "snippet-two"));
        VectorSearchExecutor.Request req = baseRequest();

        List<VectorSearchExecutor.EntityHit> hits = exec.search(req);

        Assertions.assertEquals(2, hits.size());
        Assertions.assertEquals(1L, hits.get(0).entityId);
        Assertions.assertEquals(0.9, hits.get(0).score, 1e-9);
        Assertions.assertEquals("preview", hits.get(0).fragmentKind);
        Assertions.assertEquals("snippet-one", hits.get(0).snippet);
        Assertions.assertEquals(2L, hits.get(1).entityId);
        Assertions.assertEquals(0.5, hits.get(1).score, 1e-9);
    }

    @Test
    public void testEmptyFoldedResultYieldsEmptyList() {
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        List<VectorSearchExecutor.EntityHit> hits = exec.search(baseRequest());
        Assertions.assertTrue(hits.isEmpty());
    }

    @Test
    public void testNullScoreRowIsSkipped() {
        // A NULL raw_score bypassing the IS NOT NULL guard would be a bug; the parser still
        // defends against it.
        StubExecutor exec = new StubExecutor();
        exec.queueRows(rowWithNullScore(1L), row(2L, 0.7, "preview", "ok"));
        List<VectorSearchExecutor.EntityHit> hits = exec.search(baseRequest());
        Assertions.assertEquals(1, hits.size());
        Assertions.assertEquals(2L, hits.get(0).entityId);
    }

    @Test
    public void testPaginationIsPushedDownToSql() {
        // The folded SQL appends LIMIT <offset>, <max> at the outermost level. Verifying it's
        // present catches a regression where pagination might silently move back to Java.
        StubExecutor exec = new StubExecutor();
        exec.queueRows();
        VectorSearchExecutor.Request req = baseRequest();
        req.offset = 20;
        req.maxResults = 5;
        exec.search(req);
        String sql = exec.calls.get(0);
        Assertions.assertTrue(sql.contains("LIMIT 20, 5"), sql);
    }

    private static VectorSearchExecutor.Request baseRequest() {
        VectorSearchExecutor.Request req = new VectorSearchExecutor.Request();
        req.queryEmbedding = new float[] {0.1f, 0.2f, 0.3f};
        req.contextBaseId = 1L;
        req.maxFragmentScan = 100;
        req.maxResults = 10;
        return req;
    }

    private static JsonObject row(long entityId, double vectorScore, String fragmentKind, String snippet) {
        JsonArray data = new JsonArray();
        data.add(entityId);
        data.add(vectorScore);
        data.add(fragmentKind);
        data.add(snippet);
        JsonObject row = new JsonObject();
        row.add("data", data);
        return row;
    }

    private static JsonObject rowWithNullScore(long entityId) {
        JsonArray data = new JsonArray();
        data.add(entityId);
        data.add(com.google.gson.JsonNull.INSTANCE);
        data.add(com.google.gson.JsonNull.INSTANCE);
        data.add(com.google.gson.JsonNull.INSTANCE);
        JsonObject row = new JsonObject();
        row.add("data", data);
        return row;
    }

    /**
     * Stubs {@link VectorSearchExecutor#runQuery} so tests can serve canned folded rows without
     * needing a live cluster. Records every SQL string for SQL-shape assertions.
     */
    private static final class StubExecutor extends VectorSearchExecutor {
        private final java.util.Deque<JsonArray> responses = new java.util.ArrayDeque<>();
        final List<String> calls = new ArrayList<>();

        void queueRows(JsonObject... rows) {
            JsonArray arr = new JsonArray();
            for (JsonObject r : rows) {
                arr.add(r);
            }
            responses.add(arr);
        }

        @Override
        protected JsonArray runQuery(String sql) {
            calls.add(sql);
            JsonArray r = responses.poll();
            return r == null ? new JsonArray() : r;
        }
    }
}
