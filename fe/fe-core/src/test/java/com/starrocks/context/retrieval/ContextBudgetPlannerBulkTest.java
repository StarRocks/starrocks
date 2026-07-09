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
import com.starrocks.context.ContextReadExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Asserts that {@link ContextBudgetPlanner#plan} drops the per-candidate N+1 reads. For N
 * candidates the planner must call the read executor at most three times (one bulk metadata
 * load, one bulk version-row load, one bulk neighbour-preview load) — independent of N.
 */
public class ContextBudgetPlannerBulkTest {

    @Test
    public void testPlanIssuesConstantNumberOfBulkLoadsRegardlessOfCandidateCount() {
        CountingReader reader = new CountingReader();
        // Five candidates with full metadata + body + linked previews. The pre-batch
        // implementation issued three SQLs *per candidate* = 15 round-trips. The new path must
        // collapse to three.
        for (long id = 1; id <= 5; id++) {
            reader.addEntity(id, meta(id, 3L, 9L, "Title " + id, "preview-" + id),
                    row(id, 3L, 9L, "Title " + id, "body-" + id, "preview-" + id));
            reader.addNeighbour(id, 3L, neighbourRow(100 + id, "neighbour-" + id, "linked preview"));
        }

        ContextBudgetPlanner planner = new ContextBudgetPlanner(reader);
        List<ContextSearchExecutor.Candidate> candidates = new ArrayList<>();
        for (long id = 1; id <= 5; id++) {
            candidates.add(new ContextSearchExecutor.Candidate(id, 1.0 - id * 0.01, 0.0, 0.0, 0,
                    Collections.emptyList(), null));
        }

        ContextBudgetPlanner.Result result = planner.plan(candidates, -1L, 10_000);

        Assertions.assertEquals(5, result.includedEntities.size());
        Assertions.assertEquals(1, reader.metadataCalls,
                "loadEntityMetadata should be called exactly once across all candidates");
        Assertions.assertEquals(1, reader.bulkVersionCalls,
                "loadVersionRows bulk fetch should fire exactly once");
        Assertions.assertEquals(1, reader.bulkNeighbourCalls,
                "getNeighbourPreviewsBulk should fire exactly once");
        // The pre-batch single-key methods must not fire from inside plan() any more.
        Assertions.assertEquals(0, reader.singleVersionCalls,
                "loadVersionRow / loadCurrentVersionRow must not be called from plan()");
        Assertions.assertEquals(0, reader.singleNeighbourCalls,
                "single-seed getNeighbourPreviews must not be called from plan()");
    }

    @Test
    public void testBulkPathPreservesPackedTextAcrossCandidates() {
        // Sanity check: the packed output for a known candidate set is identical regardless of
        // whether the planner used the bulk path. We compare against a hand-built string.
        CountingReader reader = new CountingReader();
        reader.addEntity(7L, meta(7L, 2L, 11L, "Doc Seven", "preview seven"),
                row(7L, 2L, 11L, "Doc Seven", "body of seven", "preview seven"));

        ContextBudgetPlanner planner = new ContextBudgetPlanner(reader);
        List<ContextSearchExecutor.Candidate> candidates = Collections.singletonList(
                new ContextSearchExecutor.Candidate(7L, 1.0, 0.0, 0.0, 0, Collections.emptyList(), null));
        ContextBudgetPlanner.Result result = planner.plan(candidates, -1L, 1000);

        Assertions.assertEquals(Collections.singletonList(7L), result.includedEntities);
        // No -A/-B/-C, so candidate.snippet is null → preview body is meta.preview.
        Assertions.assertTrue(result.packedText.contains("# Doc Seven"), result.packedText);
        Assertions.assertTrue(result.packedText.contains("preview seven"), result.packedText);
    }

    @Test
    public void testMissingMetadataAndMissingBodyAreTruncated() {
        // 1L resolves cleanly, 2L has metadata but no body row in the bulk fetch (compacted),
        // 3L has neither. Both 2L and 3L should land in `truncatedEntities`.
        CountingReader reader = new CountingReader();
        reader.addEntity(1L, meta(1L, 1L, 5L, "ok", "ok-preview"),
                row(1L, 1L, 5L, "ok", "ok-body", "ok-preview"));
        reader.addMetaOnly(2L, meta(2L, 1L, 5L, "no-body", "p2")); // no version row in bulk
        // 3L: not added at all

        ContextBudgetPlanner planner = new ContextBudgetPlanner(reader);
        List<ContextSearchExecutor.Candidate> candidates = new ArrayList<>();
        candidates.add(new ContextSearchExecutor.Candidate(1L, 1.0, 0.0, 0.0, 0, Collections.emptyList(), null));
        candidates.add(new ContextSearchExecutor.Candidate(2L, 0.9, 0.0, 0.0, 0, Collections.emptyList(), null));
        candidates.add(new ContextSearchExecutor.Candidate(3L, 0.8, 0.0, 0.0, 0, Collections.emptyList(), null));
        ContextBudgetPlanner.Result result = planner.plan(candidates, -1L, 10_000);

        Assertions.assertEquals(Collections.singletonList(1L), result.includedEntities);
        Assertions.assertEquals(2, result.truncatedEntities.size());
        Assertions.assertTrue(result.truncatedEntities.contains(2L));
        Assertions.assertTrue(result.truncatedEntities.contains(3L));
    }

    private static ContextReadExecutor.EntityMeta meta(long id, long version, long snapshotVersion,
                                                       String title, String preview) {
        return new ContextReadExecutor.EntityMeta(id, "doc_" + id, "page", version, snapshotVersion,
                preview, 1.0, title, null);
    }

    private static ContextReadExecutor.VersionRow row(long id, long version, long snapshotVersion,
                                                      String title, String body, String preview) {
        return new ContextReadExecutor.VersionRow(id, version, "doc_" + id, "page",
                1L, 2L, title, preview, body, null, null, null,
                1.0, "2026-04-28 10:00:00", "2026-04-28 10:00:00", "2026-04-28 10:00:00",
                snapshotVersion, false);
    }

    private static JsonArray neighbourRow(long id, String entityKey, String preview) {
        JsonArray rows = new JsonArray();
        JsonArray data = new JsonArray();
        data.add(id);
        data.add(entityKey);
        data.add(preview);
        data.add(9L);
        JsonObject row = new JsonObject();
        row.add("data", data);
        rows.add(row);
        return rows;
    }

    /**
     * Stub read executor that counts every call so the bulk-vs-N+1 contract is enforced. It
     * intentionally returns empty results from the legacy single-key paths to surface any
     * regression where {@link ContextBudgetPlanner#plan} accidentally drops back to per-candidate
     * loops.
     */
    private static final class CountingReader extends ContextReadExecutor {
        private final Map<Long, EntityMeta> meta = new LinkedHashMap<>();
        private final Map<Long, VersionRow> versionRowsById = new LinkedHashMap<>();
        private final Map<EntityVersionKey, JsonArray> neighboursByKey = new LinkedHashMap<>();
        int metadataCalls;
        int bulkVersionCalls;
        int bulkNeighbourCalls;
        int singleVersionCalls;
        int singleNeighbourCalls;

        void addEntity(long entityId, EntityMeta entityMeta, VersionRow versionRow) {
            meta.put(entityId, entityMeta);
            versionRowsById.put(entityId, versionRow);
        }

        void addMetaOnly(long entityId, EntityMeta entityMeta) {
            meta.put(entityId, entityMeta);
        }

        void addNeighbour(long entityId, long version, JsonArray rows) {
            neighboursByKey.put(new EntityVersionKey(entityId, version), rows);
        }

        @Override
        public Map<Long, EntityMeta> loadEntityMetadata(Collection<Long> entityIds, long snapshotFence) {
            metadataCalls++;
            Map<Long, EntityMeta> out = new LinkedHashMap<>();
            for (Long id : entityIds) {
                if (meta.containsKey(id)) {
                    out.put(id, meta.get(id));
                }
            }
            return out;
        }

        @Override
        public Map<EntityVersionKey, VersionRow> loadVersionRows(Collection<EntityVersionKey> keys) {
            bulkVersionCalls++;
            Map<EntityVersionKey, VersionRow> out = new HashMap<>();
            for (EntityVersionKey k : keys) {
                VersionRow row = versionRowsById.get(k.entityId);
                if (row != null && row.version == k.version) {
                    out.put(k, row);
                }
            }
            return out;
        }

        @Override
        public Map<EntityVersionKey, JsonArray> getNeighbourPreviewsBulk(Collection<EntityVersionKey> seeds,
                                                                         long snapshotFence,
                                                                         int maxNeighboursPerSeed) {
            bulkNeighbourCalls++;
            Map<EntityVersionKey, JsonArray> out = new HashMap<>();
            for (EntityVersionKey s : seeds) {
                JsonArray rows = neighboursByKey.get(s);
                if (rows != null) {
                    out.put(s, rows);
                }
            }
            return out;
        }

        @Override
        public VersionRow loadVersionRow(long entityId, long version) {
            singleVersionCalls++;
            return versionRowsById.get(entityId);
        }

        @Override
        public VersionRow loadCurrentVersionRow(long entityId) {
            singleVersionCalls++;
            return versionRowsById.get(entityId);
        }

        @Override
        public JsonArray getNeighbourPreviews(long seedEntityId, long seedVersion, long snapshotFence,
                                              int maxNeighbours) {
            singleNeighbourCalls++;
            JsonArray rows = neighboursByKey.get(new EntityVersionKey(seedEntityId, seedVersion));
            return rows == null ? new JsonArray() : rows;
        }
    }
}
