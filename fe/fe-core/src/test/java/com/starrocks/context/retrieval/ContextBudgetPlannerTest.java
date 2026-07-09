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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ContextBudgetPlannerTest {

    @Test
    public void testPlannerTruncatesWhenOnlyOnePreviewFits() {
        StubReadExecutor reader = new StubReadExecutor();
        reader.addEntity(1L, meta(1L, 5L, 11L, "Doc One", "tiny"), row(1L, 5L, 11L, "Doc One", repeat('a', 40), "tiny"));
        reader.addEntity(2L, meta(2L, 6L, 12L, "Doc Two", "tiny"), row(2L, 6L, 12L, "Doc Two", repeat('b', 40), "tiny"));

        ContextBudgetPlanner planner = new ContextBudgetPlanner(reader);
        List<ContextSearchExecutor.Candidate> candidates = Arrays.asList(
                new ContextSearchExecutor.Candidate(1L, 1.0, 0.0, 0.0, 0, Collections.emptyList(), null),
                new ContextSearchExecutor.Candidate(2L, 0.9, 0.0, 0.0, 0, Collections.emptyList(), null));
        ContextBudgetPlanner.Result result = planner.plan(candidates, -1L, 5);

        Assertions.assertEquals(Collections.singletonList(1L), result.includedEntities);
        Assertions.assertEquals(Collections.singletonList(2L), result.truncatedEntities);
        Assertions.assertEquals("preview", result.disclosureLevels.get(1L));
    }

    @Test
    public void testPlannerUpgradesToDeepWhenBudgetAllows() {
        StubReadExecutor reader = new StubReadExecutor();
        reader.addEntity(7L, meta(7L, 3L, 9L, "Doc Seven", "preview seven"),
                row(7L, 3L, 9L, "Doc Seven", repeat('x', 64), "preview seven"));
        reader.neighbourPreviews.put(7L, neighbourRows(42L, "doc_42", "linked preview for deep mode"));

        ContextBudgetPlanner planner = new ContextBudgetPlanner(reader);
        List<ContextSearchExecutor.Candidate> candidates = Collections.singletonList(
                new ContextSearchExecutor.Candidate(7L, 1.0, 0.0, 0.0, 0, Collections.emptyList(), null));
        ContextBudgetPlanner.Result result = planner.plan(candidates, -1L, 200);

        Assertions.assertEquals(Collections.singletonList(7L), result.includedEntities);
        Assertions.assertEquals("deep", result.disclosureLevels.get(7L));
        Assertions.assertTrue(result.packedText.contains("## Linked previews"));
        Assertions.assertTrue(result.usedTokensEstimate > 0);
    }

    @Test
    public void testSynthesisUpgradeDeferredAfterLeaves() {
        // Rank 1 is a derived_page with a body large enough to consume the upgrade budget.
        // Rank 2 is a leaf with a body half the size. The budget fits exactly ONE STANDARD
        // upgrade. Without synthesis-deferral, the rank-1 derived_page would consume the
        // upgrade slot. With it, the leaf upgrades first and the derived_page stays PREVIEW.
        StubReadExecutor reader = new StubReadExecutor();
        reader.addEntity(100L, metaTyped(100L, 1L, 0L, "Synthesis", "synth-preview", "derived_page"),
                row(100L, 1L, 0L, "Synthesis", repeat('s', 200), "synth-preview"));
        reader.addEntity(200L, meta(200L, 1L, 0L, "Leaf", "leaf-preview"),
                row(200L, 1L, 0L, "Leaf", repeat('l', 200), "leaf-preview"));

        ContextBudgetPlanner planner = new ContextBudgetPlanner(reader);
        // Pre-sorted by final_score: synthesis at rank 1, leaf at rank 2.
        List<ContextSearchExecutor.Candidate> candidates = Arrays.asList(
                new ContextSearchExecutor.Candidate(100L, 0.0, 0.0, 1.0, 0, Collections.emptyList(), null),
                new ContextSearchExecutor.Candidate(200L, 0.0, 0.0, 0.9, 0, Collections.emptyList(), null));
        // 80 tokens — enough for both PREVIEWs (~ a few each) plus exactly one STANDARD upgrade.
        ContextBudgetPlanner.Result result = planner.plan(candidates, -1L, 80, null);

        Assertions.assertEquals("preview", result.disclosureLevels.get(100L),
                "synthesis must stay at PREVIEW when budget only fits one upgrade");
        Assertions.assertEquals("standard", result.disclosureLevels.get(200L),
                "leaf must take the single upgrade slot before synthesis is considered");
    }

    private static ContextReadExecutor.EntityMeta meta(long id, long version, long snapshotVersion,
                                                       String title, String preview) {
        return metaTyped(id, version, snapshotVersion, title, preview, "page");
    }

    private static ContextReadExecutor.EntityMeta metaTyped(long id, long version, long snapshotVersion,
                                                            String title, String preview, String entityType) {
        return new ContextReadExecutor.EntityMeta(id, "doc_" + id, entityType, version, snapshotVersion,
                preview, 1.0, title, null);
    }

    private static ContextReadExecutor.VersionRow row(long id, long version, long snapshotVersion,
                                                      String title, String body, String preview) {
        return new ContextReadExecutor.VersionRow(id, version, "doc_" + id, "page",
                1L, 2L, title, preview, body, null, null, null,
                1.0, "2026-04-28 10:00:00", "2026-04-28 10:00:00", "2026-04-28 10:00:00",
                snapshotVersion, false);
    }

    private static JsonArray neighbourRows(long id, String entityKey, String preview) {
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

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static final class StubReadExecutor extends ContextReadExecutor {
        private final Map<Long, EntityMeta> meta = new LinkedHashMap<>();
        private final Map<Long, VersionRow> rows = new LinkedHashMap<>();
        private final Map<Long, JsonArray> neighbourPreviews = new LinkedHashMap<>();

        private void addEntity(long entityId, EntityMeta entityMeta, VersionRow versionRow) {
            meta.put(entityId, entityMeta);
            rows.put(entityId, versionRow);
        }

        @Override
        public Map<Long, EntityMeta> loadEntityMetadata(java.util.Collection<Long> entityIds, long snapshotFence) {
            Map<Long, EntityMeta> out = new LinkedHashMap<>();
            for (Long entityId : entityIds) {
                if (meta.containsKey(entityId)) {
                    out.put(entityId, meta.get(entityId));
                }
            }
            return out;
        }

        @Override
        public VersionRow loadVersionRow(long entityId, long version) {
            return rows.get(entityId);
        }

        @Override
        public VersionRow loadCurrentVersionRow(long entityId) {
            return rows.get(entityId);
        }

        @Override
        public java.util.Map<EntityVersionKey, VersionRow> loadVersionRows(
                java.util.Collection<EntityVersionKey> keys) {
            java.util.Map<EntityVersionKey, VersionRow> out = new java.util.HashMap<>();
            for (EntityVersionKey k : keys) {
                VersionRow row = rows.get(k.entityId);
                if (row != null) {
                    out.put(k, row);
                }
            }
            return out;
        }

        @Override
        public java.util.Map<EntityVersionKey, JsonArray> getNeighbourPreviewsBulk(
                java.util.Collection<EntityVersionKey> seeds, long snapshotFence, int maxNeighboursPerSeed) {
            java.util.Map<EntityVersionKey, JsonArray> out = new java.util.HashMap<>();
            for (EntityVersionKey s : seeds) {
                JsonArray array = neighbourPreviews.get(s.entityId);
                if (array != null) {
                    out.put(s, array);
                }
            }
            return out;
        }

        @Override
        public JsonArray getNeighbourPreviews(long seedEntityId, long seedVersion, long snapshotFence, int maxNeighbours) {
            return neighbourPreviews.getOrDefault(seedEntityId, new JsonArray());
        }
    }
}
