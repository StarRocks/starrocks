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
import com.google.gson.JsonElement;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.policy.CollectionTypePolicy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Budget-aware selector for {@link ContextSearchExecutor}. It starts broad with preview-level
 * coverage, then upgrades higher-ranked candidates to richer disclosure levels while staying within
 * the caller's token budget.
 */
public class ContextBudgetPlanner {

    private static final int DEFAULT_NEIGHBOUR_LIMIT = 4;

    private final ContextReadExecutor reader;

    public ContextBudgetPlanner(ContextReadExecutor reader) {
        this.reader = reader;
    }

    public enum DisclosureLevel {
        PREVIEW,
        STANDARD,
        DEEP
    }

    public static final class Result {
        public final String packedText;
        public final int usedTokensEstimate;
        public final List<Long> includedEntities;
        public final List<Long> truncatedEntities;
        public final Map<Long, String> disclosureLevels;

        public Result(String packedText, int usedTokensEstimate, List<Long> includedEntities,
                      List<Long> truncatedEntities, Map<Long, String> disclosureLevels) {
            this.packedText = packedText;
            this.usedTokensEstimate = usedTokensEstimate;
            this.includedEntities = includedEntities;
            this.truncatedEntities = truncatedEntities;
            this.disclosureLevels = disclosureLevels;
        }
    }

    public Result plan(List<ContextSearchExecutor.Candidate> rankedCandidates, long snapshotFence, int maxTokens) {
        return plan(rankedCandidates, snapshotFence, maxTokens, null);
    }

    /**
     * Synthesis-aware overload: the caller supplies pre-loaded entity metadata so we can both
     * skip the redundant {@link ContextReadExecutor#loadEntityMetadata} call inside
     * {@link #loadBulkData} and apply the synthesis-deferred upgrade pass — leaves get
     * STANDARD/DEEP upgrades before any synthesis entity is lifted past PREVIEW. Without this,
     * a single derived_page body can monopolise the token budget and starve the leaf evidence
     * the agent needs to ground its answer.
     */
    public Result plan(List<ContextSearchExecutor.Candidate> rankedCandidates, long snapshotFence, int maxTokens,
                       Map<Long, ContextReadExecutor.EntityMeta> preloadedMeta) {
        if (reader == null || rankedCandidates == null || rankedCandidates.isEmpty() || maxTokens <= 0) {
            return empty();
        }

        // Three batched fetches replace what used to be 3*N round-trips inside a per-candidate
        // prepare() loop. Order matters: metadata gives us each candidate's current version,
        // which then keys both the bulk body fetch and the bulk neighbour-preview fetch.
        BulkData bulk = loadBulkData(rankedCandidates, snapshotFence, preloadedMeta);

        List<PreparedCandidate> prepared = new ArrayList<>();
        List<Long> truncated = new ArrayList<>();
        for (ContextSearchExecutor.Candidate candidate : rankedCandidates) {
            PreparedCandidate preparedCandidate = prepareFromBulk(candidate, bulk);
            if (preparedCandidate == null || preparedCandidate.previewTokens > maxTokens) {
                truncated.add(candidate.entityId);
                continue;
            }
            prepared.add(preparedCandidate);
        }
        if (prepared.isEmpty()) {
            return new Result("", 0, new ArrayList<>(), truncated, new LinkedHashMap<>());
        }

        int spentTokens = 0;
        List<PreparedCandidate> included = new ArrayList<>();
        for (PreparedCandidate candidate : prepared) {
            if (spentTokens + candidate.previewTokens > maxTokens) {
                truncated.add(candidate.entityId);
                continue;
            }
            candidate.selectedLevel = DisclosureLevel.PREVIEW;
            spentTokens += candidate.previewTokens;
            included.add(candidate);
        }

        // Two-pass upgrade: leaves first, synthesis second. The same DEEP-then-STANDARD logic
        // applies in each pass; only the iteration order changes. This guarantees that for any
        // synthesis entity that ends up in `included`, every leaf above it (and every leaf below
        // it that fits in the budget) is upgraded first — agents always see leaf evidence
        // alongside synthesis bodies, which prevents the "I read the synthesis, I'm done"
        // failure mode that was costing eval grounding.
        int[] remainingHolder = new int[] {maxTokens - spentTokens};
        int[] spentHolder = new int[] {spentTokens};
        runUpgradePass(included, bulk, remainingHolder, spentHolder, /*synthesisPass=*/ false);
        runUpgradePass(included, bulk, remainingHolder, spentHolder, /*synthesisPass=*/ true);
        spentTokens = spentHolder[0];

        StringBuilder packed = new StringBuilder();
        List<Long> includedIds = new ArrayList<>();
        Map<Long, String> disclosureLevels = new LinkedHashMap<>();
        for (PreparedCandidate candidate : included) {
            if (packed.length() > 0) {
                packed.append("\n\n---\n\n");
            }
            packed.append(candidate.renderedText());
            includedIds.add(candidate.entityId);
            disclosureLevels.put(candidate.entityId, candidate.selectedLevel.name().toLowerCase(java.util.Locale.ROOT));
        }
        return new Result(packed.toString(), spentTokens, includedIds, truncated, disclosureLevels);
    }

    /**
     * Iterate {@code included} in rank order, upgrading each candidate from PREVIEW toward
     * STANDARD or DEEP within the remaining budget. The {@code synthesisPass} flag selects which
     * subset to consider: {@code false} only touches non-synthesis entities (leaves), {@code true}
     * only touches synthesis entities. Two calls in sequence (leaves then synthesis) produce the
     * leaf-first upgrade ordering.
     */
    private void runUpgradePass(List<PreparedCandidate> included, BulkData bulk,
                                int[] remainingHolder, int[] spentHolder, boolean synthesisPass) {
        int remaining = remainingHolder[0];
        int spent = spentHolder[0];
        for (PreparedCandidate candidate : included) {
            if (candidate.selectedLevel != DisclosureLevel.PREVIEW) {
                continue; // already upgraded in a prior pass
            }
            ContextReadExecutor.EntityMeta meta = bulk.metaById.get(candidate.entityId);
            boolean isSynthesis = meta != null
                    && CollectionTypePolicy.isSynthesisType(meta.entityType);
            if (isSynthesis != synthesisPass) {
                continue;
            }
            // Lazy: evaluate deep tokens only when the candidate has neighbours; evaluate standard
            // tokens only after the deep upgrade is rejected. Keeps prepareFromBulk cheap for the
            // many candidates that end up emitted at PREVIEW level.
            if (candidate.hasDeepVariant()) {
                int deepDelta = candidate.deepTokens() - candidate.previewTokens;
                if (deepDelta > 0 && deepDelta <= remaining) {
                    candidate.selectedLevel = DisclosureLevel.DEEP;
                    remaining -= deepDelta;
                    spent += deepDelta;
                    continue;
                }
            }
            int standardDelta = candidate.standardTokens() - candidate.previewTokens;
            if (standardDelta > 0 && standardDelta <= remaining) {
                candidate.selectedLevel = DisclosureLevel.STANDARD;
                remaining -= standardDelta;
                spent += standardDelta;
            }
        }
        remainingHolder[0] = remaining;
        spentHolder[0] = spent;
    }

    /**
     * One-shot fetch for everything {@link #plan} needs across a candidate set: metadata for all
     * candidates, full version rows for the visible version of each, and per-seed neighbour
     * previews. Three SQL round-trips total regardless of candidate count, replacing the prior
     * O(N) {@code loadEntityMetadata + loadVersionRow + getNeighbourPreviews} chain.
     *
     * <p>When {@code preloadedMeta} is supplied (the executor already loaded it), we skip the
     * metadata round-trip entirely. Any candidates not covered by the preloaded map are fetched
     * separately so this remains correct for partial inputs.
     */
    private BulkData loadBulkData(List<ContextSearchExecutor.Candidate> candidates, long snapshotFence,
                                  Map<Long, ContextReadExecutor.EntityMeta> preloadedMeta) {
        List<Long> ids = new ArrayList<>(candidates.size());
        for (ContextSearchExecutor.Candidate c : candidates) {
            ids.add(c.entityId);
        }
        Map<Long, ContextReadExecutor.EntityMeta> metaById;
        if (preloadedMeta == null) {
            metaById = reader.loadEntityMetadata(ids, snapshotFence);
        } else {
            metaById = new java.util.HashMap<>(preloadedMeta);
            List<Long> missing = new ArrayList<>();
            for (Long id : ids) {
                if (!metaById.containsKey(id)) {
                    missing.add(id);
                }
            }
            if (!missing.isEmpty()) {
                metaById.putAll(reader.loadEntityMetadata(missing, snapshotFence));
            }
        }

        // Use a LinkedHashSet to preserve candidate order in the bulk SQL — purely cosmetic for
        // logging, but it keeps reproducible test fixtures.
        Set<ContextReadExecutor.EntityVersionKey> versionKeys = new LinkedHashSet<>();
        for (ContextSearchExecutor.Candidate c : candidates) {
            ContextReadExecutor.EntityMeta m = metaById.get(c.entityId);
            if (m != null && m.version > 0) {
                versionKeys.add(new ContextReadExecutor.EntityVersionKey(c.entityId, m.version));
            }
        }
        Map<ContextReadExecutor.EntityVersionKey, ContextReadExecutor.VersionRow> rowsByKey =
                loadVersionRowsBulk(versionKeys);

        Set<ContextReadExecutor.EntityVersionKey> neighbourSeeds = new LinkedHashSet<>(rowsByKey.keySet());
        Map<ContextReadExecutor.EntityVersionKey, JsonArray> neighboursByKey =
                loadNeighbourPreviewsBulk(neighbourSeeds, snapshotFence);

        return new BulkData(metaById, rowsByKey, neighboursByKey);
    }

    /**
     * Hook so tests can stub the bulk fetch without spinning up a real SQL plane. Default
     * implementation forwards to {@link ContextReadExecutor#loadVersionRows}.
     */
    protected Map<ContextReadExecutor.EntityVersionKey, ContextReadExecutor.VersionRow> loadVersionRowsBulk(
            Collection<ContextReadExecutor.EntityVersionKey> keys) {
        return reader.loadVersionRows(keys);
    }

    /**
     * Hook so tests can stub the bulk fetch. Default forwards to
     * {@link ContextReadExecutor#getNeighbourPreviewsBulk}.
     */
    protected Map<ContextReadExecutor.EntityVersionKey, JsonArray> loadNeighbourPreviewsBulk(
            Collection<ContextReadExecutor.EntityVersionKey> seeds, long snapshotFence) {
        return reader.getNeighbourPreviewsBulk(seeds, snapshotFence, DEFAULT_NEIGHBOUR_LIMIT);
    }

    private PreparedCandidate prepareFromBulk(ContextSearchExecutor.Candidate candidate, BulkData bulk) {
        ContextReadExecutor.EntityMeta meta = bulk.metaById.get(candidate.entityId);
        if (meta == null) {
            return null;
        }
        ContextReadExecutor.EntityVersionKey key = meta.version > 0
                ? new ContextReadExecutor.EntityVersionKey(candidate.entityId, meta.version)
                : null;
        ContextReadExecutor.VersionRow row = key == null ? null : bulk.rowsByKey.get(key);
        if (row == null) {
            return null;
        }
        JsonArray neighbours = bulk.neighboursByKey.getOrDefault(
                new ContextReadExecutor.EntityVersionKey(row.entityId, row.version), new JsonArray());

        String previewText = renderPreview(meta.title, candidate.snippet, meta.preview);
        // standard / deep render is deferred: most candidates end up emitted at PREVIEW (or
        // truncated by budget) and never need them. The supplier closure captures the inputs;
        // PreparedCandidate memoizes on first access so token-estimate-then-render only walks
        // the body once.
        String title = meta.title;
        String body = row.body;
        String preview = meta.preview;
        Supplier<String> standardSupplier = () -> renderStandard(title, body, preview);
        Supplier<String> deepSupplier = () -> renderDeep(title, body, preview, neighbours);
        return new PreparedCandidate(
                candidate.entityId,
                previewText,
                standardSupplier,
                deepSupplier,
                hasValidNeighbour(neighbours),
                ContextPacker.estimateTokens(previewText));
    }

    /**
     * Single-candidate prepare retained for direct callers that don't go through {@link #plan}
     * (none today, but the method is {@code protected} so subclasses may have overridden it).
     * Internal scheduling no longer routes through this — {@link #plan} batches everything.
     */
    protected PreparedCandidate prepare(ContextSearchExecutor.Candidate candidate, long snapshotFence) {
        ContextReadExecutor.EntityMeta meta = reader.loadEntityMetadata(
                java.util.Collections.singletonList(candidate.entityId), snapshotFence).get(candidate.entityId);
        if (meta == null) {
            return null;
        }
        ContextReadExecutor.VersionRow row = meta.version > 0
                ? reader.loadVersionRow(candidate.entityId, meta.version)
                : reader.loadCurrentVersionRow(candidate.entityId);
        if (row == null) {
            return null;
        }
        JsonArray neighbours = reader.getNeighbourPreviews(row.entityId, row.version, row.snapshotVersion,
                DEFAULT_NEIGHBOUR_LIMIT);
        String previewText = renderPreview(meta.title, candidate.snippet, meta.preview);
        String title = meta.title;
        String body = row.body;
        String preview = meta.preview;
        Supplier<String> standardSupplier = () -> renderStandard(title, body, preview);
        Supplier<String> deepSupplier = () -> renderDeep(title, body, preview, neighbours);
        return new PreparedCandidate(
                candidate.entityId,
                previewText,
                standardSupplier,
                deepSupplier,
                hasValidNeighbour(neighbours),
                ContextPacker.estimateTokens(previewText));
    }

    /**
     * Mirrors the predicate inside {@link #renderDeep} that decides whether to append the
     * "Linked previews" block: a neighbour row is valid when its data array has at least 3
     * fields and the preview field is a non-empty string. Computing this once up front lets
     * the planner skip evaluating deep tokens entirely for candidates whose deep variant is
     * structurally identical to the standard one.
     */
    private static boolean hasValidNeighbour(JsonArray neighbourPreviews) {
        if (neighbourPreviews == null) {
            return false;
        }
        for (JsonElement row : neighbourPreviews) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            if (data.size() < 3 || data.get(2).isJsonNull()) {
                continue;
            }
            String np = data.get(2).getAsString();
            if (np != null && !np.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private String renderPreview(String title, String snippet, String preview) {
        StringBuilder out = new StringBuilder();
        if (title != null && !title.isEmpty()) {
            out.append("# ").append(title).append("\n\n");
        }
        String body = firstNonEmpty(snippet, preview, "");
        out.append(body);
        return out.toString();
    }

    private String renderStandard(String title, String body, String preview) {
        StringBuilder out = new StringBuilder();
        if (title != null && !title.isEmpty()) {
            out.append("# ").append(title).append("\n\n");
        }
        out.append(firstNonEmpty(body, preview, ""));
        return out.toString();
    }

    private String renderDeep(String title, String body, String preview, JsonArray neighbourPreviews) {
        StringBuilder out = new StringBuilder(renderStandard(title, body, preview));
        List<String> neighbours = new ArrayList<>();
        for (JsonElement row : neighbourPreviews) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            if (data.size() < 3) {
                continue;
            }
            String neighbourPreview = data.get(2).isJsonNull() ? null : data.get(2).getAsString();
            if (neighbourPreview == null || neighbourPreview.isEmpty()) {
                continue;
            }
            String entityKey = data.get(1).isJsonNull() ? null : data.get(1).getAsString();
            long entityId = data.get(0).isJsonNull() ? -1L : data.get(0).getAsLong();
            neighbours.add("- [[e:" + entityId + "]] " + firstNonEmpty(entityKey, neighbourPreview)
                    + ": " + neighbourPreview);
        }
        if (!neighbours.isEmpty()) {
            out.append("\n\n## Linked previews\n");
            for (int i = 0; i < neighbours.size(); i++) {
                if (i > 0) {
                    out.append('\n');
                }
                out.append(neighbours.get(i));
            }
        }
        return out.toString();
    }

    private String firstNonEmpty(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    private Result empty() {
        return new Result("", 0, new ArrayList<>(), new ArrayList<>(), new LinkedHashMap<>());
    }

    /**
     * Snapshot of the three batched fetches that {@link #plan} relies on, materialised once per
     * call. Held privately so the per-candidate {@link #prepareFromBulk} loop only does
     * map-lookups, no SQL.
     */
    private static final class BulkData {
        final Map<Long, ContextReadExecutor.EntityMeta> metaById;
        final Map<ContextReadExecutor.EntityVersionKey, ContextReadExecutor.VersionRow> rowsByKey;
        final Map<ContextReadExecutor.EntityVersionKey, JsonArray> neighboursByKey;

        BulkData(Map<Long, ContextReadExecutor.EntityMeta> metaById,
                 Map<ContextReadExecutor.EntityVersionKey, ContextReadExecutor.VersionRow> rowsByKey,
                 Map<ContextReadExecutor.EntityVersionKey, JsonArray> neighboursByKey) {
            this.metaById = metaById;
            this.rowsByKey = rowsByKey;
            this.neighboursByKey = neighboursByKey;
        }
    }

    protected static final class PreparedCandidate {
        private final long entityId;
        private final String previewText;
        private final Supplier<String> standardSupplier;
        private final Supplier<String> deepSupplier;
        private final boolean hasNeighbours;
        private final int previewTokens;
        private String standardText;
        private String deepText;
        private int standardTokens = -1;
        private int deepTokens = -1;
        private DisclosureLevel selectedLevel = DisclosureLevel.PREVIEW;

        private PreparedCandidate(long entityId, String previewText,
                                  Supplier<String> standardSupplier, Supplier<String> deepSupplier,
                                  boolean hasNeighbours, int previewTokens) {
            this.entityId = entityId;
            this.previewText = previewText;
            this.standardSupplier = standardSupplier;
            this.deepSupplier = deepSupplier;
            this.hasNeighbours = hasNeighbours;
            this.previewTokens = previewTokens;
        }

        private String standardText() {
            if (standardText == null) {
                standardText = standardSupplier.get();
            }
            return standardText;
        }

        private String deepText() {
            if (deepText == null) {
                deepText = deepSupplier.get();
            }
            return deepText;
        }

        private int standardTokens() {
            if (standardTokens < 0) {
                standardTokens = ContextPacker.estimateTokens(standardText());
            }
            return standardTokens;
        }

        private int deepTokens() {
            if (deepTokens < 0) {
                deepTokens = ContextPacker.estimateTokens(deepText());
            }
            return deepTokens;
        }

        private boolean hasDeepVariant() {
            return hasNeighbours;
        }

        private String renderedText() {
            switch (selectedLevel) {
                case DEEP:
                    return deepText();
                case STANDARD:
                    return standardText();
                case PREVIEW:
                default:
                    return previewText;
            }
        }
    }
}
