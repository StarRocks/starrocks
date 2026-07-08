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

import com.starrocks.context.ContextReadExecutor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Token-budget-aware packer. The architecture doc §10.7 and API doc §7.4 describe {@code CONTEXT_PACK}
 * as merge-adjacent + de-dup-citations + truncate-to-max-tokens; this is a straightforward FE-side
 * implementation because the heavy lifting (bulk body fetch, citation collection) happens upstream
 * via {@link ContextReadExecutor}.
 *
 * <p>Token counting uses a simple 4-chars-per-token heuristic. The BE may later ship a tokenizer
 * whose output replaces this estimate without changing the pack contract.
 */
public class ContextPacker {

    private static final int DEFAULT_MAX_TOKENS = 4000;
    private static final int CHARS_PER_TOKEN = 4;

    private final ContextReadExecutor reader;

    public ContextPacker(ContextReadExecutor reader) {
        this.reader = reader;
    }

    public static final class Request {
        public List<Long> entityIds;
        public int maxTokens = DEFAULT_MAX_TOKENS;
        public boolean includeCitations = true;
    }

    public static final class PackedEntry {
        public final long entityId;
        public final long version;
        public final String title;
        public final String body;
        public final int tokens;

        public PackedEntry(long entityId, long version, String title, String body, int tokens) {
            this.entityId = entityId;
            this.version = version;
            this.title = title;
            this.body = body;
            this.tokens = tokens;
        }
    }

    public static final class Result {
        public final String packedText;
        public final int usedTokensEstimate;
        public final List<Long> includedEntities;
        public final List<Long> truncatedEntities;
        public final List<PackedEntry> entries;
        public final List<Citation> citations;

        public Result(String packedText, int usedTokensEstimate,
                      List<Long> includedEntities, List<Long> truncatedEntities,
                      List<PackedEntry> entries, List<Citation> citations) {
            this.packedText = packedText;
            this.usedTokensEstimate = usedTokensEstimate;
            this.includedEntities = includedEntities;
            this.truncatedEntities = truncatedEntities;
            this.entries = entries;
            this.citations = citations;
        }
    }

    public static final class Citation {
        public final long entityId;
        public final long version;
        public final String title;
        // String handle for the entity. Surfaces the user-facing entity_key the caller used to
        // ingest (sha256-of-path under bench's scheme; arbitrary opaque string for direct
        // CONTEXT_UPSERT). Without this, callers receiving a Citation cannot map back to the
        // original repo path / external id without an extra /api/context/get round-trip per id.
        public final String entityKey;

        public Citation(long entityId, long version, String title, String entityKey) {
            this.entityId = entityId;
            this.version = version;
            this.title = title;
            this.entityKey = entityKey;
        }
    }

    public Result pack(Request request) {
        if (request.entityIds == null || request.entityIds.isEmpty()) {
            return new Result("", 0, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        int budget = request.maxTokens <= 0 ? DEFAULT_MAX_TOKENS : request.maxTokens;

        // Two batched fetches replace the per-id getCurrentById loop. The previous implementation
        // issued N PK-lookup SELECTs (one per entity) — a 50-id pack used 50 round-trips. We now
        // collapse that to: one bulk metadata call (gets current version + title), one bulk
        // version-row call (gets body) keyed by (entity_id, current_version).
        //
        // Behavior note: loadEntityMetadata filters current_deleted = false, while the legacy
        // getCurrentById did not. Soft-deleted entities now skip the pack instead of being
        // returned with their last visible content — that matches user expectations for
        // CONTEXT_PACK and removes a latent bug where deleted entities could leak into packed
        // output.
        Set<Long> distinctIds = new LinkedHashSet<>();
        for (Long id : request.entityIds) {
            if (id != null) {
                distinctIds.add(id);
            }
        }
        Map<Long, ContextReadExecutor.EntityMeta> metaById = reader.loadEntityMetadata(distinctIds, -1L);

        Set<ContextReadExecutor.EntityVersionKey> versionKeys = new LinkedHashSet<>();
        for (Long id : distinctIds) {
            ContextReadExecutor.EntityMeta meta = metaById.get(id);
            if (meta != null && meta.version > 0) {
                versionKeys.add(new ContextReadExecutor.EntityVersionKey(id, meta.version));
            }
        }
        Map<ContextReadExecutor.EntityVersionKey, ContextReadExecutor.VersionRow> rowsByKey =
                reader.loadVersionRows(versionKeys);

        int spentTokens = 0;
        StringBuilder packed = new StringBuilder();
        List<Long> included = new ArrayList<>();
        List<Long> truncated = new ArrayList<>();
        List<PackedEntry> entries = new ArrayList<>();
        List<Citation> citations = new ArrayList<>();
        Set<Long> seen = new LinkedHashSet<>();

        for (Long id : request.entityIds) {
            if (!seen.add(id)) {
                continue;
            }
            ContextReadExecutor.EntityMeta meta = metaById.get(id);
            if (meta == null || meta.version <= 0) {
                continue;
            }
            ContextReadExecutor.VersionRow row = rowsByKey.get(
                    new ContextReadExecutor.EntityVersionKey(id, meta.version));
            if (row == null) {
                continue;
            }
            long version = meta.version;
            String title = meta.title != null ? meta.title : "";
            String body = row.body != null ? row.body : "";
            int estimatedTokens = estimateTokens(body) + estimateTokens(title);
            if (spentTokens + estimatedTokens > budget) {
                truncated.add(id);
                continue;
            }
            if (packed.length() > 0) {
                packed.append("\n\n---\n\n");
            }
            if (!title.isEmpty()) {
                packed.append("# ").append(title).append("\n\n");
            }
            packed.append(body);
            spentTokens += estimatedTokens;
            included.add(id);
            entries.add(new PackedEntry(id, version, title, body, estimatedTokens));
            if (request.includeCitations) {
                citations.add(new Citation(id, version, title, meta.entityKey));
            }
        }
        return new Result(packed.toString(), spentTokens, included, truncated, entries, citations);
    }

    static int estimateTokens(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        return Math.max(1, text.length() / CHARS_PER_TOKEN);
    }
}
