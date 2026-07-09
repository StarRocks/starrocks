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

package com.starrocks.context.service;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared read-side contract service for REST and SQL TVF surfaces.
 */
public class ContextQueryService {

    private final ContextMgr contextMgr;
    private final ContextReadExecutor reader;

    public ContextQueryService(ContextMgr contextMgr, ContextReadExecutor reader) {
        this.contextMgr = contextMgr;
        this.reader = reader;
    }

    public static final class ReadRequest {
        public Long id;
        public String entityKey;
        public String contextBase;
        public String collection;
        public Long version;
        public String asOfTime;
        public ContextReadExecutor.DisclosureLevel level = ContextReadExecutor.DisclosureLevel.STANDARD;
        public int neighborLimit = 16;
        public String options;
    }

    public static final class ReadResult {
        public final ContextReadExecutor.VersionRow row;
        public final JsonArray historyRows;
        public final List<String> selectedLines;
        public final JsonArray neighbourPreviews;
        public final JsonArray neighbourBodies;
        public final long resolvedVersion;

        public ReadResult(ContextReadExecutor.VersionRow row, JsonArray historyRows, List<String> selectedLines,
                          JsonArray neighbourPreviews, JsonArray neighbourBodies, long resolvedVersion) {
            this.row = row;
            this.historyRows = historyRows;
            this.selectedLines = selectedLines;
            this.neighbourPreviews = neighbourPreviews;
            this.neighbourBodies = neighbourBodies;
            this.resolvedVersion = resolvedVersion;
        }
    }

    public ReadResult read(ReadRequest request) {
        long entityId = resolveEntityId(request);
        if (entityId <= 0) {
            return new ReadResult(null, new JsonArray(), Collections.emptyList(), new JsonArray(), new JsonArray(), -1L);
        }
        if ("--history".equals(request.options)) {
            // Scope isolation must run BEFORE exposing history: entity_id is global, so an id-based
            // request scoped to a contextbase/collection could otherwise dump another scope's full
            // version history. Load the entity's current row and validate it against the scope first.
            if (!Strings.isNullOrEmpty(request.contextBase)) {
                ContextReadExecutor.VersionRow scopeRow = reader.loadCurrentVersionRow(entityId);
                if (scopeRow == null) {
                    return new ReadResult(null, new JsonArray(), Collections.emptyList(),
                            new JsonArray(), new JsonArray(), -1L);
                }
                validateScope(scopeRow, request);
            }
            return new ReadResult(null, reader.getHistory(entityId), Collections.emptyList(),
                    new JsonArray(), new JsonArray(), -1L);
        }

        ContextReadExecutor.VersionRow row;
        long resolvedVersion;
        if (request.version != null) {
            resolvedVersion = request.version;
            row = reader.loadVersionRow(entityId, request.version);
        } else if (!Strings.isNullOrEmpty(request.asOfTime)) {
            resolvedVersion = reader.resolveVersionAsOf(entityId, request.asOfTime);
            row = resolvedVersion > 0 ? reader.loadVersionRow(entityId, resolvedVersion) : null;
        } else {
            row = reader.loadCurrentVersionRow(entityId);
            resolvedVersion = row == null ? -1L : row.version;
        }
        if (row == null) {
            return new ReadResult(null, new JsonArray(), Collections.emptyList(),
                    new JsonArray(), new JsonArray(), resolvedVersion);
        }

        // Scope check: when the caller pinned a contextbase (and optionally a collection), verify
        // the loaded entity belongs to it. entity_id is globally unique while entity_key is unique
        // only per (contextbase, collection), so the loader has no inherent scope filter and an id
        // from another scope would otherwise be readable here. Spec: API §10.4 / §11 isolation.
        validateScope(row, request);

        // PREVIEW disclosure returns only head + preview fields, never the body. The loaders always
        // hydrate the full version row, so redact the content columns before returning (and before
        // line-selection, which reads the body) so a preview read cannot leak full content.
        if (request.level == ContextReadExecutor.DisclosureLevel.PREVIEW) {
            row = previewShaped(row);
        }

        List<String> selectedLines = parseLineSelection(row.body, request.options);
        JsonArray neighbourPreviews = new JsonArray();
        JsonArray neighbourBodies = new JsonArray();
        if (request.level == ContextReadExecutor.DisclosureLevel.STANDARD
                || request.level == ContextReadExecutor.DisclosureLevel.DEEP) {
            neighbourPreviews = reader.getNeighbourPreviews(
                    row.entityId, row.version, row.snapshotVersion, request.neighborLimit);
        }
        if (request.level == ContextReadExecutor.DisclosureLevel.DEEP) {
            neighbourBodies = reader.getNeighbourBodies(
                    row.entityId, row.version, row.snapshotVersion, request.neighborLimit);
        }
        return new ReadResult(row, new JsonArray(), selectedLines, neighbourPreviews, neighbourBodies, resolvedVersion);
    }

    private long resolveEntityId(ReadRequest request) {
        if (request.id != null && request.id > 0) {
            return request.id;
        }
        if (Strings.isNullOrEmpty(request.entityKey)) {
            return -1L;
        }
        // entity_key is unique per (contextbase, collection), not globally. Without scope the
        // lookup is ambiguous — letting it fall through to a global resolveEntityIdByKey would
        // silently return whichever entity_id happened to match first, leaking the wrong base's
        // row. Spec: API §10.4 — "scope is required when looking up by entity_key".
        if (Strings.isNullOrEmpty(request.contextBase)) {
            throw new IllegalArgumentException(
                    "\"contextbase\" is required when looking up by entity_key");
        }
        ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(request.contextBase);
        if (cb == null) {
            throw new IllegalArgumentException("contextbase not found: " + request.contextBase);
        }
        Long contextBaseId = cb.getId();
        Long collectionId = null;
        if (!Strings.isNullOrEmpty(request.collection)) {
            // Reject an unresolved collection name rather than falling through with a null filter:
            // entity keys are unique only per (contextbase, collection), so a null collection would
            // let resolveEntityIdByKey return a different collection's entity on a typo.
            collectionId = resolveCollectionId(request.contextBase, request.collection);
        }
        return reader.resolveEntityIdByKey(request.entityKey, contextBaseId, collectionId);
    }

    // Verify the loaded entity belongs to the caller's pinned contextbase / collection, throwing on
    // any mismatch or unknown scope name. A request with no contextbase is unscoped and passes.
    private void validateScope(ContextReadExecutor.VersionRow row, ReadRequest request) {
        if (Strings.isNullOrEmpty(request.contextBase)) {
            return;
        }
        ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(request.contextBase);
        if (cb == null) {
            throw new IllegalArgumentException("contextbase not found: " + request.contextBase);
        }
        if (row.contextBaseId != cb.getId()) {
            throw new IllegalArgumentException("entity " + row.entityId
                    + " does not belong to contextbase " + request.contextBase);
        }
        if (!Strings.isNullOrEmpty(request.collection)) {
            long colId = resolveCollectionId(request.contextBase, request.collection);
            if (row.collectionId != colId) {
                throw new IllegalArgumentException("entity " + row.entityId
                        + " does not belong to collection " + request.contextBase + "." + request.collection);
            }
        }
    }

    // Resolve a collection name within a contextbase to its id, throwing when the name does not
    // exist so callers never silently degrade to an unscoped (any-collection) lookup.
    private long resolveCollectionId(String contextBase, String collection) {
        for (ContextMgr.CollectionMeta col : contextMgr.listCollections(contextBase)) {
            if (collection.equals(col.getName())) {
                return col.getId();
            }
        }
        throw new IllegalArgumentException("collection not found: " + contextBase + "." + collection);
    }

    // PREVIEW-shaped copy: keep the head + preview metadata but drop the full-content columns
    // (body, raw_markdown, frontmatter_json, source_json) so a preview disclosure never returns
    // the body. The loaders have no preview-only variant, so we redact after loading.
    private static ContextReadExecutor.VersionRow previewShaped(ContextReadExecutor.VersionRow row) {
        return new ContextReadExecutor.VersionRow(
                row.entityId, row.version, row.entityKey, row.entityType, row.contextBaseId, row.collectionId,
                row.title, row.preview, null, null, null, null,
                row.confidence, row.createdTime, row.updatedTime, row.commitTime, row.snapshotVersion, row.deleted);
    }

    static List<String> parseLineSelection(String markdown, String options) {
        if (Strings.isNullOrEmpty(options) || !options.startsWith("-L")) {
            return Collections.emptyList();
        }
        String selector = options.substring(2);
        String[] lines = markdown == null ? new String[0] : markdown.split("\n", -1);
        int start;
        int end;
        if (selector.startsWith("-")) {
            int tail = parsePositiveInt(selector.substring(1));
            start = Math.max(1, lines.length - tail + 1);
            end = lines.length;
        } else {
            String[] parts = selector.split("-", 2);
            start = parsePositiveInt(parts[0]);
            end = parts.length > 1 ? parsePositiveInt(parts[1]) : start;
        }
        List<String> out = new ArrayList<>();
        for (int i = Math.max(1, start); i <= Math.min(lines.length, end); i++) {
            out.add(lines[i - 1]);
        }
        return out;
    }

    private static int parsePositiveInt(String raw) {
        try {
            return Math.max(1, Integer.parseInt(raw));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid line selector: " + raw);
        }
    }
}
