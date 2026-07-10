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
import com.starrocks.context.ContextReadExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Asserts that {@link ContextPacker#pack} drops the per-id N+1 SQL pattern. For N entities the
 * packer must call the read executor exactly twice (one bulk metadata load, one bulk version-row
 * load) — independent of N. Also verifies output identity, dedup, truncation, and the
 * {@code includeCitations} flag.
 */
public class ContextPackerBulkTest {

    @Test
    public void testPackIssuesConstantNumberOfBulkLoadsRegardlessOfEntityCount() {
        // Five entities; pre-batch this would have issued 5 getCurrentById calls. The new path
        // must collapse to 1 metadata + 1 version-rows = 2 calls total.
        CountingReader reader = new CountingReader();
        for (long id = 1; id <= 5; id++) {
            reader.addEntity(id, meta(id, 3L, "Title " + id), row(id, 3L, "Title " + id, "body-" + id));
        }

        ContextPacker packer = new ContextPacker(reader);
        ContextPacker.Request req = new ContextPacker.Request();
        req.entityIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        req.maxTokens = 10_000;
        req.includeCitations = true;

        ContextPacker.Result result = packer.pack(req);

        Assertions.assertEquals(5, result.includedEntities.size());
        Assertions.assertEquals(1, reader.metadataCalls,
                "loadEntityMetadata should be called exactly once for the whole batch");
        Assertions.assertEquals(1, reader.bulkVersionCalls,
                "loadVersionRows should be called exactly once for the whole batch");
        Assertions.assertEquals(0, reader.getCurrentByIdCalls,
                "getCurrentById must not fire from pack() any more");
        Assertions.assertEquals(0, reader.singleVersionCalls,
                "single-row loadVersionRow must not fire from pack() any more");
    }

    @Test
    public void testPackOutputMatchesHandBuiltReference() {
        // Two entities packed in order — assert byte-equal output: "# Title\n\nbody" delimited
        // by "\n\n---\n\n" between entries, citations preserved.
        CountingReader reader = new CountingReader();
        reader.addEntity(7L, meta(7L, 2L, "Doc Seven"), row(7L, 2L, "Doc Seven", "body of seven"));
        reader.addEntity(9L, meta(9L, 4L, "Doc Nine"), row(9L, 4L, "Doc Nine", "body of nine"));

        ContextPacker packer = new ContextPacker(reader);
        ContextPacker.Request req = new ContextPacker.Request();
        req.entityIds = Arrays.asList(7L, 9L);
        req.maxTokens = 10_000;
        req.includeCitations = true;

        ContextPacker.Result result = packer.pack(req);

        Assertions.assertEquals("# Doc Seven\n\nbody of seven\n\n---\n\n# Doc Nine\n\nbody of nine",
                result.packedText);
        Assertions.assertEquals(Arrays.asList(7L, 9L), result.includedEntities);
        Assertions.assertEquals(2, result.citations.size());
        Assertions.assertEquals(7L, result.citations.get(0).entityId);
        Assertions.assertEquals(2L, result.citations.get(0).version);
        Assertions.assertEquals("Doc Seven", result.citations.get(0).title);
    }

    @Test
    public void testPackDedupsRepeatedEntityIds() {
        // Listing the same id twice should produce only one entry in the pack — original
        // semantics preserved by the `seen` set.
        CountingReader reader = new CountingReader();
        reader.addEntity(1L, meta(1L, 1L, "Solo"), row(1L, 1L, "Solo", "body"));

        ContextPacker packer = new ContextPacker(reader);
        ContextPacker.Request req = new ContextPacker.Request();
        req.entityIds = Arrays.asList(1L, 1L, 1L);
        req.maxTokens = 1000;
        req.includeCitations = false;

        ContextPacker.Result result = packer.pack(req);

        Assertions.assertEquals(1, result.includedEntities.size());
        Assertions.assertTrue(result.citations.isEmpty(),
                "includeCitations=false should yield an empty citations list");
    }

    @Test
    public void testTruncatesOverBudgetEntities() {
        // First entity fits, second blows the budget — second must land in truncatedEntities, not
        // included, even though it was reachable.
        CountingReader reader = new CountingReader();
        reader.addEntity(1L, meta(1L, 1L, "Small"), row(1L, 1L, "Small", "x"));
        reader.addEntity(2L, meta(2L, 1L, "Large"), row(2L, 1L, "Large", repeat('y', 4000)));

        ContextPacker packer = new ContextPacker(reader);
        ContextPacker.Request req = new ContextPacker.Request();
        req.entityIds = Arrays.asList(1L, 2L);
        req.maxTokens = 10;

        ContextPacker.Result result = packer.pack(req);

        Assertions.assertEquals(1, result.includedEntities.size());
        Assertions.assertEquals(1L, (long) result.includedEntities.get(0));
        Assertions.assertEquals(1, result.truncatedEntities.size());
        Assertions.assertEquals(2L, (long) result.truncatedEntities.get(0));
    }

    @Test
    public void testMissingMetadataAndMissingBodyAreSkipped() {
        // 1L resolves; 2L has metadata but no body row in the bulk fetch (compacted); 3L has
        // neither. Both 2L and 3L must be silently skipped — the legacy getCurrentById behavior
        // for "row not found" rather than landing in `truncatedEntities`.
        CountingReader reader = new CountingReader();
        reader.addEntity(1L, meta(1L, 1L, "ok"), row(1L, 1L, "ok", "body"));
        reader.addMetaOnly(2L, meta(2L, 5L, "no-body")); // no version row

        ContextPacker packer = new ContextPacker(reader);
        ContextPacker.Request req = new ContextPacker.Request();
        req.entityIds = Arrays.asList(1L, 2L, 3L);
        req.maxTokens = 10_000;

        ContextPacker.Result result = packer.pack(req);

        Assertions.assertEquals(1, result.includedEntities.size());
        Assertions.assertEquals(1L, (long) result.includedEntities.get(0));
        Assertions.assertTrue(result.truncatedEntities.isEmpty(),
                "missing entities should be silently skipped, not marked truncated");
    }

    private static ContextReadExecutor.EntityMeta meta(long id, long version, String title) {
        return new ContextReadExecutor.EntityMeta(id, "doc_" + id, "page", version, 100L,
                "preview-" + id, 1.0, title, null);
    }

    private static ContextReadExecutor.VersionRow row(long id, long version, String title, String body) {
        return new ContextReadExecutor.VersionRow(id, version, "doc_" + id, "page",
                1L, 2L, title, "preview-" + id, body, null, null, null,
                1.0, "2026-04-29 10:00:00", "2026-04-29 10:00:00", "2026-04-29 10:00:00",
                100L, false);
    }

    private static String repeat(char c, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Stub read executor that counts every call. Returns deterministic bulk results from in-memory
     * maps and surfaces zero from any legacy single-row method so a regression in
     * {@link ContextPacker#pack} that drops back to per-id calls would be caught immediately.
     */
    private static final class CountingReader extends ContextReadExecutor {
        private final Map<Long, EntityMeta> meta = new LinkedHashMap<>();
        private final Map<Long, VersionRow> rowsById = new LinkedHashMap<>();
        int metadataCalls;
        int bulkVersionCalls;
        int getCurrentByIdCalls;
        int singleVersionCalls;

        void addEntity(long id, EntityMeta entityMeta, VersionRow versionRow) {
            meta.put(id, entityMeta);
            rowsById.put(id, versionRow);
        }

        void addMetaOnly(long id, EntityMeta entityMeta) {
            meta.put(id, entityMeta);
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
                VersionRow row = rowsById.get(k.entityId);
                if (row != null && row.version == k.version) {
                    out.put(k, row);
                }
            }
            return out;
        }

        @Override
        public JsonArray getCurrentById(long entityId) {
            getCurrentByIdCalls++;
            return new JsonArray();
        }

        @Override
        public VersionRow loadVersionRow(long entityId, long version) {
            singleVersionCalls++;
            return rowsById.get(entityId);
        }

        @Override
        public VersionRow loadCurrentVersionRow(long entityId) {
            singleVersionCalls++;
            return rowsById.get(entityId);
        }
    }

    @SuppressWarnings("unused")
    private static List<Long> ids(long... values) {
        List<Long> out = new ArrayList<>(values.length);
        for (long v : values) {
            out.add(v);
        }
        return out;
    }
}
