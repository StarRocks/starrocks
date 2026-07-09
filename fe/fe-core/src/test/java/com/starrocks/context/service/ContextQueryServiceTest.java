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

import com.google.gson.JsonArray;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Guards the read-facade contract: preview disclosure must not leak the body, and scope isolation
 * (contextbase / collection) must hold for both the point read and the --history path, including
 * rejecting unresolved collection names.
 */
public class ContextQueryServiceTest {

    private static ContextReadExecutor.VersionRow row(long entityId, long cbId, long colId, String body) {
        return new ContextReadExecutor.VersionRow(entityId, 1L, "k", "doc", cbId, colId, "title", "preview",
                body, "raw-md", "{}", "{}", 0.9, "t0", "t1", "t2", 5L, false);
    }

    private static final class StubReader extends ContextReadExecutor {
        ContextReadExecutor.VersionRow current;

        @Override
        public VersionRow loadCurrentVersionRow(long entityId) {
            return current;
        }

        @Override
        public VersionRow loadVersionRow(long entityId, long version) {
            return current;
        }

        @Override
        public JsonArray getHistory(long entityId) {
            return new JsonArray();
        }

        @Override
        public JsonArray getNeighbourPreviews(long seedEntityId, long seedVersion, long snapshotFence, int max) {
            return new JsonArray();
        }

        @Override
        public JsonArray getNeighbourBodies(long seedEntityId, long seedVersion, long snapshotFence, int max) {
            return new JsonArray();
        }

        @Override
        public long resolveEntityIdByKey(String entityKey, Long contextBaseId, Long collectionId) {
            return 7L;
        }
    }

    private static final class StubMgr extends ContextMgr {
        @Override
        public ContextBaseMeta getContextBase(String name) {
            return "cbA".equals(name) ? new ContextBaseMeta(1L, "cbA", null) : null;
        }

        @Override
        public List<CollectionMeta> listCollections(String contextBase) {
            return Arrays.asList(new CollectionMeta(10L, 1L, "colA", "doc", null));
        }
    }

    @Test
    public void testPreviewLevelStripsBody() {
        StubReader reader = new StubReader();
        reader.current = row(7L, 1L, 10L, "SECRET BODY");
        ContextQueryService svc = new ContextQueryService(new StubMgr(), reader);
        ContextQueryService.ReadRequest req = new ContextQueryService.ReadRequest();
        req.id = 7L;
        req.level = ContextReadExecutor.DisclosureLevel.PREVIEW;
        ContextQueryService.ReadResult r = svc.read(req);
        assertNull(r.row.body, "preview must not return the body");
        assertNull(r.row.rawMarkdown, "preview must not return raw markdown");
        assertEquals("preview", r.row.preview, "preview field is still returned");
    }

    @Test
    public void testStandardLevelKeepsBody() {
        StubReader reader = new StubReader();
        reader.current = row(7L, 1L, 10L, "SECRET BODY");
        ContextQueryService svc = new ContextQueryService(new StubMgr(), reader);
        ContextQueryService.ReadRequest req = new ContextQueryService.ReadRequest();
        req.id = 7L;
        req.level = ContextReadExecutor.DisclosureLevel.STANDARD;
        assertEquals("SECRET BODY", svc.read(req).row.body, "standard disclosure returns the body");
    }

    @Test
    public void testHistoryEnforcesScopeBeforeReturning() {
        StubReader reader = new StubReader();
        reader.current = row(7L, 2L /* different contextbase */, 10L, "b");
        ContextQueryService svc = new ContextQueryService(new StubMgr(), reader);
        ContextQueryService.ReadRequest req = new ContextQueryService.ReadRequest();
        req.id = 7L;
        req.contextBase = "cbA";
        req.options = "--history";
        assertThrows(IllegalArgumentException.class, () -> svc.read(req),
                "history of an entity in another contextbase must be rejected");
    }

    @Test
    public void testUnresolvedCollectionRejectedOnKeyLookup() {
        ContextQueryService svc = new ContextQueryService(new StubMgr(), new StubReader());
        ContextQueryService.ReadRequest req = new ContextQueryService.ReadRequest();
        req.entityKey = "k";
        req.contextBase = "cbA";
        req.collection = "does-not-exist";
        assertThrows(IllegalArgumentException.class, () -> svc.read(req),
                "an unresolved collection name must be rejected, not fall through to any-collection");
    }
}
