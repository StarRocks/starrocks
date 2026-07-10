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

package com.starrocks.context;

import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.context.markdown.MarkdownExtractor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for the static helpers added by Plan B (Issue #B):
 * {@link ContextWriteExecutor#validateEntityKey(String)},
 * {@link ContextWriteExecutor#collectRefKeys(MarkdownExtractor.Extracted)},
 * {@link ContextWriteExecutor#mergeRefKeyResolution(MarkdownExtractor.Extracted, Map, Map, List)}.
 *
 * <p>The end-to-end flow (single-row upsert + batch upsert against the internal tables) is
 * covered by the SQL integration test under {@code test/sql/test_semantic_context/}. These tests
 * focus on the resolution semantics so a regression in the helper logic is caught without
 * spinning a full cluster.
 */
public class ContextWriteExecutorKeyRefsTest {

    @Test
    public void testValidateEntityKeyAcceptsLetters() {
        Assertions.assertEquals("smb_baseline",
                ContextWriteExecutor.validateEntityKey("smb_baseline"));
        Assertions.assertEquals("team.smb.baseline",
                ContextWriteExecutor.validateEntityKey("team.smb.baseline"));
        Assertions.assertEquals("a1",
                ContextWriteExecutor.validateEntityKey("a1"));
    }

    @Test
    public void testValidateEntityKeyAcceptsNullAndEmpty() {
        // Anonymous (id-only) upserts pass entity_key=null / "". Validation must be a no-op so
        // we don't break the "create entity without naming it" workflow.
        Assertions.assertNull(ContextWriteExecutor.validateEntityKey(null));
        Assertions.assertEquals("", ContextWriteExecutor.validateEntityKey(""));
    }

    @Test
    public void testValidateEntityKeyRejectsDigitOnly() {
        ContextException ex = Assertions.assertThrows(ContextException.class,
                () -> ContextWriteExecutor.validateEntityKey("12345"));
        Assertions.assertEquals(ContextErrorCode.INVALID_ENTITY_KEY, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("12345"));

        // Single-digit too: still ambiguous with [[e:0]].
        Assertions.assertThrows(ContextException.class,
                () -> ContextWriteExecutor.validateEntityKey("0"));
    }

    @Test
    public void testCollectRefKeysBodyAndFrontmatter() {
        String body = "---\nsource: [foo_key, 17, bar_key]\nrefs:\n  - baz_key\n---\nbody [[e:smb_baseline]] then [[e:42]]";
        MarkdownExtractor.Extracted ex = MarkdownExtractor.extract(body, null);
        Set<String> keys = ContextWriteExecutor.collectRefKeys(ex);
        // Only the FIRST matching ref-key wins per parseSources(), so `refs:` is ignored once
        // `source:` produces a non-empty list. The body contributes smb_baseline; numeric refs
        // (42, 17) are absent from the key set since they bypass resolution.
        Assertions.assertTrue(keys.contains("smb_baseline"));
        Assertions.assertTrue(keys.contains("foo_key"));
        Assertions.assertTrue(keys.contains("bar_key"));
        Assertions.assertFalse(keys.contains("17"));
        Assertions.assertFalse(keys.contains("42"));
    }

    @Test
    public void testMergeRefKeyResolutionResolvesAllKeys() {
        String body = "Pages: [[e:foo]] and [[e:bar]]; numeric ref [[e:42]] passes through unchanged.";
        MarkdownExtractor.Extracted ex = MarkdownExtractor.extract(body, null);
        Map<String, Long> live = new HashMap<>();
        live.put("foo", 100L);
        live.put("bar", 200L);
        List<String> unresolved = new ArrayList<>();
        Map<String, Long> resolved =
                ContextWriteExecutor.mergeRefKeyResolution(ex, live, null, unresolved);
        Assertions.assertEquals(2, resolved.size());
        Assertions.assertEquals(100L, resolved.get("foo"));
        Assertions.assertEquals(200L, resolved.get("bar"));
        Assertions.assertTrue(unresolved.isEmpty());
    }

    @Test
    public void testMergeRefKeyResolutionPreferInBatchOverLive() {
        // When a key appears in BOTH maps, the in-batch entry wins so an entity created in the
        // current batch shadows a pre-existing entity with the same key (won't happen in practice
        // because entity_key reuse picks up the existing id, but the precedence rule guards
        // against future paths that bypass the reuse step).
        String body = "[[e:foo]]";
        MarkdownExtractor.Extracted ex = MarkdownExtractor.extract(body, null);
        Map<String, Long> live = new HashMap<>();
        live.put("foo", 100L);
        Map<String, Long> inBatch = new HashMap<>();
        inBatch.put("foo", 999L);
        List<String> unresolved = new ArrayList<>();
        Map<String, Long> resolved =
                ContextWriteExecutor.mergeRefKeyResolution(ex, live, inBatch, unresolved);
        Assertions.assertEquals(999L, resolved.get("foo"));
        Assertions.assertTrue(unresolved.isEmpty());
    }

    @Test
    public void testMergeRefKeyResolutionReportsUnresolved() {
        String body = "[[e:foo]] and [[e:never_existed]] and [[e:bar]]";
        MarkdownExtractor.Extracted ex = MarkdownExtractor.extract(body, null);
        Map<String, Long> live = new HashMap<>();
        live.put("foo", 100L);
        live.put("bar", 200L);
        List<String> unresolved = new ArrayList<>();
        Map<String, Long> resolved =
                ContextWriteExecutor.mergeRefKeyResolution(ex, live, null, unresolved);
        // foo and bar resolved; never_existed is in unresolved.
        Assertions.assertEquals(2, resolved.size());
        Assertions.assertEquals(1, unresolved.size());
        Assertions.assertEquals("never_existed", unresolved.get(0));
    }

    @Test
    public void testMergeRefKeyResolutionDedupesUnresolved() {
        // A body that cites the same missing key twice should report it ONCE in the unresolved
        // list so the error message stays compact.
        String body = "[[e:missing]] and again [[e:missing]] plus [[e:also_missing]]";
        MarkdownExtractor.Extracted ex = MarkdownExtractor.extract(body, null);
        List<String> unresolved = new ArrayList<>();
        ContextWriteExecutor.mergeRefKeyResolution(ex, new HashMap<>(), null, unresolved);
        Assertions.assertEquals(2, unresolved.size());
        Assertions.assertEquals("missing", unresolved.get(0));
        Assertions.assertEquals("also_missing", unresolved.get(1));
    }

    @Test
    public void testMergeRefKeyResolutionCoversFrontmatter() {
        // Frontmatter refs participate in the same resolution / unresolved-tracking flow as
        // body refs. A key referenced ONLY from frontmatter must still surface as unresolved.
        String body = "---\nsource: [orphan_key]\n---\nbody with no refs";
        MarkdownExtractor.Extracted ex = MarkdownExtractor.extract(body, null);
        List<String> unresolved = new ArrayList<>();
        ContextWriteExecutor.mergeRefKeyResolution(ex, new HashMap<>(), null, unresolved);
        Assertions.assertEquals(1, unresolved.size());
        Assertions.assertEquals("orphan_key", unresolved.get(0));
    }
}
