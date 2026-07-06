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

package com.starrocks.context.markdown;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Covers the entity_key-shaped variant of {@code [[e:<X>]]} markdown refs and the matching
 * frontmatter list extensions. The legacy numeric-only path is exercised in
 * {@link MarkdownExtractorTest}; this file isolates the new shape so a regression in either
 * direction is easy to spot.
 */
public class MarkdownExtractorKeyRefsTest {

    @Test
    public void testInlineKeyRefsPopulateKeyField() {
        String body = "Deal scoring blends [[e:smb_baseline]] and [[e:enterprise_baseline]] inputs.";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(2, extracted.inlineRefs.size());
        Assertions.assertNull(extracted.inlineRefs.get(0).dstEntityId);
        Assertions.assertEquals("smb_baseline", extracted.inlineRefs.get(0).dstEntityKey);
        Assertions.assertEquals(0, extracted.inlineRefs.get(0).ord);
        Assertions.assertNull(extracted.inlineRefs.get(1).dstEntityId);
        Assertions.assertEquals("enterprise_baseline", extracted.inlineRefs.get(1).dstEntityKey);
        Assertions.assertEquals(1, extracted.inlineRefs.get(1).ord);
    }

    @Test
    public void testInlineNumericAndKeyInterleavedKeepOrd() {
        // ord must be sequential across the union; numeric and key refs share the same counter.
        String body = "First [[e:42]] then [[e:smb_baseline]] then [[e:99]] then [[e:enterprise_baseline]].";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(4, extracted.inlineRefs.size());
        Assertions.assertEquals(42L, extracted.inlineRefs.get(0).dstEntityId);
        Assertions.assertEquals("smb_baseline", extracted.inlineRefs.get(1).dstEntityKey);
        Assertions.assertEquals(99L, extracted.inlineRefs.get(2).dstEntityId);
        Assertions.assertEquals("enterprise_baseline", extracted.inlineRefs.get(3).dstEntityKey);
        Assertions.assertEquals(0, extracted.inlineRefs.get(0).ord);
        Assertions.assertEquals(1, extracted.inlineRefs.get(1).ord);
        Assertions.assertEquals(2, extracted.inlineRefs.get(2).ord);
        Assertions.assertEquals(3, extracted.inlineRefs.get(3).ord);
    }

    @Test
    public void testKeyCaseSensitive() {
        // entity_key resolution is exact-match on a VARCHAR column; markdown extraction preserves
        // case so two visually-different tokens stay distinct.
        String body = "[[e:Foo]] and [[e:foo]]";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(2, extracted.inlineRefs.size());
        Assertions.assertEquals("Foo", extracted.inlineRefs.get(0).dstEntityKey);
        Assertions.assertEquals("foo", extracted.inlineRefs.get(1).dstEntityKey);
    }

    @Test
    public void testKeyRejectsDigitLeading() {
        // Leading character must be a letter or underscore so a digit-only key cannot exist (it
        // would always be parsed as a numeric id by the regex's first alternative). `123abc` is
        // rejected by both branches: numeric branch doesn't match the `abc` suffix, key branch
        // doesn't match the leading digit. The entire bracket pair is dropped.
        String body = "[[e:123abc]] should not match.";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertTrue(extracted.inlineRefs.isEmpty());
    }

    @Test
    public void testFrontmatterScalarKeyAndId() {
        String bodyWithKey = "---\nsource: smb_baseline\n---\nbody\n";
        MarkdownExtractor.Extracted ek = MarkdownExtractor.extract(bodyWithKey, null);
        Assertions.assertTrue(ek.sourceIds.isEmpty(), "key scalar yields no numeric mirror entry");
        Assertions.assertEquals(1, ek.sourceRefs.size());
        Assertions.assertNull(ek.sourceRefs.get(0).id);
        Assertions.assertEquals("smb_baseline", ek.sourceRefs.get(0).key);

        // Legacy numeric scalar continues to populate sourceIds for back-compat consumers.
        String bodyWithId = "---\nsource: 201\n---\nbody\n";
        MarkdownExtractor.Extracted ei = MarkdownExtractor.extract(bodyWithId, null);
        Assertions.assertEquals(1, ei.sourceIds.size());
        Assertions.assertEquals(201L, ei.sourceIds.get(0));
        Assertions.assertEquals(1, ei.sourceRefs.size());
        Assertions.assertEquals(201L, ei.sourceRefs.get(0).id);
        Assertions.assertNull(ei.sourceRefs.get(0).key);
    }

    @Test
    public void testFrontmatterInlineListMixedNumericAndKey() {
        // Mixed bare tokens plus a quoted form. Quoted tokens are unwrapped before classification
        // so `"bar"` becomes the entity_key `bar`.
        String body = "---\nsource: [201, smb_baseline, \"bar\", 305]\n---\nbody\n";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(4, extracted.sourceRefs.size());
        Assertions.assertEquals(201L, extracted.sourceRefs.get(0).id);
        Assertions.assertEquals("smb_baseline", extracted.sourceRefs.get(1).key);
        Assertions.assertEquals("bar", extracted.sourceRefs.get(2).key);
        Assertions.assertEquals(305L, extracted.sourceRefs.get(3).id);
        // sourceIds keeps only the numeric tokens in original order, dropping the keys.
        Assertions.assertEquals(2, extracted.sourceIds.size());
        Assertions.assertEquals(201L, extracted.sourceIds.get(0));
        Assertions.assertEquals(305L, extracted.sourceIds.get(1));
    }

    @Test
    public void testFrontmatterBlockListMixedNumericAndKey() {
        String body = "---\nrefs:\n  - 201\n  - smb_baseline\n  - 305\n---\nbody\n";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(3, extracted.sourceRefs.size());
        Assertions.assertEquals(201L, extracted.sourceRefs.get(0).id);
        Assertions.assertEquals("smb_baseline", extracted.sourceRefs.get(1).key);
        Assertions.assertEquals(305L, extracted.sourceRefs.get(2).id);
        Assertions.assertEquals(2, extracted.sourceIds.size());
    }

    @Test
    public void testInvalidShapesAreDropped() {
        // Each malformed token should produce zero refs; the well-formed `[[e:foo]]` survives.
        String body = "[[e:]] [[e: leading_space]] [[e:has space]] [[e:foo]] [[e:99999999999999999999]]";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        // `[[e:99999...]]` matches the numeric branch but overflows Long → dropped by the parser.
        Assertions.assertEquals(1, extracted.inlineRefs.size());
        Assertions.assertEquals("foo", extracted.inlineRefs.get(0).dstEntityKey);
    }

    @Test
    public void testKeyWithDotAndColonAccepted() {
        // VARCHAR(512) entity_key allows punctuation; the regex char class covers `.`, `/`, `:`,
        // `-`, `_` so namespaced keys like `team_a.smb.baseline` or `agent:planner` parse cleanly.
        String body = "[[e:team_a.smb.baseline]] cites [[e:agent:planner]]";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(2, extracted.inlineRefs.size());
        Assertions.assertEquals("team_a.smb.baseline", extracted.inlineRefs.get(0).dstEntityKey);
        Assertions.assertEquals("agent:planner", extracted.inlineRefs.get(1).dstEntityKey);
    }
}
