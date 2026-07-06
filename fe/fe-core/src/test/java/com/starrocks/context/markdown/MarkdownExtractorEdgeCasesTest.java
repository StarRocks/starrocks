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
 * Tests for tricky markdown shapes that surfaced from the architecture doc examples: nested
 * single-hash headings (which should not split because they're {@code #} not {@code ##}),
 * malformed frontmatter (should be ignored gracefully), non-numeric ref ids (should be skipped
 * rather than crash the regex engine), and long body preview truncation.
 */
public class MarkdownExtractorEdgeCasesTest {

    @Test
    public void testSingleHashHeadingsDoNotSplit() {
        // Only '##' and deeper should split; a single '#' is a document title and stays in one section.
        String body = "# Title\n\nsome prose\n\nmore prose";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(1, extracted.sections.size());
    }

    @Test
    public void testMalformedFrontmatterIgnored() {
        // The frontmatter fence is never closed → the extractor should treat the whole thing as body.
        String body = "---\ntype: page\n(no closing fence)\nstill body";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertTrue(extracted.sourceIds.isEmpty());
        Assertions.assertTrue(extracted.body.contains("(no closing fence)"));
    }

    @Test
    public void testKeyAndNumericRefsCoexist() {
        // `[[e:notanumber]]` is a well-formed entity_key reference (alphanumeric, starts with a
        // letter); `[[e:]]` is malformed (empty target) and is dropped. `[[e:42]]` is numeric.
        // Both surviving refs land in inlineRefs with the appropriate shape.
        String body = "[[e:notanumber]] and [[e:]] and [[e:42]]";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(2, extracted.inlineRefs.size());
        Assertions.assertNull(extracted.inlineRefs.get(0).dstEntityId);
        Assertions.assertEquals("notanumber", extracted.inlineRefs.get(0).dstEntityKey);
        Assertions.assertEquals(42L, extracted.inlineRefs.get(1).dstEntityId);
        Assertions.assertNull(extracted.inlineRefs.get(1).dstEntityKey);
    }

    @Test
    public void testEmptyFrontmatterYieldsBodyAfterFence() {
        String body = "---\n---\nonly body\n";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertTrue(extracted.sourceIds.isEmpty());
        Assertions.assertTrue(extracted.body.contains("only body"));
    }

    @Test
    public void testPreviewTruncatedAt512Chars() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append('a');
        }
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(sb.toString(), null);
        Assertions.assertEquals(512, extracted.preview.length());
    }

    @Test
    public void testSourceLineMalformedIdsSkipped() {
        String body = "---\ntype: page\nsource: [201, notanumber, 202]\n---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(2, extracted.sourceIds.size());
        Assertions.assertEquals(201L, extracted.sourceIds.get(0));
        Assertions.assertEquals(202L, extracted.sourceIds.get(1));
    }
}
