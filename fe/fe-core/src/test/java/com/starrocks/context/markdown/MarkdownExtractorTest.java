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

public class MarkdownExtractorTest {

    @Test
    public void testExtractInlineRefsPreservesOrderAndDuplicates() {
        String body = "We use PKCE for public clients [[e:231]].\n"
                + "See the incident summary [[e:445]] and [[e:231]] again.";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(3, extracted.inlineRefs.size());
        Assertions.assertEquals(231L, extracted.inlineRefs.get(0).dstEntityId);
        Assertions.assertEquals(445L, extracted.inlineRefs.get(1).dstEntityId);
        Assertions.assertEquals(231L, extracted.inlineRefs.get(2).dstEntityId);
    }

    @Test
    public void testPreviewFallsBackToFirstParagraph() {
        String body = "SMB stage duration and conversion baselines.\n\n## Details\n\nMore body here.";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals("SMB stage duration and conversion baselines.", extracted.preview);
    }

    @Test
    public void testProvidedPreviewWinsOverAutoDerived() {
        String body = "auto-derived text";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, "explicit preview");
        Assertions.assertEquals("explicit preview", extracted.preview);
    }

    @Test
    public void testSectionSplittingOnHeaders() {
        String body = "Intro paragraph.\n\n## First\nfirst section body.\n\n## Second\nsecond section body.";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(3, extracted.sections.size());
        Assertions.assertTrue(extracted.sections.get(0).text.startsWith("Intro paragraph"));
        Assertions.assertTrue(extracted.sections.get(1).text.startsWith("## First"));
        Assertions.assertTrue(extracted.sections.get(2).text.startsWith("## Second"));
    }

    @Test
    public void testShortHeaderlessBodyIsSingleSection() {
        String body = "A short plain-text note with no markdown headers at all.";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(1, extracted.sections.size());
        Assertions.assertEquals(body, extracted.sections.get(0).text);
    }

    @Test
    public void testLongHeaderlessBodyIsWindowedWithOverlap() {
        // No ATX headers + > section cap: must window into multiple bounded, overlapping sections
        // instead of collapsing into one diluted whole-body fragment.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 120; i++) {
            sb.append("Line ").append(i).append(" discusses topic alpha beta gamma delta epsilon.\n");
        }
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(sb.toString(), null);

        Assertions.assertTrue(extracted.sections.size() >= 2,
                "long header-less body should be windowed, got " + extracted.sections.size());
        // Every window stays under the cap.
        for (MarkdownExtractor.Section s : extracted.sections) {
            Assertions.assertTrue(s.text.length() <= 2000,
                    "window exceeded cap: " + s.text.length());
        }
        // Consecutive windows overlap (next window starts on/before the previous window's last line).
        boolean overlaps = false;
        for (int i = 1; i < extracted.sections.size(); i++) {
            if (extracted.sections.get(i).lineStart <= extracted.sections.get(i - 1).lineEnd) {
                overlaps = true;
                break;
            }
        }
        Assertions.assertTrue(overlaps, "expected overlapping windows");
        // Ordinals stay globally sequential.
        for (int i = 0; i < extracted.sections.size(); i++) {
            Assertions.assertEquals(i, extracted.sections.get(i).ordinal);
        }
    }

    @Test
    public void testOverlongHeaderSectionIsWindowed() {
        StringBuilder sb = new StringBuilder("## Big Section\n");
        for (int i = 0; i < 120; i++) {
            sb.append("Body line ").append(i).append(" with enough words to grow the section size.\n");
        }
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(sb.toString(), null);
        // The single header section is over-long, so it splits into multiple windows.
        Assertions.assertTrue(extracted.sections.size() >= 2,
                "over-long header section should window, got " + extracted.sections.size());
    }

    @Test
    public void testFrontmatterSourceParse() {
        String body = "---\n"
                + "type: page\n"
                + "source: [201, 202, 305]\n"
                + "---\n"
                + "body goes here";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertEquals(3, extracted.sourceIds.size());
        Assertions.assertEquals(201L, extracted.sourceIds.get(0));
        Assertions.assertEquals(305L, extracted.sourceIds.get(2));
        Assertions.assertTrue(extracted.body.startsWith("body goes here"));
    }

    @Test
    public void testFrontmatterAbsentYieldsEmptySourceList() {
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract("just body", null);
        Assertions.assertTrue(extracted.sourceIds.isEmpty());
    }

    @Test
    public void testEmptyBodyIsSafe() {
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(null, null);
        Assertions.assertEquals("", extracted.body);
        Assertions.assertEquals("", extracted.preview);
        Assertions.assertTrue(extracted.sections.isEmpty() || extracted.sections.size() == 1);
    }
}
