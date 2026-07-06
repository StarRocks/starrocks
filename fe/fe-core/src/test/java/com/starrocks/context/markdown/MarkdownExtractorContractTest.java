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

public class MarkdownExtractorContractTest {

    @Test
    public void testScalarSourceFrontmatterIsCaptured() {
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract("---\nsource: 201\n---\nhello", null);
        Assertions.assertEquals(1, extracted.sourceIds.size());
        Assertions.assertEquals(201L, extracted.sourceIds.get(0));
        Assertions.assertEquals("[201]", extracted.sourceJson);
    }

    @Test
    public void testCanonicalizeRawMarkdownRebuildsFrontmatter() {
        String markdown = MarkdownExtractor.canonicalizeRawMarkdown(
                null,
                "hello",
                "{\"id\":\"301\",\"type\":\"page\"}",
                "[201,202]");
        Assertions.assertTrue(markdown.startsWith("---\n"));
        Assertions.assertTrue(markdown.contains("id: 301"));
        Assertions.assertTrue(markdown.contains("type: page"));
        Assertions.assertTrue(markdown.contains("source: [201, 202]"));
        Assertions.assertTrue(markdown.endsWith("hello"));
    }
}
