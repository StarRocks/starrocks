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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubfieldAccessPathNormalizerTest {

    // ============================================================
    // Tests for tokenizeJsonPath
    // ============================================================

    @Test
    public void testTokenizeSimplePath() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("$.a.b.c");
        assertEquals(4, tokens.size());
        assertEquals("$", tokens.get(0).value);
        assertFalse(tokens.get(0).wasQuoted);
        assertEquals("a", tokens.get(1).value);
        assertFalse(tokens.get(1).wasQuoted);
        assertEquals("b", tokens.get(2).value);
        assertEquals("c", tokens.get(3).value);
    }

    @Test
    public void testTokenizeQuotedDotField() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("$.\"a.b\".c");
        assertEquals(3, tokens.size());
        assertEquals("$", tokens.get(0).value);
        assertFalse(tokens.get(0).wasQuoted);
        assertEquals("a.b", tokens.get(1).value);
        assertTrue(tokens.get(1).wasQuoted);
        assertEquals("c", tokens.get(2).value);
        assertFalse(tokens.get(2).wasQuoted);
    }

    @Test
    public void testTokenizeQuotedBracketField() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("$.foo.\"bar[0]\".baz");
        assertEquals(4, tokens.size());
        assertEquals("$", tokens.get(0).value);
        assertEquals("foo", tokens.get(1).value);
        assertFalse(tokens.get(1).wasQuoted);
        assertEquals("bar[0]", tokens.get(2).value);
        assertTrue(tokens.get(2).wasQuoted);
        assertEquals("baz", tokens.get(3).value);
        assertFalse(tokens.get(3).wasQuoted);
    }

    @Test
    public void testTokenizeMultipleQuotedSegments() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("$.\"a.b\".\"c[1]\".d");
        assertEquals(4, tokens.size());
        assertEquals("$", tokens.get(0).value);
        assertEquals("a.b", tokens.get(1).value);
        assertTrue(tokens.get(1).wasQuoted);
        assertEquals("c[1]", tokens.get(2).value);
        assertTrue(tokens.get(2).wasQuoted);
        assertEquals("d", tokens.get(3).value);
        assertFalse(tokens.get(3).wasQuoted);
    }

    @Test
    public void testTokenizeAdjacentQuotedSegments() {
        // Two quoted segments next to each other separated by dot
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("\"a.b\".\"c.d\"");
        assertEquals(2, tokens.size());
        assertEquals("a.b", tokens.get(0).value);
        assertTrue(tokens.get(0).wasQuoted);
        assertEquals("c.d", tokens.get(1).value);
        assertTrue(tokens.get(1).wasQuoted);
    }

    @Test
    public void testTokenizeUnmatchedQuote() {
        // Unmatched quote should return empty list
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("$.\"abc");
        assertTrue(tokens.isEmpty());
    }

    @Test
    public void testTokenizeEmptyString() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("");
        assertTrue(tokens.isEmpty());
    }

    @Test
    public void testTokenizeWithoutRoot() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("a.b.c");
        assertEquals(3, tokens.size());
        assertEquals("a", tokens.get(0).value);
        assertEquals("b", tokens.get(1).value);
        assertEquals("c", tokens.get(2).value);
    }

    @Test
    public void testTokenizeSingleToken() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("abc");
        assertEquals(1, tokens.size());
        assertEquals("abc", tokens.get(0).value);
        assertFalse(tokens.get(0).wasQuoted);
    }

    @Test
    public void testTokenizeSingleQuotedToken() {
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("\"a.b\"");
        assertEquals(1, tokens.size());
        assertEquals("a.b", tokens.get(0).value);
        assertTrue(tokens.get(0).wasQuoted);
    }

    @Test
    public void testTokenizeQuotedWithSpecialChars() {
        // Quoted field with mixed special characters
        var tokens = SubfieldAccessPathNormalizer.tokenizeJsonPath("$.\"a.b[*]\".c");
        assertEquals(3, tokens.size());
        assertEquals("a.b[*]", tokens.get(1).value);
        assertTrue(tokens.get(1).wasQuoted);
    }

    // ============================================================
    // Tests for parseSimpleJsonPath
    // ============================================================

    @Test
    public void testParseSimplePath() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.a.b.c");
        assertEquals(List.of("a", "b", "c"), result);
    }

    @Test
    public void testParseQuotedDotField() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.\"a.b\".c");
        assertEquals(List.of("\"a.b\"", "c"), result);
    }

    @Test
    public void testParseQuotedBracketField() {
        // Key bug fix: previously "bar[0]" lost its quoting and was incorrectly
        // parsed as array access instead of literal field name
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.foo.\"bar[0]\".baz");
        assertEquals(List.of("foo", "\"bar[0]\"", "baz"), result);
    }

    @Test
    public void testParseQuotedComplexBracketField() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.foo.\"arr[1][2]\".next");
        assertEquals(List.of("foo", "\"arr[1][2]\"", "next"), result);
    }

    @Test
    public void testParseQuotedDotAndBracketMixed() {
        // "my.inner.term" should be kept as a single quoted field
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.foo.\"my.inner.term\".baz");
        assertEquals(List.of("foo", "\"my.inner.term\"", "baz"), result);
    }

    @Test
    public void testParseWithoutRoot() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("a.b.c");
        assertEquals(List.of("a", "b", "c"), result);
    }

    @Test
    public void testParseEmptyPath() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseDollarOnly() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseRecursivePath() {
        // ".." is not supported
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.a..b");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseUnpairedQuote() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.\"abc");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseUnquotedArrayPath() {
        // Unquoted array access should be preserved as-is (no special handling in parseSimpleJsonPath)
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.a[0].b");
        assertEquals(List.of("a[0]", "b"), result);
    }

    @Test
    public void testParseMultipleQuotedSegments() {
        List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath("$.\"a.b\".\"c[0]\".d");
        assertEquals(List.of("\"a.b\"", "\"c[0]\"", "d"), result);
    }
}
