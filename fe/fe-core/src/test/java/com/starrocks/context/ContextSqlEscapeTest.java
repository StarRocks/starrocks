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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ContextSqlEscapeTest {

    // Control bytes built from char so the source file doesn't embed raw NUL / ESC / DEL
    // bytes (some editors and grep tools choke on them, and linters flag the file as binary).
    private static String chr(int c) {
        return Character.toString((char) c);
    }

    @Test
    public void testLiteralNullReturnsKeyword() {
        Assertions.assertEquals("NULL", ContextSqlEscape.literal(null));
    }

    @Test
    public void testLiteralOrNullDistinguishesNullAndEmpty() {
        Assertions.assertEquals("NULL", ContextSqlEscape.literalOrNull(null));
        Assertions.assertEquals("NULL", ContextSqlEscape.literalOrNull(""));
        Assertions.assertEquals("'a'", ContextSqlEscape.literalOrNull("a"));
    }

    @Test
    public void testLiteralEscapesSingleQuote() {
        Assertions.assertEquals("'it''s'", ContextSqlEscape.literal("it's"));
    }

    @Test
    public void testLiteralEscapesBackslash() {
        Assertions.assertEquals("'a\\\\b'", ContextSqlEscape.literal("a\\b"));
    }

    @Test
    public void testLiteralDropsNul() {
        // NUL must be stripped — StarRocks rejects strings containing 0x00 and the previous
        // escape passed it through. Result keeps the surrounding characters but no NUL.
        Assertions.assertEquals("'ab'", ContextSqlEscape.literal("a" + chr(0x00) + "b"));
    }

    @Test
    public void testLiteralEscapesWhitespaceControls() {
        // The four common whitespace controls get readable escapes so audit-log readers can
        // grep the resulting SQL line-by-line.
        Assertions.assertEquals("'a\\nb'", ContextSqlEscape.literal("a\nb"));
        Assertions.assertEquals("'a\\rb'", ContextSqlEscape.literal("a\rb"));
        Assertions.assertEquals("'a\\tb'", ContextSqlEscape.literal("a\tb"));
    }

    @Test
    public void testLiteralEncodesOtherControlBytes() {
        // 0x01..0x08, 0x0b..0x1f and 0x7f all encode as \xNN.
        Assertions.assertEquals("'a\\x01b'", ContextSqlEscape.literal("a" + chr(0x01) + "b"));
        Assertions.assertEquals("'a\\x0bb'", ContextSqlEscape.literal("a" + chr(0x0b) + "b"));
        Assertions.assertEquals("'a\\x1fb'", ContextSqlEscape.literal("a" + chr(0x1f) + "b"));
        Assertions.assertEquals("'a\\x7fb'", ContextSqlEscape.literal("a" + chr(0x7f) + "b"));
    }

    @Test
    public void testLiteralKeepsRegularUtf8() {
        // High-ASCII / multibyte characters pass through unchanged.
        Assertions.assertEquals("'语义上下文'", ContextSqlEscape.literal("语义上下文"));
        Assertions.assertEquals("'café'", ContextSqlEscape.literal("café"));
    }

    @Test
    public void testInjectionPatternsCannotBreakOutOfLiteral() {
        // Pre-fix, the weak escape only handled "'" → "''". An input crafted to inject a comment
        // or a stacked statement after a closing quote relies on the escape leaving the quote
        // intact. With proper escaping the quote becomes '' (still inside the literal) and the
        // trailing payload becomes literal data.
        String malicious = "x'); DROP TABLE heads; --";
        String quoted = ContextSqlEscape.literal(malicious);
        Assertions.assertTrue(quoted.startsWith("'") && quoted.endsWith("'"));
        // Count of single quotes inside the body must be even (each ' → '').
        int innerQuotes = 0;
        for (int i = 1; i < quoted.length() - 1; i++) {
            if (quoted.charAt(i) == '\'') {
                innerQuotes++;
            }
        }
        Assertions.assertEquals(0, innerQuotes % 2);
    }

    @Test
    public void testBodyMatchesLiteralWithoutQuotes() {
        // body() is the un-quoted helper used by call sites that already supply their own
        // quoting (e.g. PARSE_JSON('…')). The escape rules must match literal()'s inner half.
        String input = "a\nb'c\\d";
        String literal = ContextSqlEscape.literal(input);
        String body = ContextSqlEscape.body(input);
        Assertions.assertEquals("'" + body + "'", literal);
    }

    @Test
    public void testBodyHandlesNullAndEmpty() {
        Assertions.assertEquals("", ContextSqlEscape.body(null));
        Assertions.assertEquals("", ContextSqlEscape.body(""));
    }
}
