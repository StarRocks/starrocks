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

package com.starrocks.sql.parser;

import com.starrocks.analysis.MatchExpr;
import com.starrocks.analysis.MatchExpr.PhrasePattern;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link MatchExpr#parsePhrasePattern(String)} covering the
 * 8 corner cases enumerated in the add-tantivy-fe-ddl-sql change spec D8.
 */
public class MatchPhraseParserTest {

    private static void assertParsed(String raw, String expectedText, int expectedSlop) {
        PhrasePattern parsed = MatchExpr.parsePhrasePattern(raw);
        assertEquals(expectedText, parsed.getText(),
                "text mismatch for input: \"" + raw + "\"");
        assertEquals(expectedSlop, parsed.getSlop(),
                "slop mismatch for input: \"" + raw + "\"");
    }

    @Test
    public void plainPattern_noSlopMarker_keepsOriginalText() {
        assertParsed("a b c", "a b c", 0);
    }

    @Test
    public void trailingTildeNumber_isExtractedAsSlop() {
        assertParsed("a b c ~3", "a b c", 3);
    }

    @Test
    public void leadingZeroSlop_isAccepted() {
        assertParsed("a b c ~03", "a b c", 3);
    }

    @Test
    public void noWhitespaceBeforeTilde_treatedAsLiteral() {
        // No \s before ~3 -> regex does not match, slop=0, text intact
        assertParsed("a b c~3", "a b c~3", 0);
    }

    @Test
    public void tildeWithoutDigits_treatedAsLiteral() {
        assertParsed("a b c ~", "a b c ~", 0);
    }

    @Test
    public void onlySlopMarkerNoText_yieldsEmptyText() {
        // "~3" matches the (?:^|\s+) anchor alternative; text becomes empty.
        // Caller (ExpressionAnalyzer) is expected to reject empty text.
        assertParsed("~3", "", 3);
    }

    @Test
    public void multipleSlopMarkers_onlyTrailingOneWins() {
        // The middle "~3" stays in the text; only the trailing "~5" is extracted.
        assertParsed("a b c ~3 ~5", "a b c ~3", 5);
    }

    @Test
    public void negativeSlop_treatedAsLiteral() {
        // The minus sign is not part of \d+, so "~-1" is not recognized as a
        // slop marker and remains in the text.
        assertParsed("a b c ~-1", "a b c ~-1", 0);
    }

    @Test
    public void nullInput_yieldsEmptyResult() {
        PhrasePattern parsed = MatchExpr.parsePhrasePattern(null);
        assertEquals("", parsed.getText());
        assertEquals(0, parsed.getSlop());
    }

    @Test
    public void slopOutOfRange_throws() {
        // Bigger than INT_MAX/2 should be rejected (huge slop has O(n*slop) cost).
        SemanticException ex = assertThrows(SemanticException.class,
                () -> MatchExpr.parsePhrasePattern("a b c ~9999999999"));
        // Sanity check the message mentions slop and "out of range".
        String msg = ex.getMessage();
        assertEquals(true, msg.toLowerCase().contains("slop"),
                "message should mention slop: " + msg);
    }

    @Test
    public void slopAtBoundary_isAccepted() {
        int max = Integer.MAX_VALUE / 2;
        assertParsed("a b c ~" + max, "a b c", max);
    }

    @Test
    public void trailingWhitespaceAfterSlop_isTolerated() {
        assertParsed("a b c ~3   ", "a b c", 3);
    }
}
