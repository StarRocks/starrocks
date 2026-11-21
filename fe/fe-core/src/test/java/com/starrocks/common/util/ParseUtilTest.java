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

package com.starrocks.common.util;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParseUtilTest {

    @Test
    public void testParseBooleanValue() {
        Assertions.assertEquals(true, ParseUtil.parseBooleanValue("on", "var"));
        Assertions.assertEquals(true, ParseUtil.parseBooleanValue("TRUE", "var"));
        Assertions.assertEquals(true, ParseUtil.parseBooleanValue("1", "var"));

        Assertions.assertEquals(false, ParseUtil.parseBooleanValue("off", "var"));
        Assertions.assertEquals(false, ParseUtil.parseBooleanValue("FALSE", "var"));
        Assertions.assertEquals(false, ParseUtil.parseBooleanValue("0", "var"));
    }

    @Test
    public void testParseBooleanValueException() {
        Throwable exception = assertThrows(SemanticException.class, () -> ParseUtil.parseBooleanValue("tru", "var"));
        assertThat(exception.getMessage(),
                containsString("Invalid var: 'tru'. Expected values should be 1, 0, on, off, true, or false (case insensitive)"));
    }

    @Test
    public void testBackquoteEscapesInnerBackticks() {
        Assertions.assertEquals("`a``b`", ParseUtil.backquote("a`b"));
        Assertions.assertEquals("```ab```", ParseUtil.backquote("`ab`"));
        Assertions.assertEquals("`ab`", ParseUtil.backquote("ab"));
    }

    @Test
    public void testAnalyzeDataVolumeNormal() throws Exception {
        Assertions.assertEquals(1L, ParseUtil.analyzeDataVolume("1B"));
        Assertions.assertEquals(1024L, ParseUtil.analyzeDataVolume("1K"));
        Assertions.assertEquals(1024L * 1024, ParseUtil.analyzeDataVolume("1MB"));
        Assertions.assertEquals(10L * 1024 * 1024 * 1024, ParseUtil.analyzeDataVolume("10G"));
        Assertions.assertEquals(8L * 1024 * 1024 * 1024 * 1024, ParseUtil.analyzeDataVolume("8t"));
        Assertions.assertEquals(2L * 1024 * 1024 * 1024 * 1024 * 1024, ParseUtil.analyzeDataVolume("2P"));
    }

    @Test
    public void testAnalyzeDataVolumeOverflow() {
        // Each of these parses fine but its product with the byte-unit multiplier
        // overflows a long (into negative, 0, or an unrelated positive value).
        // They must all be rejected at analysis time.
        for (String s : new String[] {"8589934592G", "17179869184G", "17179869185G", "9223372036854775807G"}) {
            try {
                ParseUtil.analyzeDataVolume(s);
                Assertions.fail("expected AnalysisException for overflow input: " + s);
            } catch (AnalysisException e) {
                Assertions.assertTrue(e.getMessage().contains("overflow"),
                        "unexpected message for " + s + ": " + e.getMessage());
            }
        }
    }

    @Test
    public void testAnalyzeDataVolumeInvalidInput() {
        for (String s : new String[] {"0G", "-1G", "abc", "", "1XB"}) {
            try {
                ParseUtil.analyzeDataVolume(s);
                Assertions.fail("expected AnalysisException for invalid input: '" + s + "'");
            } catch (AnalysisException e) {
                // expected
            }
        }
    }
}
