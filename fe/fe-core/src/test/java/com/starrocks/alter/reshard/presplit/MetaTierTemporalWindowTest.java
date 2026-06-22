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

package com.starrocks.alter.reshard.presplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;

public class MetaTierTemporalWindowTest {

    // A locale that pins the Arabic-Indic numbering system (-u-nu-arab) so locale-sensitive
    // number formatting would emit non-ASCII digits regardless of the JDK locale provider's
    // default for ar-SA. Proves the rendered microsecond fraction is ASCII no matter the JVM
    // default locale.
    private static final Locale ARABIC = Locale.forLanguageTag("ar-SA-u-nu-arab");

    @Test
    public void renderDateTimeUsesAsciiDigitsUnderNonAsciiLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(ARABIC);
            LocalDateTime withMicros = LocalDateTime.ofEpochSecond(1, 500123000, ZoneOffset.UTC);
            Assertions.assertEquals("1970-01-01 00:00:01.500123",
                    MetaTierTemporalWindow.renderDateTime(withMicros));
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void renderDateTimeKeepsFixedSixDigitFraction() {
        // The fraction is always 6 digits and trailing zeros are NOT trimmed, matching the
        // boundary text the BE load stores: 500000 us must render ".500000", not ".5".
        LocalDateTime trailingZeros = LocalDateTime.ofEpochSecond(1, 500_000_000, ZoneOffset.UTC);
        Assertions.assertEquals("1970-01-01 00:00:01.500000",
                MetaTierTemporalWindow.renderDateTime(trailingZeros));
    }

    @Test
    public void renderDateTimeOmitsFractionWhenNoSubSecond() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(ARABIC);
            LocalDateTime wholeSecond = LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC);
            Assertions.assertEquals("1970-01-01 00:00:01",
                    MetaTierTemporalWindow.renderDateTime(wholeSecond));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
