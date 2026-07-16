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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringUtilsTest {

    @Test
    public void asciiOrderingUnchanged() {
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("abc", "abd") < 0);
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("abd", "abc") > 0);
        Assertions.assertEquals(0, StringUtils.compareStringWithUTF8ByteArray("abc", "abc"));
    }

    @Test
    public void shorterPrefixIsSmaller() {
        // "abc" < "abcd" (common prefix, shorter first) -- matches BE length tie-break.
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("abc", "abcd") < 0);
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("abcd", "abc") > 0);
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("", "a") < 0);
        Assertions.assertEquals(0, StringUtils.compareStringWithUTF8ByteArray("", ""));
    }

    @Test
    public void nonAsciiComparesUnsigned() {
        // 'z' = 0x7A, 'é' = U+00E9 = UTF-8 0xC3 0xA9. Unsigned: 0x7A < 0xC3, so "z" < "é".
        // (Signed-byte compare would treat 0xC3 as -61 and wrongly rank "é" first.)
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("z", "é") < 0);
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("é", "z") > 0);
        // Multi-byte CJK '中' = 0xE4 0xB8 0xAD, still greater than ASCII 'a'.
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("a", "中") < 0);
        // Two non-ASCII: 'é'(0xC3..) < '中'(0xE4..).
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("é", "中") < 0);
    }

    @Test
    public void trailingNulByteHasNoSpecialCase() {
        // "abc\0" is longer than "abc"; with no NUL special case, longer is larger.
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("abc", "abc\0") < 0);
        Assertions.assertTrue(StringUtils.compareStringWithUTF8ByteArray("abc\0", "abc") > 0);
    }
}
