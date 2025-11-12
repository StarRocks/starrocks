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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UtilTest {

    @Test
    public void testGetResultForUrl() {
        Assertions.assertThrows(Exception.class,
                () -> Util.getResultForUrl("http://127.0.0.1:23/invalid", null, 1000, 1000));
    }


    @Test
    public void returnsFalseWhenInputIsNull() {
        assertFalse(Util.stringToBool(null));
    }

    @Test
    public void returnsFalseWhenInputIsEmpty() {
        assertFalse(Util.stringToBool(""));
    }

    @Test
    public void returnsFalseWhenInputIsWhitespace() {
        assertFalse(Util.stringToBool("   "));
    }

    @Test
    public void returnsTrueWhenInputIsTrueIgnoringCase() {
        assertTrue(Util.stringToBool("TRUE"));
        assertTrue(Util.stringToBool("true"));
        assertTrue(Util.stringToBool("TrUe"));
    }

    @Test
    public void returnsTrueWhenInputIsOne() {
        assertTrue(Util.stringToBool("1"));
    }

    @Test
    public void returnsFalseWhenInputIsZero() {
        assertFalse(Util.stringToBool("0"));
    }

    @Test
    public void returnsFalseWhenInputIsFalseIgnoringCase() {
        assertFalse(Util.stringToBool("FALSE"));
        assertFalse(Util.stringToBool("false"));
        assertFalse(Util.stringToBool("FaLsE"));
    }

    @Test
    public void returnsTrueWhenInputHasTrailingWhitespace() {
        assertTrue(Util.stringToBool("true   "));
        assertTrue(Util.stringToBool("1   "));
    }

    @Test
    public void returnsFalseWhenInputIsInvalid() {
        assertFalse(Util.stringToBool("invalid"));
        assertFalse(Util.stringToBool("yes"));
        assertFalse(Util.stringToBool("no"));
    }
}
