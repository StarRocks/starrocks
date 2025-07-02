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
}
