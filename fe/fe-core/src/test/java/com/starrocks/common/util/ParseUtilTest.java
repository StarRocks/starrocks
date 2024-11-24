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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ParseUtilTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testParseBooleanValue() {
        Assert.assertEquals(true, ParseUtil.parseBooleanValue("on", "var"));
        Assert.assertEquals(true, ParseUtil.parseBooleanValue("TRUE", "var"));
        Assert.assertEquals(true, ParseUtil.parseBooleanValue("1", "var"));

        Assert.assertEquals(false, ParseUtil.parseBooleanValue("off", "var"));
        Assert.assertEquals(false, ParseUtil.parseBooleanValue("FALSE", "var"));
        Assert.assertEquals(false, ParseUtil.parseBooleanValue("0", "var"));
    }

    @Test
    public void testParseBooleanValueException() {
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage(
                "Invalid var: 'tru'. Expected values should be 1, 0, on, off, true, or false (case insensitive)");
        ParseUtil.parseBooleanValue("tru", "var");
    }
}
