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

public class ChannelExecutorPatternTest {

    @Test
    public void testSafeRegexPatternAccepted() {
        Assertions.assertTrue(ChannelExecutor.isSafeRegexPattern("foo.*bar"));
        Assertions.assertDoesNotThrow(() -> ChannelExecutor.validateUserPattern("foo.*bar"));
    }

    @Test
    public void testNestedQuantifierRegexRejected() {
        Assertions.assertFalse(ChannelExecutor.isSafeRegexPattern("(a+)+$"));
        Assertions.assertFalse(ChannelExecutor.isSafeRegexPattern("(a|aa)+$"));
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ChannelExecutor.validateUserPattern("(a+)+$"));
        Assertions.assertTrue(ex.getMessage().contains("unsafe regex"), ex.getMessage());
    }

    @Test
    public void testLookaroundAndBackreferenceRejected() {
        Assertions.assertFalse(ChannelExecutor.isSafeRegexPattern("(?=a)b"));
        Assertions.assertFalse(ChannelExecutor.isSafeRegexPattern("(a)\\1"));
    }

    @Test
    public void testPatternLengthCapped() {
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            pattern.append('a');
        }
        Assertions.assertFalse(ChannelExecutor.isSafeRegexPattern(pattern.toString()));
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ChannelExecutor.validateUserPattern(pattern.toString()));
        Assertions.assertTrue(ex.getMessage().contains("max length"), ex.getMessage());
    }
}
