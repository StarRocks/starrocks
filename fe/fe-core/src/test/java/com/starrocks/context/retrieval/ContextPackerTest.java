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

package com.starrocks.context.retrieval;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class ContextPackerTest {

    @Test
    public void testEmptyInputProducesEmptyPack() {
        ContextPacker.Request req = new ContextPacker.Request();
        req.entityIds = Collections.emptyList();
        ContextPacker.Result result = new ContextPacker(null).pack(req);
        Assertions.assertEquals("", result.packedText);
        Assertions.assertEquals(0, result.usedTokensEstimate);
        Assertions.assertTrue(result.includedEntities.isEmpty());
    }

    @Test
    public void testTokenEstimateUsesFourCharsPerToken() {
        Assertions.assertEquals(0, ContextPacker.estimateTokens(""));
        Assertions.assertEquals(0, ContextPacker.estimateTokens(null));
        // 4-char string → 1 token.
        Assertions.assertEquals(1, ContextPacker.estimateTokens("abcd"));
        // 17-char string → floor(17/4)=4 tokens, min 1 applied when below 4.
        Assertions.assertEquals(4, ContextPacker.estimateTokens("seventeen_chars__"));
        // 3-char string → min-1 clamp kicks in so callers never see 0 for a non-empty input.
        Assertions.assertEquals(1, ContextPacker.estimateTokens("abc"));
    }
}
