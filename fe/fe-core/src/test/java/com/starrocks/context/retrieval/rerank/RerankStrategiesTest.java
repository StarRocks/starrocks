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

package com.starrocks.context.retrieval.rerank;

import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RerankStrategiesTest {

    @Test
    public void testResolveNullReturnsAdditiveDefault() {
        Assertions.assertEquals(AdditiveRerankStrategy.NAME,
                RerankStrategies.resolve(null).name());
        Assertions.assertEquals(AdditiveRerankStrategy.NAME,
                RerankStrategies.resolve("").name());
    }

    @Test
    public void testResolveCaseInsensitive() {
        Assertions.assertEquals(VectorAnchorGreedyRerankStrategy.NAME,
                RerankStrategies.resolve("VECTOR_ANCHOR_GREEDY").name());
        Assertions.assertEquals(VectorAnchorGreedyRerankStrategy.NAME,
                RerankStrategies.resolve("Vector_Anchor_Greedy").name());
        Assertions.assertEquals(AdditiveRerankStrategy.NAME,
                RerankStrategies.resolve("ADDITIVE").name());
    }

    @Test
    public void testUnknownStrategyThrowsInvalidArgument() {
        ContextException ex = Assertions.assertThrows(ContextException.class,
                () -> RerankStrategies.resolve("does_not_exist"));
        Assertions.assertEquals(ContextErrorCode.INVALID_ARGUMENT, ex.getCode());
        // Error message should enumerate available strategies so callers can fix typos.
        Assertions.assertTrue(ex.getMessage().contains("vector_anchor_greedy"));
        Assertions.assertTrue(ex.getMessage().contains("additive"));
    }

    @Test
    public void testAvailableIncludesBuiltins() {
        Assertions.assertTrue(RerankStrategies.available().contains(AdditiveRerankStrategy.NAME));
        Assertions.assertTrue(RerankStrategies.available().contains(RrfRerankStrategy.NAME));
        Assertions.assertTrue(RerankStrategies.available().contains(VectorAnchorGreedyRerankStrategy.NAME));
    }

    @Test
    public void testResolveRrf() {
        Assertions.assertEquals(RrfRerankStrategy.NAME, RerankStrategies.resolve("rrf").name());
        Assertions.assertEquals(RrfRerankStrategy.NAME, RerankStrategies.resolve("RRF").name());
    }

    @Test
    public void testRrfNeedsGraphExpandedPool() {
        // RRF ranks graphScore, so it needs the BFS-merged pool like additive.
        Assertions.assertTrue(new RrfRerankStrategy().needsGraphExpandedPool());
    }

    @Test
    public void testNeedsGraphExpandedPoolContract() {
        // Additive uses graphScore directly → needs the BFS-merged pool.
        Assertions.assertTrue(new AdditiveRerankStrategy().needsGraphExpandedPool());
        // Greedy loads its own adjacency restricted to the pool → doesn't need BFS.
        Assertions.assertFalse(new VectorAnchorGreedyRerankStrategy().needsGraphExpandedPool());
    }
}
