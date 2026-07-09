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

package com.starrocks.context.retrieval.rerank.support;

import com.starrocks.context.retrieval.ReferenceExpander;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AdjacencyLoaderTest {

    @Test
    public void testForwardAndBackwardMergeUndirected() {
        // Edge 1 -> 2 from forward, edge 3 -> 4 from backward. Resulting adj must be undirected.
        Set<Long> pool = new LinkedHashSet<>(Arrays.asList(1L, 2L, 3L, 4L));
        StubExpander expander = new StubExpander()
                .addForward(1L, 2L)
                .addBackward(3L, 4L);

        Map<Long, Set<Long>> adj = AdjacencyLoader.loadUndirected1Hop(
                expander, pool, /*contextBaseId*/ 1L, Collections.singletonList(1L),
                /*snapshotFence*/ -1L, /*edgeTypes*/ null);

        Assertions.assertEquals(new HashSet<>(Arrays.asList(2L)), adj.get(1L));
        Assertions.assertEquals(new HashSet<>(Arrays.asList(1L)), adj.get(2L));
        Assertions.assertEquals(new HashSet<>(Arrays.asList(4L)), adj.get(3L));
        Assertions.assertEquals(new HashSet<>(Arrays.asList(3L)), adj.get(4L));
    }

    @Test
    public void testOutOfPoolNeighborDropped() {
        Set<Long> pool = new LinkedHashSet<>(Arrays.asList(1L, 2L));
        StubExpander expander = new StubExpander().addForward(1L, 99L);
        Map<Long, Set<Long>> adj = AdjacencyLoader.loadUndirected1Hop(
                expander, pool, 1L, Collections.singletonList(1L), -1L, null);
        Assertions.assertFalse(adj.containsKey(1L), "edge to out-of-pool neighbor must not be recorded");
        Assertions.assertTrue(adj.isEmpty());
    }

    @Test
    public void testSelfLoopDropped() {
        Set<Long> pool = new LinkedHashSet<>(Collections.singletonList(1L));
        StubExpander expander = new StubExpander().addForward(1L, 1L);
        Map<Long, Set<Long>> adj = AdjacencyLoader.loadUndirected1Hop(
                expander, pool, 1L, Collections.singletonList(1L), -1L, null);
        Assertions.assertTrue(adj.isEmpty());
    }

    @Test
    public void testEmptyInputReturnsEmpty() {
        Map<Long, Set<Long>> adj = AdjacencyLoader.loadUndirected1Hop(
                new StubExpander(), new HashSet<>(), 1L, Collections.singletonList(1L), -1L, null);
        Assertions.assertTrue(adj.isEmpty());
    }

    @Test
    public void testNullExpanderReturnsEmpty() {
        // Defensive: callers in unit tests may not supply an expander. Loader must not NPE.
        Map<Long, Set<Long>> adj = AdjacencyLoader.loadUndirected1Hop(
                null, new HashSet<>(Collections.singletonList(1L)),
                1L, Collections.singletonList(1L), -1L, null);
        Assertions.assertTrue(adj.isEmpty());
    }

    /**
     * Same stub shape as {@link com.starrocks.context.retrieval.rerank.VectorAnchorGreedyRerankStrategyTest}'s
     * — duplicated here to keep this test self-contained without a shared test-only helper class.
     */
    private static final class StubExpander extends ReferenceExpander {
        private final List<long[]> forward = new ArrayList<>();
        private final List<long[]> backward = new ArrayList<>();

        StubExpander addForward(long src, long dst) {
            forward.add(new long[] {src, dst});
            return this;
        }

        StubExpander addBackward(long dst, long src) {
            backward.add(new long[] {dst, src});
            return this;
        }

        @Override
        public List<long[]> scan(Collection<Long> nodes, Direction direction,
                                 Long contextBaseId, Long collectionId, List<Long> collectionIds,
                                 long snapshotFence, Collection<String> refKinds) {
            List<long[]> src = direction == Direction.FORWARD ? forward : backward;
            List<long[]> out = new ArrayList<>();
            for (long[] pair : src) {
                if (nodes.contains(pair[0])) {
                    out.add(pair);
                }
            }
            return out;
        }
    }
}
