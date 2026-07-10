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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link ReferenceExpander}'s relevance-propagating path score: a row reached from a seed of
 * relevance {@code w} gets {@code pathScore = w * 1/(1+hop)} instead of the seed-blind
 * {@code 1/(1+hop)}. Stubs {@code runQuery} so no SQL plane is needed.
 */
public class ReferenceExpanderSeedWeightTest {

    /** Expander whose per-hop query returns a fixed edge set: 100->10, 200->20 (kind "ref"). */
    private static ReferenceExpander stubExpander() {
        return new ReferenceExpander() {
            @Override
            JsonArray runQuery(String sql) {
                JsonArray batch = new JsonArray();
                batch.add(edge(100, 10));
                batch.add(edge(200, 20));
                return batch;
            }
        };
    }

    private static JsonObject edge(long src, long dst) {
        JsonArray data = new JsonArray();
        data.add(src);
        data.add(dst);
        data.add("ref");
        JsonObject row = new JsonObject();
        row.add("data", data);
        return row;
    }

    private static ReferenceExpander.Request req(Map<Long, Double> seedWeights) {
        ReferenceExpander.Request r = new ReferenceExpander.Request();
        r.seeds = Arrays.asList(100L, 200L);
        r.direction = ReferenceExpander.Direction.FORWARD;
        r.depth = 1;
        r.seedWeights = seedWeights;
        return r;
    }

    private static double pathScoreOf(ReferenceExpander.Result res, long entityId) {
        return res.rows.stream().filter(row -> row.entityId == entityId)
                .findFirst().orElseThrow(() -> new AssertionError("no row for " + entityId)).pathScore;
    }

    @Test
    public void testSeedRelevancePropagatesToHopScore() {
        Map<Long, Double> weights = new HashMap<>();
        weights.put(100L, 0.9);
        weights.put(200L, 0.3);
        ReferenceExpander.Result res = stubExpander().expand(req(weights));

        // Seed rows carry their own weight.
        Assertions.assertEquals(0.9, pathScoreOf(res, 100L), 1e-12);
        Assertions.assertEquals(0.3, pathScoreOf(res, 200L), 1e-12);
        // Hop-1 children inherit seed weight × 1/(1+hop) = weight × 0.5.
        Assertions.assertEquals(0.9 * 0.5, pathScoreOf(res, 10L), 1e-12);
        Assertions.assertEquals(0.3 * 0.5, pathScoreOf(res, 20L), 1e-12);
        // The doc off the high-relevance seed outranks the one off the marginal seed — impossible
        // under the flat seed-blind 1/(1+hop), which would tie them at 0.5.
        Assertions.assertTrue(pathScoreOf(res, 10L) > pathScoreOf(res, 20L));
    }

    @Test
    public void testNullSeedWeightsReproducesLegacyHopOnlyScore() {
        ReferenceExpander.Result res = stubExpander().expand(req(null));
        // Legacy behavior: every seed 1.0, every hop-1 child 1/(1+1) = 0.5.
        Assertions.assertEquals(1.0, pathScoreOf(res, 100L), 1e-12);
        Assertions.assertEquals(1.0, pathScoreOf(res, 200L), 1e-12);
        Assertions.assertEquals(0.5, pathScoreOf(res, 10L), 1e-12);
        Assertions.assertEquals(0.5, pathScoreOf(res, 20L), 1e-12);
    }

    @Test
    public void testMissingSeedDefaultsToWeightOne() {
        // Only seed 100 has a weight; seed 200 falls back to 1.0.
        Map<Long, Double> weights = new HashMap<>();
        weights.put(100L, 0.4);
        ReferenceExpander.Result res = stubExpander().expand(req(weights));
        Assertions.assertEquals(0.4, pathScoreOf(res, 100L), 1e-12);
        Assertions.assertEquals(1.0, pathScoreOf(res, 200L), 1e-12);
        Assertions.assertEquals(0.4 * 0.5, pathScoreOf(res, 10L), 1e-12);
        Assertions.assertEquals(1.0 * 0.5, pathScoreOf(res, 20L), 1e-12);
    }
}
