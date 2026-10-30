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

package com.starrocks.connector.statistics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The distinct-value extrapolation on its own: given how many distinct values the sampled partitions
 * hold together (U), how many they hold counted separately (S), how many were sampled (P) and how many
 * the table has (N), what does the whole table hold?
 */
public class ExternalNdvExtrapolateTest {

    @Test
    public void testTheSolvedDomainMayExceedAnyBoundDerivedFromTheInputs() {
        // The domain size behind a measurement has no upper bound that can be written down from
        // (U, S, P, N). An earlier version bisected it over [d, S*N]: here the root is 10 while that
        // range stops at 6, so the search pinned itself to the wrong end and under-reported.
        //   P=2, d=1, S=2, U=1.9  ->  U = D(1-(1-1/D)^2) = 2 - 1/D  ->  D = 10
        //   over N=3 partitions:       10 * (1 - 0.9^3) = 2.71
        double ndv = StatisticsUtils.extrapolateNdv(1.9, 2, 2, 3);

        Assertions.assertEquals(2.71, ndv, 0.01);
        // Sanity: strictly between "no growth" and "proportional growth".
        Assertions.assertTrue(ndv > 1.9 && ndv < 1.9 * 3 / 2.0, "got " + ndv);
    }

    @Test
    public void testSketchNoiseAroundNoOverlapDoesNotFlipTheAnswer() {
        // For exact sets U <= S, so a U measured at or just under S means "no value is in two
        // partitions" - the sketch simply rounded. Comparing exactly made the same disjoint column
        // come out either at full linear growth or far below it depending on which way it rounded.
        double exact = StatisticsUtils.extrapolateNdv(1000, 1000, 10, 300);
        double roundedDown = StatisticsUtils.extrapolateNdv(995, 1000, 10, 300);
        double roundedUp = StatisticsUtils.extrapolateNdv(1005, 1000, 10, 300);

        Assertions.assertEquals(30000, exact, 1.0);
        Assertions.assertEquals(995 * 30.0, roundedDown, 1.0);
        Assertions.assertEquals(1005 * 30.0, roundedUp, 1.0);
    }

    @Test
    public void testSketchNoiseAroundFullOverlapDoesNotFlipTheAnswer() {
        // The other end: U cannot be below one partition's worth either.
        Assertions.assertEquals(100, StatisticsUtils.extrapolateNdv(100, 1000, 10, 300), 0.001);
        Assertions.assertEquals(99, StatisticsUtils.extrapolateNdv(99, 1000, 10, 300), 0.001);
        Assertions.assertEquals(101, StatisticsUtils.extrapolateNdv(101, 1000, 10, 300), 0.001);
    }

    @Test
    public void testTwoSampledPartitions() {
        // The smallest case the curve is still meaningful for.
        double ndv = StatisticsUtils.extrapolateNdv(150, 200, 2, 100);

        Assertions.assertTrue(ndv > 150, "expected growth, got " + ndv);
        Assertions.assertTrue(ndv <= 150 * 50.0, "expected at most proportional growth, got " + ndv);
    }

    @Test
    public void testNothingToScaleToIsLeftAlone() {
        // Already covering every partition, and the degenerate inputs around it.
        Assertions.assertEquals(500, StatisticsUtils.extrapolateNdv(500, 1000, 300, 300), 0.001);
        Assertions.assertEquals(500, StatisticsUtils.extrapolateNdv(500, 1000, 300, 200), 0.001);
        Assertions.assertEquals(500, StatisticsUtils.extrapolateNdv(500, 0, 10, 300), 0.001);
        Assertions.assertEquals(500, StatisticsUtils.extrapolateNdv(500, 1000, 0, 300), 0.001);
    }

}
