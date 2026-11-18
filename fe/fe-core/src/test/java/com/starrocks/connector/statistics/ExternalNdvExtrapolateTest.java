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

    // A column whose merged distinct count after k partitions is a - b * r^k.
    private static double curve(double a, double b, double r, int k) {
        return a - b * Math.pow(r, k);
    }

    private static double growth(double thirdNdv, long thirdPartitions, double twoThirdsNdv,
                                 long twoThirdsPartitions, double mergedNdv, int sampled, int total) {
        return StatisticsUtils.extrapolateNdvFromGrowth(thirdNdv, thirdPartitions, twoThirdsNdv, twoThirdsPartitions,
                mergedNdv, sampled, total);
    }

    @Test
    public void testGrowthCurveRecoversAGeometricCurve() {
        // Each partition adds 0.995 of what the one before it added: the three points over 300 partitions
        // pin the curve down, and the estimate is the same curve at 1800.
        double a = 2_000_000;
        double r = 0.995;
        double ndv = growth(curve(a, a, r, 100), 100, curve(a, a, r, 200), 200, curve(a, a, r, 300), 300, 1800);

        Assertions.assertEquals(curve(a, a, r, 1800), ndv, curve(a, a, r, 1800) * 1e-6);
    }

    @Test
    public void testGrowthCurveUsesTheReportedSubsetSizes() {
        // A hash pick does not split 300 partitions into exact thirds; the curve has to be fitted at the
        // sizes the subsets actually have, here 115 and 207.
        double a = 1_000_000;
        double r = 0.99;
        double ndv = growth(curve(a, a, r, 115), 115, curve(a, a, r, 207), 207, curve(a, a, r, 300), 300, 1000);

        Assertions.assertEquals(curve(a, a, r, 1000), ndv, curve(a, a, r, 1000) * 1e-6);
    }

    @Test
    public void testGrowthCurveIsLinearForSteadyGrowth() {
        // A fixed set of hot values next to a steady stream of new ones: every partition adds the same
        // number of values, so the overlap the occupancy model sees does not stop the growth.
        Assertions.assertEquals(1801000, growth(101000, 100, 201000, 200, 301000, 300, 1800), 1e-6);
        // Faster than linear carries on at the latest rate rather than accelerating; the caller's ceiling
        // bounds it further.
        Assertions.assertEquals(301000 + 150000.0 * 15, growth(101000, 100, 151000, 200, 301000, 300, 1800), 1e-6);
    }

    @Test
    public void testGrowthCurveIgnoresGrowthInsideTheSketchError() {
        // A column that stopped growing reads as a few HLL-noise increments; extrapolating their ratio
        // over 1500 partitions would turn noise into a large number.
        Assertions.assertEquals(0, growth(10000, 100, 10050, 200, 10100, 300, 1800));
        Assertions.assertEquals(0, growth(10000, 100, 10100, 200, 10050, 300, 1800));
        Assertions.assertEquals(0, growth(10000, 100, 20000, 200, 20100, 300, 1800));
    }

    @Test
    public void testGrowthCurveWithoutUsableSubsetsSaysNothing() {
        // A backend too old to report them.
        Assertions.assertEquals(0, growth(0, 0, 0, 0, 300, 300, 1800));
        // Subsets that do not nest strictly inside the collected partitions: empty, equal, or all of them.
        Assertions.assertEquals(0, growth(100, 0, 200, 200, 300, 300, 1800));
        Assertions.assertEquals(0, growth(100, 200, 200, 200, 300, 300, 1800));
        Assertions.assertEquals(0, growth(100, 100, 200, 300, 300, 300, 1800));
        // Nothing to scale to.
        Assertions.assertEquals(0, growth(100, 100, 200, 200, 300, 300, 300));
    }

    @Test
    public void testGrowthCurveCatchesWhatTheOccupancyModelMisses() {
        // 1000 values every partition holds plus 1000 new ones per partition. The overlap makes the
        // occupancy model read a nearly exhausted domain; the growth curve sees the new values keep coming.
        int sampled = 300;
        int total = 1800;
        double merged = 1000 + 1000.0 * sampled;
        double perPartitionSum = 2000.0 * sampled;
        double occupancy = StatisticsUtils.extrapolateNdv(merged, perPartitionSum, sampled, total);
        double grown = growth(1000 + 1000.0 * 100, 100, 1000 + 1000.0 * 200, 200, merged, sampled, total);
        double truth = 1000 + 1000.0 * total;

        Assertions.assertTrue(occupancy < truth * 0.3, "occupancy model got " + occupancy);
        Assertions.assertEquals(truth, grown, 1e-6);
    }
}
