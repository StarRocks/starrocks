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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.varcharColumn;

public class FingerprintRowIdBoundariesTest {

    private static final Column ROW_ID_COLUMN = varcharColumn("__ROW_ID__");
    private static final String SYMBOLS = "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final long GIB = 1L << 30;

    private long savedMinSplitSize;

    @BeforeEach
    public void setUp() {
        savedMinSplitSize = Config.tablet_reshard_min_split_size;
    }

    @AfterEach
    public void tearDown() {
        Config.tablet_reshard_min_split_size = savedMinSplitSize;
    }

    @Test
    public void testFourTabletsUseTwoSymbols() {
        // 4 tablets need 256 * 4 = 1024 positions, so two symbols (4096 positions) are the shortest
        // prefix; quarter points are 1024, 2048, 3072 = "E+", "U+", "k+". A wrong symbol order or
        // prefix length would shift every tablet's share.
        Config.tablet_reshard_min_split_size = 1;

        List<Tuple> cuts = boundariesOf(FingerprintRowIdBoundaries.plan(1_000_000_000L, 4, ROW_ID_COLUMN));

        Assertions.assertEquals(List.of("E+", "U+", "k+"), cutStrings(cuts));
    }

    @Test
    public void testMoreThan16TabletsUseThreeSymbols() {
        // 128 tablets need 32768 positions, more than two symbols give, so every cut has length 3.
        Config.tablet_reshard_min_split_size = 1;

        List<Tuple> cuts = boundariesOf(FingerprintRowIdBoundaries.plan(1_000_000_000L, 128, ROW_ID_COLUMN));

        Assertions.assertEquals(127, cuts.size());
        for (String cut : cutStrings(cuts)) {
            Assertions.assertEquals(3, cut.length());
        }
        Assertions.assertEquals("+U+", cutStrings(cuts).get(0));
        assertStrictlyIncreasing(cuts);
    }

    @Test
    public void testUnevenCountsKeepSharesWithinRounding() {
        Config.tablet_reshard_min_split_size = 1;

        for (int n : new int[] {3, 48, 63, 1023}) {
            List<Tuple> cuts = boundariesOf(FingerprintRowIdBoundaries.plan(1L << 40, n, ROW_ID_COLUMN));
            List<String> strings = cutStrings(cuts);
            Assertions.assertEquals(n - 1, cuts.size());
            long space = 1;
            for (int i = 0; i < strings.get(0).length(); i++) {
                space *= 64;
            }
            Assertions.assertTrue(space >= 256L * n, "n=" + n + " space=" + space);

            double expected = (double) space / n;
            long previous = 0;
            for (String cut : strings) {
                long position = positionOf(cut);
                Assertions.assertTrue(Math.abs((position - previous) - expected) < 1,
                        "n=" + n + " width " + (position - previous) + " vs " + expected);
                previous = position;
            }
            Assertions.assertTrue(Math.abs((space - previous) - expected) < 1, "n=" + n + " last bucket");
        }
    }

    @Test
    public void testCountIsClampedByMinimumSplitSize() {
        // 5 GiB holds only two tablets of at least 2 GiB, whatever was requested.
        Config.tablet_reshard_min_split_size = 2 * GIB;

        List<Tuple> cuts = boundariesOf(FingerprintRowIdBoundaries.plan(5 * GIB, 8, ROW_ID_COLUMN));

        Assertions.assertEquals(List.of("U+"), cutStrings(cuts));
    }

    @Test
    public void testTooSmallForTwoMinimumSizeTablets() {
        Config.tablet_reshard_min_split_size = 2 * GIB;

        assertSkipped(SkipReason.ROW_ID_SPAN_TOO_SMALL, FingerprintRowIdBoundaries.plan(3 * GIB, 8, ROW_ID_COLUMN));
    }

    @Test
    public void testUnknownEstimateIsNotDerivable() {
        Config.tablet_reshard_min_split_size = 1;

        assertSkipped(SkipReason.ESTIMATE_UNAVAILABLE, FingerprintRowIdBoundaries.plan(0, 8, ROW_ID_COLUMN));
    }

    @Test
    public void testNonPositiveMinimumSplitSizeIsNotDerivable() {
        Config.tablet_reshard_min_split_size = 0;

        assertSkipped(SkipReason.DERIVATION_FAILED, FingerprintRowIdBoundaries.plan(GIB, 8, ROW_ID_COLUMN));
    }

    @Test
    public void testCutsCarryTheColumnType() {
        // The BE compares boundaries against the stored key, so the variant type must be the schema's.
        Config.tablet_reshard_min_split_size = 1;

        for (Tuple cut : boundariesOf(FingerprintRowIdBoundaries.plan(1_000_000_000L, 8, ROW_ID_COLUMN))) {
            for (Variant value : cut.getValues()) {
                Assertions.assertEquals(ROW_ID_COLUMN.getType(), value.getType());
            }
        }
    }

    @Test
    public void testFingerprintsSpreadEvenlyAcrossTheCuts() throws NoSuchAlgorithmException {
        Config.tablet_reshard_min_split_size = 1;
        List<String> cuts = cutStrings(boundariesOf(FingerprintRowIdBoundaries.plan(1_000_000_000L, 8, ROW_ID_COLUMN)));

        int rows = 200_000;
        int[] buckets = new int[8];
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < rows; i++) {
            String fingerprint = Base64.getEncoder().encodeToString(
                    digest.digest(Long.toString(i).getBytes(StandardCharsets.UTF_8)));
            int tablet = 0;
            for (String cut : cuts) {
                if (cut.compareTo(fingerprint) <= 0) {
                    tablet++;
                }
            }
            buckets[tablet]++;
        }

        double mean = rows / 8.0;
        for (int count : buckets) {
            Assertions.assertTrue(count >= 0.9 * mean && count <= 1.1 * mean, "bucket " + count + " mean " + mean);
        }
    }

    /** Inverse of the planner's encoding: the cut's position within the prefix space. */
    private static long positionOf(String cut) {
        long position = 0;
        for (char symbol : cut.toCharArray()) {
            position = position * 64 + SYMBOLS.indexOf(symbol);
        }
        return position;
    }

    private static List<String> cutStrings(List<Tuple> cuts) {
        List<String> strings = new ArrayList<>();
        for (Tuple cut : cuts) {
            Assertions.assertEquals(1, cut.getValues().size(), "row-id sort key is single-column");
            strings.add(cut.getValues().get(0).getStringValue());
        }
        return strings;
    }

    private static List<Tuple> boundariesOf(DerivedBoundarySource.Result result) {
        Assertions.assertNull(result.skipReason(), "expected cuts, got skip");
        Assertions.assertNotNull(result.boundaries());
        return result.boundaries().getBoundaries();
    }

    private static void assertSkipped(SkipReason expected, DerivedBoundarySource.Result result) {
        Assertions.assertEquals(expected, result.skipReason());
        Assertions.assertNull(result.boundaries(), "a skipped plan must not carry boundaries");
    }

    private static void assertStrictlyIncreasing(List<Tuple> boundaries) {
        for (int i = 1; i < boundaries.size(); i++) {
            Assertions.assertTrue(boundaries.get(i - 1).compareTo(boundaries.get(i)) < 0,
                    "boundaries not strictly increasing: " + boundaries);
        }
    }
}
