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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

class SampleFileSelectorTest {

    @Test
    void equalFilesAreTakenAtEvenlySpacedPositions() {
        // 100 files of 10 bytes, a 10% budget. Grid points visit 0.5, then 0.25/0.75, then
        // 0.125/0.625/0.375/0.875, then 1/16, 9/16, 5/16 ... (van der Corput order).
        List<SampleFileSelector.Candidate> candidates = equalFiles(100, 10L);

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 100L, 1, 0);

        Assertions.assertEquals(List.of(6, 12, 25, 31, 37, 50, 56, 62, 75, 87), indexesOf(selected));
    }

    @Test
    void aLargeFileIsTakenBeforeSmallerOnesAroundIt() {
        // "b" covers 70% of the bytes, so it holds the first grid point even though "a" sorts first.
        List<SampleFileSelector.Candidate> candidates = List.of(
                candidate("a", 100L), candidate("b", 700L), candidate("c", 100L), candidate("d", 100L));

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 700L, 1, 0);

        Assertions.assertEquals(List.of("b"), pathsOf(selected));
    }

    @Test
    void everyStratumGetsItsByteShareOfTheLimit() {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            candidates.add(new SampleFileSelector.Candidate("a/f" + i, 100L, List.of("a")));
        }
        for (int i = 0; i < 2; i++) {
            candidates.add(new SampleFileSelector.Candidate("b/f" + i, 100L, List.of("b")));
        }

        // 80% of a 500-byte limit is 400 bytes of "a" (4 files); 20% is 100 bytes of "b" (1 file).
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 500L, 1, 0);

        Assertions.assertEquals(List.of("a/f1", "a/f2", "a/f4", "a/f6", "b/f1"), pathsOf(selected));
    }

    @Test
    void aStratumBelowOneFileOfQuotaStillKeepsOneFile() {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>(equalFiles(99, 100L));
        candidates.add(new SampleFileSelector.Candidate("z/only", 100L, List.of("z")));

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 1000L, 1, 0);

        Assertions.assertTrue(pathsOf(selected).contains("z/only"), "every stratum keeps at least one file");
    }

    @Test
    void theFileFloorRaisesASmallSelection() {
        List<SampleFileSelector.Candidate> candidates = equalFiles(10, 100L);

        // 150 bytes would take two files; the floor of four wins.
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 150L, 4, 0);

        Assertions.assertEquals(List.of(1, 2, 5, 7), indexesOf(selected));
    }

    @Test
    void theFileFloorStopsAtFourTimesTheByteLimit() {
        // Large files: the floor of 64 would take all 40 files, 10 times the limit.
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(equalFiles(40, 100L), 400L, 64, 0);

        Assertions.assertEquals(16, selected.size());
        Assertions.assertEquals(1_600L, selected.stream().mapToLong(SampleFileSelector.Candidate::bytes).sum());
    }

    @Test
    void theFloorByteCeilingIsSharedByBytesButEveryStratumKeepsOneFile() {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>();
        candidates.add(new SampleFileSelector.Candidate("big/f", 1_000L, List.of("big")));
        for (int i = 0; i < 10; i++) {
            candidates.add(new SampleFileSelector.Candidate("small/f" + i, 10L, List.of("small")));
        }

        // Four times the 110-byte limit is 440 bytes: 400 for "big", whose one file is larger and still taken,
        // and 40 for "small", which stops there at 4 files although its share of the floor is 6.
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 110L, 64, 0);

        Assertions.assertEquals(1, selected.stream().filter(c -> c.stratum().equals(List.of("big"))).count());
        Assertions.assertEquals(4, selected.stream().filter(c -> c.stratum().equals(List.of("small"))).count());
    }

    @Test
    void aByteLimitNearTheTopOfTheLongRangeDoesNotOverflowTheFloorCeiling() {
        long half = Long.MAX_VALUE / 2;
        List<SampleFileSelector.Candidate> candidates = List.of(candidate("a", half), candidate("b", half));

        Assertions.assertEquals(2, SampleFileSelector.select(candidates, half - 1, 2, 0).size());
    }

    @Test
    void theFileFloorIsSharedAcrossStrataByBytes() {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            candidates.add(new SampleFileSelector.Candidate(String.format("a/f%02d", i), 10L, List.of("a")));
        }
        for (int i = 0; i < 10; i++) {
            candidates.add(new SampleFileSelector.Candidate(String.format("b/f%02d", i), 10L, List.of("b")));
        }

        // Floors: ceil(10 * 0.75) = 8 files of "a", ceil(10 * 0.25) = 3 files of "b".
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 40L, 10, 0);

        Assertions.assertEquals(8, selected.stream().filter(c -> c.stratum().equals(List.of("a"))).count());
        Assertions.assertEquals(3, selected.stream().filter(c -> c.stratum().equals(List.of("b"))).count());
    }

    @Test
    void theFileCapBoundsASelectionOfSmallFiles() {
        // A 50-file byte quota, but at most 8 files: the first 8 grid points.
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(equalFiles(100, 10L), 500L, 1, 8);

        Assertions.assertEquals(List.of(6, 12, 25, 37, 50, 62, 75, 87), indexesOf(selected));
    }

    @Test
    void theFileCapIsSharedByBytesButEveryStratumKeepsOneFile() {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            candidates.add(new SampleFileSelector.Candidate(String.format("a/f%02d", i), 10L, List.of("a")));
        }
        for (int i = 0; i < 10; i++) {
            candidates.add(new SampleFileSelector.Candidate(String.format("b/f%02d", i), 10L, List.of("b")));
        }
        candidates.add(new SampleFileSelector.Candidate("c/f00", 4L, List.of("c")));

        // Caps: floor(4 * 300/404) = 2 for "a", floor(4 * 100/404) = 0 -> 1 for "b", and 1 for "c".
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 202L, 1, 4);

        Assertions.assertEquals(2, selected.stream().filter(c -> c.stratum().equals(List.of("a"))).count());
        Assertions.assertEquals(1, selected.stream().filter(c -> c.stratum().equals(List.of("b"))).count());
        Assertions.assertEquals(1, selected.stream().filter(c -> c.stratum().equals(List.of("c"))).count());
    }

    @Test
    void moreStrataThanTheFileCapSampleOnlyTheHeaviestOneFileEach() {
        // Five strata of two files each, 50/40/30/20/10 bytes per file. One file of every stratum would
        // already exceed a cap of 3, so only the three heaviest strata are sampled, one file each.
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>();
        long[] fileBytes = {50L, 40L, 30L, 20L, 10L};
        for (int stratum = 0; stratum < fileBytes.length; stratum++) {
            for (int file = 0; file < 2; file++) {
                candidates.add(new SampleFileSelector.Candidate(
                        "s" + stratum + "/f" + file, fileBytes[stratum], List.of("s" + stratum)));
            }
        }

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 100L, 1, 3);

        Assertions.assertEquals(List.of("s0/f1", "s1/f1", "s2/f1"), pathsOf(selected));
    }

    @Test
    void strataWithEqualBytesAreRankedByTheirValuesInOrder() {
        // Joining ["a", "z"] and ["a-", "b"] would rank "a-/b" first ('-' sorts before '/'); value by value,
        // "a" sorts before "a-", so ["a", "z"] is the stratum kept under a cap of 1.
        List<SampleFileSelector.Candidate> candidates = List.of(
                new SampleFileSelector.Candidate("p/f0", 10L, List.of("a-", "b")),
                new SampleFileSelector.Candidate("q/f0", 10L, List.of("a", "z")));

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 10L, 1, 1);

        Assertions.assertEquals(List.of("q/f0"), pathsOf(selected));
    }

    @Test
    void quotasCapsAndFloorsAreExactAtIntegerBoundaries() {
        // 70 of 100 bytes under a cap of 90 is a cap of exactly 63 files (a double share gives 62.99...).
        Assertions.assertEquals(63, countInStratum(
                SampleFileSelector.select(twoStrata(70, 30), 99L, 1, 90), "a"));
        // 90 of 110 bytes under a 77-byte limit is a quota of exactly 63 bytes (a double share gives 63.00...01).
        Assertions.assertEquals(63, countInStratum(
                SampleFileSelector.select(twoStrata(90, 20), 77L, 1, 0), "a"));
    }

    /** One-byte files: {@code first} of them in stratum "a", {@code second} in stratum "b". */
    private static List<SampleFileSelector.Candidate> twoStrata(int first, int second) {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>();
        for (int i = 0; i < first; i++) {
            candidates.add(new SampleFileSelector.Candidate(String.format("a/f%03d", i), 1L, List.of("a")));
        }
        for (int i = 0; i < second; i++) {
            candidates.add(new SampleFileSelector.Candidate(String.format("b/f%03d", i), 1L, List.of("b")));
        }
        return candidates;
    }

    private static long countInStratum(List<SampleFileSelector.Candidate> selected, String stratum) {
        return selected.stream().filter(c -> c.stratum().equals(List.of(stratum))).count();
    }

    @Test
    void theFileCapWinsOverTheMinimumFileCount() {
        // The floor of four would take f5, f2, f7, f1; the cap of three stops after f7.
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(equalFiles(10, 100L), 150L, 4, 3);

        Assertions.assertEquals(List.of(2, 5, 7), indexesOf(selected));
    }

    @Test
    void theSelectionDependsOnlyOnTheFilesNotOnTheirListingOrder() {
        List<SampleFileSelector.Candidate> shuffled = new ArrayList<>(equalFiles(100, 10L));
        Collections.shuffle(shuffled, new Random(42L));

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(shuffled, 100L, 1, 0);

        List<Integer> sortedIndexes = new ArrayList<>(indexesOf(selected));
        Collections.sort(sortedIndexes);
        Assertions.assertEquals(List.of(6, 12, 25, 31, 37, 50, 56, 62, 75, 87), sortedIndexes);
        List<SampleFileSelector.Candidate> inInputOrder = shuffled.stream()
                .filter(selected::contains).collect(Collectors.toList());
        Assertions.assertEquals(inInputOrder, selected, "the result keeps the input order");
    }

    @Test
    void aSingleFileLargerThanTheLimitIsStillTaken() {
        List<SampleFileSelector.Candidate> candidates = List.of(candidate("huge", 5_000L));

        Assertions.assertEquals(List.of("huge"), pathsOf(SampleFileSelector.select(candidates, 100L, 1, 0)));
    }

    @Test
    void zeroByteFilesAreNeverTaken() {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>();
        candidates.add(candidate("f000-empty", 0L));
        candidates.addAll(equalFiles(10, 100L));

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 300L, 1, 0);

        Assertions.assertFalse(pathsOf(selected).contains("f000-empty"));
        Assertions.assertEquals(3, selected.size());
    }

    @Test
    void aPathListedTwiceIsTwoIndependentCandidates() {
        // Broker Load can list one file under two file groups; the selector must not merge or drop them.
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>(equalFiles(10, 100L));
        candidates.add(candidate("f005", 100L));

        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, 1_100L, 1, 0);

        Assertions.assertEquals(11, selected.size(), "a limit covering both copies takes both");
        Assertions.assertEquals(selected, SampleFileSelector.select(candidates, 1_100L, 1, 0), "deterministic");
    }

    @Test
    void aLimitCoveringTheInputTakesEveryNonEmptyFile() {
        List<SampleFileSelector.Candidate> candidates = equalFiles(10, 100L);

        Assertions.assertEquals(10, SampleFileSelector.select(candidates, 1_000L, 1, 0).size());
    }

    private static List<SampleFileSelector.Candidate> equalFiles(int count, long bytes) {
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            candidates.add(candidate(String.format("f%03d", i), bytes));
        }
        return candidates;
    }

    private static SampleFileSelector.Candidate candidate(String path, long bytes) {
        return new SampleFileSelector.Candidate(path, bytes, List.of());
    }

    private static List<String> pathsOf(List<SampleFileSelector.Candidate> candidates) {
        return candidates.stream().map(SampleFileSelector.Candidate::path).collect(Collectors.toList());
    }

    private static List<Integer> indexesOf(List<SampleFileSelector.Candidate> candidates) {
        return candidates.stream().map(c -> Integer.parseInt(c.path().substring(1))).collect(Collectors.toList());
    }
}
