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

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Chooses which input files a data-tier sample scans when scanning all of them would take too long.
 *
 * <p>Files are grouped into strata (one per path partition value, or a single stratum), and each
 * stratum receives a share of the byte budget proportional to its bytes, plus a floor on its file
 * count: at least one file, and together at least the requested minimum. Within a stratum the files
 * are taken in {@link #visitOrder} until both the byte quota and the file floor are met, so a stratum
 * whose floor is already met overshoots its quota by at most one file; the floor itself can take it
 * further, but not past four times the limit ({@link #floorByteCeiling}), shared by bytes in the same
 * way. A file cap, also shared by bytes, bounds the count from above; it wins over the minimum but not
 * over one file per stratum. When there are more strata than the cap, one file per stratum would
 * itself exceed it, so only the heaviest cap-many strata are sampled, one file each.
 *
 * <p>The order is systematic sampling with probability proportional to size over the path-sorted
 * files. Systematic in path order because files written by a range-partitioned job are named in
 * sort-key order, so evenly spaced picks cover the whole key range; for unordered files it is as good
 * as a random pick. Size-weighted so that a large file is never skipped. The result depends only on the
 * files themselves, never on the order they were listed in.
 */
final class SampleFileSelector {

    /** One input file; {@code stratum} is empty when the files are not stratified. */
    record Candidate(String path, long bytes, List<String> stratum) {
        Candidate {
            Objects.requireNonNull(path, "path");
            Preconditions.checkArgument(bytes >= 0L, "bytes must be non-negative, was %s", bytes);
            stratum = List.copyOf(stratum);
        }
    }

    // The file floor adds files only until the scan reaches this many times the byte limit: past that, the
    // spread it buys costs more time than the limit exists to save.
    private static final int FLOOR_BYTE_LIMIT_MULTIPLE = 4;

    // Past this level a grid point no longer lands in any non-empty file of a stratum smaller than
    // 2^62 bytes; files still unvisited by then are taken in path order.
    private static final int MAX_GRID_LEVEL = 62;

    private SampleFileSelector() {
    }

    /**
     * Returns the chosen files in input order. Zero-byte files are never chosen: they hold no rows.
     */
    static List<Candidate> select(List<Candidate> candidates, long byteLimit, int minFiles, int maxFiles) {
        long totalBytes = 0L;
        Map<List<String>, List<Integer>> strata = new LinkedHashMap<>();
        for (int index = 0; index < candidates.size(); index++) {
            Candidate candidate = candidates.get(index);
            if (candidate.bytes() > 0L) {
                totalBytes += candidate.bytes();
                strata.computeIfAbsent(candidate.stratum(), key -> new ArrayList<>()).add(index);
            }
        }
        boolean[] chosen = new boolean[candidates.size()];
        Set<List<String>> oneFileStrata = maxFiles > 0 && strata.size() > maxFiles
                ? heaviestStrata(candidates, strata, maxFiles) : null;
        for (Map.Entry<List<String>, List<Integer>> stratum : strata.entrySet()) {
            if (oneFileStrata == null) {
                selectInStratum(candidates, stratum.getValue(), byteLimit, minFiles, maxFiles, totalBytes, chosen);
            } else if (oneFileStrata.contains(stratum.getKey())) {
                selectInStratum(candidates, stratum.getValue(), byteLimit, 1, 1, totalBytes, chosen);
            }
        }
        List<Candidate> selected = new ArrayList<>();
        for (int index = 0; index < candidates.size(); index++) {
            if (chosen[index]) {
                selected.add(candidates.get(index));
            }
        }
        return selected;
    }

    /** The most bytes the file floor may take the whole scan to: a fixed multiple of the byte limit. */
    static long floorByteCeiling(long byteLimit) {
        return LongMath.saturatedMultiply(byteLimit, FLOOR_BYTE_LIMIT_MULTIPLE);
    }

    /** The {@code count} strata with the most bytes; a tie goes to the stratum whose values sort first. */
    private static Set<List<String>> heaviestStrata(List<Candidate> candidates, Map<List<String>, List<Integer>> strata,
                                                    int count) {
        Map<List<String>, Long> bytesByStratum = new HashMap<>();
        for (Map.Entry<List<String>, List<Integer>> stratum : strata.entrySet()) {
            long bytes = 0L;
            for (int index : stratum.getValue()) {
                bytes += candidates.get(index).bytes();
            }
            bytesByStratum.put(stratum.getKey(), bytes);
        }
        List<List<String>> ranked = new ArrayList<>(strata.keySet());
        ranked.sort(Comparator.comparingLong((List<String> stratum) -> bytesByStratum.get(stratum)).reversed()
                .thenComparing(SampleFileSelector::compareValues));
        return new HashSet<>(ranked.subList(0, count));
    }

    /** Orders two strata of the same arity by their values, first differing value first. */
    private static int compareValues(List<String> left, List<String> right) {
        for (int i = 0; i < left.size(); i++) {
            int compared = left.get(i).compareTo(right.get(i));
            if (compared != 0) {
                return compared;
            }
        }
        return 0;
    }

    private static void selectInStratum(List<Candidate> candidates, List<Integer> stratumFiles, long byteLimit,
                                        int minFiles, int maxFiles, long totalBytes, boolean[] chosen) {
        List<Integer> byPath = new ArrayList<>(stratumFiles);
        // List.sort is stable, so a path listed twice keeps its listing order.
        byPath.sort(Comparator.comparing(index -> candidates.get(index).path()));
        long stratumBytes = 0L;
        for (int index : byPath) {
            stratumBytes += candidates.get(index).bytes();
        }
        // The stratum's shares of the byte limit and of the file counts, in exact integer arithmetic: a double
        // share can land one file off at an integer boundary.
        long byteQuota = shareCeiling(byteLimit, stratumBytes, totalBytes);
        long floorBytes = shareCeiling(floorByteCeiling(byteLimit), stratumBytes, totalBytes);
        long fileCap = maxFiles <= 0 ? Long.MAX_VALUE : Math.max(1L, shareFloor(maxFiles, stratumBytes, totalBytes));
        long fileFloor = Math.min(fileCap,
                Math.min(byPath.size(), Math.max(1L, shareCeiling(Math.max(0, minFiles), stratumBytes, totalBytes))));
        long takenBytes = 0L;
        long takenFiles = 0L;
        for (int index : visitOrder(candidates, byPath, stratumBytes, fileCap)) {
            if (takenFiles >= fileCap
                    || (takenBytes >= byteQuota && (takenFiles >= fileFloor || takenBytes >= floorBytes))) {
                return;
            }
            chosen[index] = true;
            takenBytes += candidates.get(index).bytes();
            takenFiles++;
        }
    }

    /** {@code floor(amount * part / whole)} for {@code 0 <= part <= whole}, which cannot overflow the result. */
    private static long shareFloor(long amount, long part, long whole) {
        return BigInteger.valueOf(amount).multiply(BigInteger.valueOf(part)).divide(BigInteger.valueOf(whole))
                .longValue();
    }

    /** {@code ceil(amount * part / whole)} for {@code 0 <= part <= whole}. */
    private static long shareCeiling(long amount, long part, long whole) {
        BigInteger[] quotientAndRemainder = BigInteger.valueOf(amount).multiply(BigInteger.valueOf(part))
                .divideAndRemainder(BigInteger.valueOf(whole));
        return quotientAndRemainder[0].longValue() + (quotientAndRemainder[1].signum() > 0 ? 1L : 0L);
    }

    /**
     * The stratum's files in the order the sample takes them. The path-sorted files are laid end to
     * end on a cumulative-byte axis scaled to {@code [0, 1)}, and level {@code L} places grid points at
     * {@code m / 2^(L+1)}; the odd {@code m} are the level's new points, and all levels up to {@code L}
     * together form an evenly spaced grid. A file is visited at the first level whose grid falls inside
     * it, so a larger file is reached earlier and a file wider than the spacing is certain. Within a
     * level, the new points are visited in bit-reversed order -- the base-2 van der Corput sequence --
     * so a selection that stops part-way through a level is still spread over the whole path range
     * instead of bunched at its start. The order stops growing once it holds {@code limit} files, which
     * is all a capped stratum can take.
     */
    private static List<Integer> visitOrder(List<Candidate> candidates, List<Integer> byPath, long stratumBytes,
                                            long limit) {
        int fileCount = byPath.size();
        double[] start = new double[fileCount];
        double[] end = new double[fileCount];
        long cumulative = 0L;
        for (int file = 0; file < fileCount; file++) {
            start[file] = (double) cumulative / stratumBytes;
            cumulative += candidates.get(byPath.get(file)).bytes();
            end[file] = (double) cumulative / stratumBytes;
        }
        boolean[] visited = new boolean[fileCount];
        List<Integer> order = new ArrayList<>(fileCount);
        long wanted = Math.min(fileCount, limit);
        for (int level = 0; level <= MAX_GRID_LEVEL && order.size() < wanted; level++) {
            double gridSize = Math.scalb(1.0, level + 1);
            List<long[]> reached = new ArrayList<>();
            for (int file = 0; file < fileCount; file++) {
                if (visited[file]) {
                    continue;
                }
                // The first grid point at or after the file's start. Every point of a lower level is an
                // even m and lies in an already visited file, so a new hit here is always an odd m.
                long point = Math.max(1L, (long) Math.ceil(start[file] * gridSize));
                if (point < end[file] * gridSize) {
                    long pointInLevel = (point - 1) >>> 1;
                    long rank = level == 0 ? 0L : Long.reverse(pointInLevel) >>> (Long.SIZE - level);
                    reached.add(new long[] {rank, file});
                }
            }
            reached.sort(Comparator.<long[]>comparingLong(hit -> hit[0]).thenComparingLong(hit -> hit[1]));
            for (long[] hit : reached) {
                visited[(int) hit[1]] = true;
                order.add(byPath.get((int) hit[1]));
            }
        }
        for (int file = 0; file < fileCount && order.size() < wanted; file++) {
            if (!visited[file]) {
                order.add(byPath.get(file));
            }
        }
        return order;
    }
}
