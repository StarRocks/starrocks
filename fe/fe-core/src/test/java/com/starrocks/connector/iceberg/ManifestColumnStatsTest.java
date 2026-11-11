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

package com.starrocks.connector.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class ManifestColumnStatsTest {
    // k_* columns are clustered by file index, r_* columns cover the whole domain in every file.
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "k_long", Types.LongType.get()),
            Types.NestedField.optional(2, "r_int", Types.IntegerType.get()),
            Types.NestedField.optional(3, "k_date", Types.DateType.get()),
            Types.NestedField.optional(4, "k_ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(5, "r_double", Types.DoubleType.get()),
            Types.NestedField.optional(6, "k_str", Types.StringType.get()),
            Types.NestedField.optional(7, "r_dec", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(8, "sparse", Types.LongType.get()),
            Types.NestedField.optional(9, "missing", Types.IntegerType.get()),
            Types.NestedField.optional(10, "b", Types.BooleanType.get()),
            Types.NestedField.optional(11, "r_float", Types.FloatType.get()),
            Types.NestedField.optional(12, "k_int_neg", Types.IntegerType.get()));
    private static final int FILES = 300;

    @Test
    public void testViewsReturnTheWrittenStatistics() {
        Random random = new Random(1);
        List<DataFile> files = dataFiles(random);
        ManifestColumnStats stats = build(files, Set.of(), 100);
        Assertions.assertEquals(SCHEMA.columns().size(), stats.columnCount());
        for (int i = 0; i < files.size(); i++) {
            DataFile source = files.get(i);
            assertSameMap(source.lowerBounds(), stats.lowerBounds(i));
            assertSameMap(source.upperBounds(), stats.upperBounds(i));
            assertSameMap(source.valueCounts(), stats.valueCounts(i));
            assertSameMap(source.nullValueCounts(), stats.nullValueCounts(i));
            assertSameMap(source.nanValueCounts(), stats.nanValueCounts(i));
            assertSameMap(source.columnSizes(), stats.columnSizes(i));
        }
    }

    @Test
    public void testPruningMatchesTheSdkOnFullStatistics() {
        Random random = new Random(7);
        List<DataFile> files = dataFiles(random);
        List<DataFile> cachedAll = cache(files, Set.of(), 100);
        // 6 columns: the two priority columns and the four lowest other field ids
        Set<Integer> priority = Set.of(6, 12);
        List<DataFile> cachedSome = cache(files, priority, 6);
        Set<Integer> someIds = Set.of(1, 2, 3, 4, 6, 12);

        int pruned = 0;
        for (int p = 0; p < 1500; p++) {
            Expression expr = randomPredicate(random);
            switch (random.nextInt(4)) {
                case 0 -> expr = Expressions.and(expr, randomPredicate(random));
                case 1 -> expr = Expressions.or(expr, randomPredicate(random));
                case 2 -> expr = Expressions.not(expr);
                default -> {
                }
            }
            InclusiveMetricsEvaluator evaluator = new InclusiveMetricsEvaluator(SCHEMA, expr);
            for (int i = 0; i < files.size(); i++) {
                boolean full = evaluator.eval(files.get(i));
                Assertions.assertEquals(full, evaluator.eval(cachedAll.get(i)), expr + " on file " + i);
                Assertions.assertEquals(evaluator.eval(files.get(i).copyWithStats(someIds)),
                        evaluator.eval(cachedSome.get(i)), expr + " on file " + i);
                pruned += full ? 0 : 1;
            }
        }
        // both outcomes are exercised
        Assertions.assertTrue(pruned > 0 && pruned < 1500 * files.size());
    }

    @Test
    public void testColumnLimitKeepsPriorityColumnsFirst() {
        List<DataFile> files = dataFiles(new Random(3));
        ManifestColumnStats stats = build(files, Set.of(9, 12), 3);
        // both priority columns plus the lowest other field id
        Assertions.assertEquals(3, stats.columnCount());
        Assertions.assertEquals(Set.of(1, 12), stats.lowerBounds(0).keySet());
        Assertions.assertNotNull(stats.valueCounts(0).get(12));

        ManifestColumnStats onlyPriority = build(files, Set.of(12), 0);
        Assertions.assertEquals(1, onlyPriority.columnCount());

        ManifestColumnStats none = build(files, Set.of(), 0);
        Assertions.assertEquals(0, none.columnCount());
        Assertions.assertNull(none.lowerBounds(0));
        Assertions.assertNull(none.nullValueCounts(0));
    }

    @Test
    public void testMissingEntriesStayMissing() {
        ManifestColumnStats.Builder builder = ManifestColumnStats.builder(Set.of(), null, 10);
        // file 0: counts and an 8-byte bound for column 1; file 1: nothing for column 1, a 3-byte bound for column 2;
        // file 2: a 5-byte bound for column 2 (switches it to variable width)
        builder.add(dataFile(Map.of(1, 10L), Map.of(1, 0L), Map.of(1, bytes(8, 1)), Map.of(1, bytes(8, 2))));
        builder.add(dataFile(Map.of(), Map.of(), Map.of(2, bytes(3, 3)), Map.of()));
        builder.add(dataFile(Map.of(), Map.of(), Map.of(2, bytes(5, 4)), Map.of(2, bytes(5, 5))));
        ManifestColumnStats stats = builder.build();

        Assertions.assertEquals(0L, stats.nullValueCounts(0).get(1));
        // a file after the last one with a count must not read as a count of 0
        Assertions.assertNull(stats.nullValueCounts(1).get(1));
        Assertions.assertNull(stats.nullValueCounts(2).get(1));
        Assertions.assertFalse(stats.valueCounts(2).containsKey(1));
        Assertions.assertNull(stats.nanValueCounts(0).get(1));
        Assertions.assertNull(stats.lowerBounds(0).get(2));
        Assertions.assertEquals(bytes(3, 3), stats.lowerBounds(1).get(2));
        Assertions.assertNull(stats.upperBounds(1).get(2));
        Assertions.assertEquals(bytes(5, 4), stats.lowerBounds(2).get(2));
        Assertions.assertEquals(bytes(8, 2), stats.upperBounds(0).get(1));
        Assertions.assertEquals(Map.of(1, bytes(8, 1)), new HashMap<>(stats.lowerBounds(0)));
    }

    @Test
    public void testCollectionHoldsAtMostTheLimit() {
        // 400 columns; each file carries a different slice, the lowest ids arrive last
        int columns = 400;
        int limit = 10;
        Set<Integer> priority = Set.of(390, 5000);
        ManifestColumnStats.Builder builder = ManifestColumnStats.builder(priority, null, limit);
        List<DataFile> files = new ArrayList<>();
        for (int f = 0; f < 40; f++) {
            Map<Integer, Long> counts = new HashMap<>();
            Map<Integer, ByteBuffer> bounds = new HashMap<>();
            for (int id = columns - 10 * f; id > columns - 10 * f - 30 && id > 0; id--) {
                counts.put(id, 10L);
                bounds.put(id, bytes(4, id));
            }
            DataFile file = dataFile(counts, counts, bounds, bounds);
            files.add(file);
            builder.add(file);
            // the priority column seen so far plus at most `limit` others
            Assertions.assertTrue(builder.collectedColumnCount() <= 1 + limit, "file " + f);
        }
        ManifestColumnStats stats = builder.build();
        // priority 390 plus the 9 lowest other ids
        Assertions.assertEquals(limit, stats.columnCount());
        Set<Integer> kept = new TreeSet<>();
        for (int i = 0; i < files.size(); i++) {
            kept.addAll(stats.valueCounts(i).keySet());
        }
        Assertions.assertEquals(Set.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 390), kept);
        for (int i = 0; i < files.size(); i++) {
            for (int id : kept) {
                Assertions.assertEquals(files.get(i).lowerBounds().get(id), stats.lowerBounds(i).get(id));
                Assertions.assertEquals(files.get(i).valueCounts().get(id), stats.valueCounts(i).get(id));
            }
        }
    }

    @Test
    public void testCandidateRanksDecideTheKeptColumns() {
        List<DataFile> files = dataFiles(new Random(5));
        // only ids 2, 7, 11 are candidates, ranked 11 < 2 < 7
        ManifestColumnStats.Builder builder = ManifestColumnStats.builder(Set.of(), Map.of(2, 20L, 7, 30L, 11, 10L), 2);
        files.forEach(builder::add);
        ManifestColumnStats stats = builder.build();
        Assertions.assertEquals(Set.of(2, 11), stats.lowerBounds(1).keySet());
    }

    @Test
    public void testRetainedBytesMatchesTheHeap() {
        List<DataFile> files = dataFiles(new Random(11));
        ManifestColumnStats stats = build(files, Set.of(), 100);
        long actual = GraphLayout.parseInstance(stats).totalSize();
        Assertions.assertEquals(actual, stats.retainedBytes(), actual * 0.1);

        // longer bounds on the last column only must be weighed in full
        List<DataFile> longer = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            DataFile file = files.get(i);
            Map<Integer, ByteBuffer> lower = new HashMap<>(file.lowerBounds());
            Map<Integer, ByteBuffer> upper = new HashMap<>(file.upperBounds());
            lower.put(12, bytes(200, i));
            upper.put(12, bytes(200, i + 1));
            longer.add(dataFile(file.columnSizes(), file.valueCounts(), file.nullValueCounts(), lower, upper));
        }
        ManifestColumnStats longerStats = build(longer, Set.of(), 100);
        long longerActual = GraphLayout.parseInstance(longerStats).totalSize();
        // the packed 200-byte values replace 4-byte ints: at least 2 x 196 bytes more per file
        Assertions.assertTrue(longerStats.retainedBytes() - stats.retainedBytes() >= 2L * 196 * FILES);
        Assertions.assertEquals(longerActual, longerStats.retainedBytes(), longerActual * 0.1);
    }

    // Column sizes differ from file to file, so they encode as one int[] per column: about 4 bytes per kept column
    // per file on top of the bounds and counts.
    @Test
    public void testColumnSizesCostAnIntPerColumnPerFile() {
        int columns = 100;
        Random random = new Random(19);
        List<DataFile> withSizes = new ArrayList<>();
        List<DataFile> withoutSizes = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            Map<Integer, Long> sizes = new HashMap<>();
            Map<Integer, Long> values = new HashMap<>();
            Map<Integer, Long> nulls = new HashMap<>();
            Map<Integer, ByteBuffer> lower = new HashMap<>();
            Map<Integer, ByteBuffer> upper = new HashMap<>();
            for (int id = 1; id <= columns; id++) {
                sizes.put(id, 1000L + random.nextInt(1_000_000));
                values.put(id, 10L);
                nulls.put(id, (long) random.nextInt(3));
                lower.put(id, Conversions.toByteBuffer(Types.LongType.get(), (long) random.nextInt(1000)));
                upper.put(id, Conversions.toByteBuffer(Types.LongType.get(), 1000L + random.nextInt(1000)));
            }
            withSizes.add(dataFile(sizes, values, nulls, lower, upper));
            withoutSizes.add(dataFile(null, values, nulls, lower, upper));
        }
        ManifestColumnStats with = build(withSizes, Set.of(), columns);
        ManifestColumnStats without = build(withoutSizes, Set.of(), columns);
        long withHeap = GraphLayout.parseInstance(with).totalSize();
        long withoutHeap = GraphLayout.parseInstance(without).totalSize();
        double perFile = (double) (withHeap - withoutHeap) / FILES;
        System.out.printf("column sizes: %d B/file without, +%.1f B/file with (%d columns, %d files)%n",
                withoutHeap / FILES, perFile, columns, FILES);
        Assertions.assertEquals(4.0 * columns, perFile, 0.1 * 4 * columns);
        // the sizes are weighed exactly; the whole store stays within a few bytes of the heap
        Assertions.assertEquals(withHeap - withoutHeap, with.retainedBytes() - without.retainedBytes());
        Assertions.assertEquals(withHeap, with.retainedBytes(), 64);
    }

    @Test
    public void testExpectedFileCountOnlySizesTheArrays() {
        List<DataFile> files = dataFiles(new Random(13));
        ManifestColumnStats unsized = build(files, Set.of(), 100);
        for (int expected : new int[] {files.size(), files.size() / 3, files.size() * 2}) {
            ManifestColumnStats stats = build(files, Set.of(), 100, expected);
            for (int i = 0; i < files.size(); i++) {
                DataFile source = files.get(i);
                assertSameMap(source.lowerBounds(), stats.lowerBounds(i));
                assertSameMap(source.upperBounds(), stats.upperBounds(i));
                assertSameMap(source.valueCounts(), stats.valueCounts(i));
                assertSameMap(source.nullValueCounts(), stats.nullValueCounts(i));
                assertSameMap(source.nanValueCounts(), stats.nanValueCounts(i));
                assertSameMap(source.columnSizes(), stats.columnSizes(i));
            }
            Assertions.assertEquals(unsized.retainedBytes(), stats.retainedBytes());
            long actual = GraphLayout.parseInstance(stats).totalSize();
            Assertions.assertEquals(actual, stats.retainedBytes(), actual * 0.1);
        }
    }

    @Test
    public void testCachedFileCannotBeCopiedWithStatistics() {
        List<DataFile> files = dataFiles(new Random(17));
        DataFile cached = cache(files, Set.of(), 100).get(0);
        Assertions.assertThrows(UnsupportedOperationException.class, cached::copy);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> cached.copyWithStats(Set.of(1)));
        DataFile copy = cached.copyWithoutStats();
        Assertions.assertEquals(files.get(0).location(), copy.location());
        Assertions.assertNull(copy.lowerBounds());
        Assertions.assertNull(copy.columnSizes());
        Assertions.assertEquals(files.get(0).columnSizes(), cached.columnSizes());
        // a plain wrapper still copies
        Assertions.assertEquals(files.get(0).lowerBounds(), DataFileWrapper.wrap(files.get(0)).copy().lowerBounds());
    }

    @Test
    public void testEncodingsReturnTheWrittenStatistics() {
        // 1: same bound in every file; 2: int bounds in some files only; 3: counts above Integer.MAX_VALUE;
        // 4: same string in every file; 5: strings in some files only; 6: counts only
        List<DataFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            long rows = 100 + i;
            Map<Integer, Long> values = new HashMap<>(Map.of(1, rows, 2, rows, 4, rows, 5, rows, 6, rows));
            Map<Integer, Long> nulls = new HashMap<>(Map.of(1, 0L, 2, 0L, 4, 0L, 6, (long) i));
            values.put(3, 3_000_000_000L + i);
            nulls.put(3, i % 2 == 0 ? 0L : 3_000_000_000L);
            Map<Integer, ByteBuffer> lower = new HashMap<>(Map.of(1, bytes(8, 1), 4, bytes(40, 4)));
            Map<Integer, ByteBuffer> upper = new HashMap<>(Map.of(1, bytes(8, 2), 4, bytes(40, 5)));
            if (i % 3 != 0) {
                lower.put(2, bytes(4, i));
                upper.put(2, bytes(4, i + 1));
                lower.put(5, bytes(5 + i % 30, i));
                upper.put(5, bytes(5 + i % 30, i + 1));
            }
            files.add(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("/path/to/e-" + i + ".parquet")
                    .withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(rows)
                    .withMetrics(new Metrics(rows, null, values, nulls, null, lower, upper))
                    .build());
        }
        ManifestColumnStats stats = build(files, Set.of(), 100);
        for (int i = 0; i < files.size(); i++) {
            DataFile source = files.get(i);
            assertSameMap(source.lowerBounds(), stats.lowerBounds(i));
            assertSameMap(source.upperBounds(), stats.upperBounds(i));
            assertSameMap(source.valueCounts(), stats.valueCounts(i));
            assertSameMap(source.nullValueCounts(), stats.nullValueCounts(i));
            assertSameMap(source.nanValueCounts(), stats.nanValueCounts(i));
            assertSameMap(source.columnSizes(), stats.columnSizes(i));
        }
        // every get() returns a fresh buffer, so a caller cannot modify the shared values
        stats.lowerBounds(1).get(5).put(0, (byte) 0x7F);
        Assertions.assertEquals(files.get(1).lowerBounds().get(5), stats.lowerBounds(1).get(5));
        long actual = GraphLayout.parseInstance(stats).totalSize();
        Assertions.assertEquals(actual, stats.retainedBytes(), actual * 0.1);
    }

    @Test
    public void testTruncatedBoundsStayInclusive() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "s", Types.StringType.get()),
                Types.NestedField.optional(2, "b", Types.BinaryType.get()));
        Random random = new Random(19);
        // 1-, 2-, 3- and 4-byte UTF-8 characters
        String alphabet = new String(new int[] {'a', 'b', 0xE9, 0x4E2D, 0x1F600}, 0, 5);
        List<DataFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            String low = randomString(random, alphabet, 5 + random.nextInt(30));
            String high = low + randomString(random, alphabet, random.nextInt(10));
            byte[] binaryLow = new byte[5 + random.nextInt(30)];
            random.nextBytes(binaryLow);
            byte[] binaryHigh = new byte[24];
            // all 0xFF past 16 bytes in some files: no truncated upper bound exists
            Arrays.fill(binaryHigh, i % 10 == 0 ? (byte) 0xFF : (byte) (0x80 + random.nextInt(0x70)));
            binaryLow[0] = 0; // below every upper bound, which starts at 0x80 or above
            files.add(dataFile(Map.of(1, 10L, 2, 10L), Map.of(1, 0L, 2, 0L),
                    Map.of(1, Conversions.toByteBuffer(Types.StringType.get(), low), 2, ByteBuffer.wrap(binaryLow)),
                    Map.of(1, Conversions.toByteBuffer(Types.StringType.get(), high), 2, ByteBuffer.wrap(binaryHigh))));
        }
        ManifestColumnStats.Builder builder = ManifestColumnStats.builder(Set.of(), null, 100, 0,
                Map.of(1, Type.TypeID.STRING, 2, Type.TypeID.BINARY));
        files.forEach(builder::add);
        ManifestColumnStats stats = builder.build();
        for (int i = 0; i < files.size(); i++) {
            String low = Conversions.fromByteBuffer(Types.StringType.get(), files.get(i).lowerBounds().get(1)).toString();
            String high = Conversions.fromByteBuffer(Types.StringType.get(), files.get(i).upperBounds().get(1)).toString();
            String cachedLow = Conversions.fromByteBuffer(Types.StringType.get(), stats.lowerBounds(i).get(1)).toString();
            String cachedHigh = Conversions.fromByteBuffer(Types.StringType.get(), stats.upperBounds(i).get(1)).toString();
            Assertions.assertTrue(cachedLow.codePointCount(0, cachedLow.length()) <= 16 && low.startsWith(cachedLow));
            Assertions.assertTrue(cachedHigh.codePointCount(0, cachedHigh.length()) <= 16);
            Assertions.assertTrue(Comparators.charSequences().compare(high, cachedHigh) <= 0);
            Assertions.assertTrue(stats.lowerBounds(i).get(2).remaining() <= 16);
            ByteBuffer cachedBinaryHigh = stats.upperBounds(i).get(2);
            if (i % 10 == 0) {
                Assertions.assertNull(cachedBinaryHigh);
            } else {
                Assertions.assertTrue(cachedBinaryHigh.remaining() <= 16);
                Assertions.assertTrue(Comparators.unsignedBytes().compare(
                        files.get(i).upperBounds().get(2), cachedBinaryHigh) <= 0);
            }
        }
        // pruning only gets weaker: a file the full statistics keep is never dropped
        int weaker = 0;
        for (int p = 0; p < 500; p++) {
            String value = randomString(random, alphabet, 1 + random.nextInt(30));
            Expression expr = switch (random.nextInt(5)) {
                case 0 -> Expressions.lessThan("s", value);
                case 1 -> Expressions.greaterThanOrEqual("s", value);
                case 2 -> Expressions.equal("s", value);
                case 3 -> Expressions.startsWith("s", value);
                default -> Expressions.notStartsWith("s", value);
            };
            InclusiveMetricsEvaluator evaluator = new InclusiveMetricsEvaluator(schema, expr);
            for (int i = 0; i < files.size(); i++) {
                boolean full = evaluator.eval(files.get(i));
                boolean cached = evaluator.eval(DataFileWrapper.wrap(files.get(i).copyWithoutStats(), stats, i));
                Assertions.assertTrue(!full || cached, expr + " on file " + i);
                weaker += full == cached ? 0 : 1;
            }
        }
        Assertions.assertTrue(weaker < 500 * files.size() / 2);
    }

    private static String randomString(Random random, String alphabet, int codePoints) {
        int[] points = alphabet.codePoints().toArray();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codePoints; i++) {
            sb.appendCodePoint(points[random.nextInt(points.length)]);
        }
        return sb.toString();
    }

    private static ManifestColumnStats build(List<DataFile> files, Set<Integer> priority, int limit) {
        return build(files, priority, limit, 0);
    }

    private static ManifestColumnStats build(List<DataFile> files, Set<Integer> priority, int limit,
                                             int expectedFileCount) {
        ManifestColumnStats.Builder builder =
                ManifestColumnStats.builder(priority, null, limit, expectedFileCount, null);
        for (int i = 0; i < files.size(); i++) {
            Assertions.assertEquals(i, builder.add(files.get(i)));
        }
        return builder.build();
    }

    private static List<DataFile> cache(List<DataFile> files, Set<Integer> priority, int limit) {
        ManifestColumnStats stats = build(files, priority, limit);
        List<DataFile> cached = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            cached.add(DataFileWrapper.wrap(files.get(i).copyWithoutStats(), stats, i));
        }
        return cached;
    }

    private static <V> void assertSameMap(Map<Integer, V> expected, Map<Integer, V> actual) {
        Map<Integer, V> expectedOrEmpty = expected == null ? Map.of() : expected;
        Assertions.assertEquals(expectedOrEmpty, new HashMap<>(actual));
        Assertions.assertEquals(expectedOrEmpty.size(), actual.size());
        for (Map.Entry<Integer, V> entry : expectedOrEmpty.entrySet()) {
            Assertions.assertEquals(entry.getValue(), actual.get(entry.getKey()));
            Assertions.assertTrue(actual.containsKey(entry.getKey()));
        }
        Assertions.assertNull(actual.get(1000));
    }

    private static ByteBuffer bytes(int length, int seed) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (seed * 31 + i);
        }
        return ByteBuffer.wrap(bytes);
    }

    private static DataFile dataFile(Map<Integer, Long> valueCounts, Map<Integer, Long> nullCounts,
                                     Map<Integer, ByteBuffer> lower, Map<Integer, ByteBuffer> upper) {
        return dataFile(null, valueCounts, nullCounts, lower, upper);
    }

    private static DataFile dataFile(Map<Integer, Long> sizes, Map<Integer, Long> valueCounts,
                                     Map<Integer, Long> nullCounts, Map<Integer, ByteBuffer> lower,
                                     Map<Integer, ByteBuffer> upper) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/f" + System.nanoTime() + ".parquet")
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(10)
                .withMetrics(new Metrics(10L, sizes, valueCounts, nullCounts, null, lower, upper))
                .build();
    }

    private static long ts(int i) {
        return 1_700_000_000_000_000L + i * 3_600_000_000L;
    }

    private static List<DataFile> dataFiles(Random random) {
        List<DataFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            Map<Integer, Long> values = new HashMap<>();
            Map<Integer, Long> nulls = new HashMap<>();
            Map<Integer, Long> nans = new HashMap<>();
            Map<Integer, Long> sizes = new HashMap<>();
            Map<Integer, ByteBuffer> lower = new HashMap<>();
            Map<Integer, ByteBuffer> upper = new HashMap<>();
            long rows = 1000;
            for (Types.NestedField column : SCHEMA.columns()) {
                int id = column.fieldId();
                if (id == 9 && i % 3 == 0) {
                    continue; // no statistics for this column in these files
                }
                values.put(id, rows);
                // column 3 has the same size in every file; column 4 sizes exceed an int
                sizes.put(id, id == 3 ? 4096L : id == 4 ? 5_000_000_000L + i : 100L + random.nextInt(10000));
                if (id == 8 && i % 5 == 0) {
                    nulls.put(id, rows); // all null: counts, no bounds
                    continue;
                }
                nulls.put(id, random.nextInt(4) == 0 ? 0L : random.nextInt(50));
                Object low;
                Object high;
                switch (id) {
                    case 1 -> {
                        low = i * 1000L;
                        high = i * 1000L + 999;
                    }
                    case 2 -> {
                        low = random.nextInt(10);
                        high = 1000 + random.nextInt(10);
                    }
                    case 3 -> {
                        low = 8000 + i / 10;
                        high = 8000 + i / 10 + 1;
                    }
                    case 4 -> {
                        low = ts(i);
                        high = ts(i) + 3_599_999_999L;
                    }
                    case 5 -> {
                        low = -1.0 + random.nextDouble();
                        high = 100 + random.nextDouble();
                        nans.put(id, (long) random.nextInt(3));
                    }
                    case 6 -> {
                        // variable length: some bounds fit in 8 bytes, most do not
                        low = i % 7 == 0 ? "k" + i : String.format("key-%06d", i);
                        high = String.format("key-%06d~", i);
                    }
                    case 7 -> {
                        low = new BigDecimal(random.nextInt(100) + ".25");
                        high = new BigDecimal(9000 + random.nextInt(100) + ".75");
                    }
                    case 8 -> {
                        low = (long) random.nextInt(100);
                        high = 1_000_000L + random.nextInt(100);
                    }
                    case 9 -> {
                        low = i;
                        high = i + 10;
                    }
                    case 10 -> {
                        low = false;
                        high = true;
                    }
                    case 11 -> {
                        low = -5f + random.nextFloat();
                        high = 50f + random.nextFloat();
                        nans.put(id, 0L);
                    }
                    default -> {
                        low = (i - FILES / 2) * 10;
                        high = (i - FILES / 2) * 10 + 9;
                    }
                }
                lower.put(id, Conversions.toByteBuffer(column.type(), low));
                upper.put(id, Conversions.toByteBuffer(column.type(), high));
            }
            files.add(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("/path/to/data-" + i + ".parquet")
                    .withFormat(FileFormat.PARQUET).withFileSizeInBytes(1 << 20).withRecordCount(rows)
                    .withMetrics(new Metrics(rows, sizes, values, nulls, nans, lower, upper))
                    .build());
        }
        return files;
    }

    private static Expression randomPredicate(Random random) {
        int id = 1 + random.nextInt(12);
        String name = SCHEMA.findField(id).name();
        Object value = switch (id) {
            case 1 -> (long) random.nextInt(FILES * 1000 + 100_000) - 50_000;
            case 2 -> random.nextInt(1100) - 20;
            case 3 -> 7990 + random.nextInt(FILES / 10 + 20);
            case 4 -> ts(random.nextInt(FILES + 100) - 50) + random.nextInt(1000);
            case 5 -> random.nextDouble() * 120 - 10;
            case 6 -> String.format("key-%06d", random.nextInt(FILES + 100)) + (random.nextBoolean() ? "" : "a");
            case 7 -> new BigDecimal(random.nextInt(10000) + ".50");
            case 8 -> (long) random.nextInt(1_100_000);
            case 9 -> random.nextInt(FILES + 100);
            case 10 -> random.nextBoolean();
            case 11 -> random.nextFloat() * 60 - 10;
            default -> random.nextInt(FILES * 10 + 100) - FILES * 5 - 50;
        };
        boolean floating = id == 5 || id == 11;
        return switch (random.nextInt(12)) {
            case 0 -> Expressions.lessThan(name, value);
            case 1 -> Expressions.lessThanOrEqual(name, value);
            case 2 -> Expressions.greaterThan(name, value);
            case 3 -> Expressions.greaterThanOrEqual(name, value);
            case 4 -> Expressions.equal(name, value);
            case 5 -> Expressions.notEqual(name, value);
            case 6 -> Expressions.in(name, value, value);
            case 7 -> Expressions.notIn(name, value);
            case 8 -> Expressions.isNull(name);
            case 9 -> Expressions.notNull(name);
            case 10 -> floating ? Expressions.isNaN(name) : Expressions.equal(name, value);
            default -> {
                if (id == 6) {
                    String prefix = ((String) value).substring(0, 8);
                    yield random.nextBoolean() ? Expressions.startsWith(name, prefix)
                            : Expressions.notStartsWith(name, prefix);
                }
                yield floating ? Expressions.notNaN(name) : Expressions.lessThan(name, value);
            }
        };
    }
}
