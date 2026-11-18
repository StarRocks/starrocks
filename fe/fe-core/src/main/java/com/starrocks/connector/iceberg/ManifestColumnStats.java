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

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

/**
 * Column statistics of the data files of one manifest, stored column by column for the manifest cache.
 * <p>
 * Iceberg's BaseFile keeps per-file statistics in six boxed {@code Map<Integer, ...>}s, about 500 bytes per column
 * per file; this layout keeps the same values in primitive arrays, encoded per column in the cheapest exact form
 * (see {@link Column}). Each cached data file reads its statistics through read-only map views
 * ({@link #lowerBounds(int)} etc.) that return the values Iceberg wrote, except that STRING and BINARY bounds longer
 * than {@link #TRUNCATE_LENGTH} are truncated the way Iceberg's default metrics mode truncates them; scans that need
 * exact statistics bypass the cache.
 */
public final class ManifestColumnStats {
    private static final long ABSENT = -1L;
    public static final int TRUNCATE_LENGTH = 16;

    private final int[] columnIds;
    private final Column[] columns;
    private final long retainedBytes;

    private ManifestColumnStats(int[] columnIds, Column[] columns, Longs recordCounts) {
        this.columnIds = columnIds;
        this.columns = columns;
        long bytes = OBJECT_BYTES + intArrayBytes(columnIds.length) + refArrayBytes(columns.length);
        boolean recordCountsUsed = false;
        for (Column column : columns) {
            bytes += column.retainedBytes();
            recordCountsUsed |= column.usesRecordCounts();
        }
        // the record counts are shared by every column whose value counts equal them; counted once
        this.retainedBytes = bytes + (recordCountsUsed ? recordCounts.retainedBytes() : 0);
    }

    /**
     * @param priorityColumnIds columns whose statistics are always kept
     * @param candidateRanks the other columns that may be kept, lower rank first, or null for any column ranked by
     *                       field id
     * @param columnLimit how many columns to keep in total; priority columns are kept even beyond it
     */
    public static Builder builder(Set<Integer> priorityColumnIds, Map<Integer, Long> candidateRanks, int columnLimit) {
        return builder(priorityColumnIds, candidateRanks, columnLimit, 0, null);
    }

    /**
     * @param expectedFileCount the number of files that will be added, 0 if unknown; when it is right, every
     *                          collecting array is allocated once at its final size
     * @param truncatedTypes the STRING and BINARY columns, whose bounds longer than {@link #TRUNCATE_LENGTH}
     *                       characters (bytes) are truncated like Iceberg's default {@code truncate(16)} metrics
     *                       mode; null keeps every bound as written
     */
    public static Builder builder(Set<Integer> priorityColumnIds, Map<Integer, Long> candidateRanks, int columnLimit,
                                  int expectedFileCount, Map<Integer, Type.TypeID> truncatedTypes) {
        return new Builder(priorityColumnIds, candidateRanks, columnLimit, expectedFileCount, truncatedTypes);
    }

    public int columnCount() {
        return columnIds.length;
    }

    // Heap retained by these statistics, computed once at build time for the cache weigher: the store is shared by
    // every data file of the manifest, so sampling it through the files (or sampling its columns) misweighs it.
    public long retainedBytes() {
        return retainedBytes;
    }

    // Null when no column is kept, matching a data file copied without statistics.
    public Map<Integer, ByteBuffer> lowerBounds(int file) {
        return columns.length == 0 ? null : new BoundMap(file, true);
    }

    public Map<Integer, ByteBuffer> upperBounds(int file) {
        return columns.length == 0 ? null : new BoundMap(file, false);
    }

    public Map<Integer, Long> valueCounts(int file) {
        return columns.length == 0 ? null : new CountMap(file, CountKind.VALUE);
    }

    public Map<Integer, Long> nullValueCounts(int file) {
        return columns.length == 0 ? null : new CountMap(file, CountKind.NULL);
    }

    public Map<Integer, Long> nanValueCounts(int file) {
        return columns.length == 0 ? null : new CountMap(file, CountKind.NAN);
    }

    public Map<Integer, Long> columnSizes(int file) {
        return columns.length == 0 ? null : new CountMap(file, CountKind.SIZE);
    }

    private Column column(Object key) {
        if (!(key instanceof Integer)) {
            return null;
        }
        int pos = Arrays.binarySearch(columnIds, (Integer) key);
        return pos < 0 ? null : columns[pos];
    }

    private enum CountKind {
        VALUE, NULL, NAN, SIZE
    }

    // Statistics of one column. Each part is encoded at build time in the cheapest exact form for this manifest:
    // a value shared by every file is stored once, counts equal to the file's record count are not stored, counts
    // that fit use an int[], fixed-width bounds use primitive arrays and variable-width bounds share one byte[].
    // A null part has no entry in any file.
    private static final class Column {
        final Bounds lower;
        final Bounds upper;
        final Longs valueCounts;
        final Longs nullCounts;
        final Longs nanCounts;
        final Longs sizes;

        Column(ColumnBuilder b, int fileCount, Longs recordCounts) {
            if (b.width > 0) {
                this.lower = FixedBounds.encode(b.width, b.lower, b.hasLower, fileCount);
                this.upper = FixedBounds.encode(b.width, b.upper, b.hasUpper, fileCount);
            } else {
                // variable-width bounds, or no bound at all (counts only)
                this.lower = VarBounds.encode(b.lowerBytes, fileCount);
                this.upper = VarBounds.encode(b.upperBytes, fileCount);
            }
            this.valueCounts = Longs.encode(b.valueCounts, fileCount, recordCounts);
            this.nullCounts = Longs.encode(b.nullCounts, fileCount, null);
            this.nanCounts = Longs.encode(b.nanCounts, fileCount, null);
            this.sizes = Longs.encode(b.sizes, fileCount, null);
        }

        ByteBuffer bound(int file, boolean isLower) {
            Bounds bounds = isLower ? lower : upper;
            return bounds == null ? null : bounds.get(file);
        }

        Long count(int file, CountKind kind) {
            Longs counts;
            switch (kind) {
                case VALUE:
                    counts = valueCounts;
                    break;
                case NULL:
                    counts = nullCounts;
                    break;
                case NAN:
                    counts = nanCounts;
                    break;
                default:
                    counts = sizes;
            }
            if (counts == null) {
                return null;
            }
            long count = counts.get(file);
            return count == ABSENT ? null : count;
        }

        boolean usesRecordCounts() {
            return valueCounts instanceof RecordCountLongs;
        }

        long retainedBytes() {
            return COLUMN_BYTES + retained(lower) + retained(upper) + retained(valueCounts) + retained(nullCounts) +
                    retained(nanCounts) + retained(sizes);
        }

        private static long retained(Bounds bounds) {
            return bounds == null ? 0 : bounds.retainedBytes();
        }

        private static long retained(Longs longs) {
            return longs == null ? 0 : longs.retainedBytes();
        }
    }

    // Per-file counts (or record counts), ABSENT for a file without an entry.
    private abstract static class Longs {
        abstract long get(int file);

        abstract long retainedBytes();

        // `raw` may be shorter than fileCount: the remaining files have no entry. Returns null when no file has one.
        static Longs encode(long[] raw, int fileCount, Longs recordCounts) {
            if (raw == null || fileCount == 0) {
                return null;
            }
            long first = valueAt(raw, 0);
            boolean anyPresent = false;
            boolean allPresent = true;
            boolean allSame = true;
            boolean fitsInt = true;
            boolean equalsRecordCount = recordCounts != null;
            for (int i = 0; i < fileCount; i++) {
                long value = valueAt(raw, i);
                if (value == ABSENT) {
                    allPresent = false;
                } else {
                    anyPresent = true;
                }
                allSame &= value == first;
                fitsInt &= value >= ABSENT && value <= Integer.MAX_VALUE;
                if (equalsRecordCount && value != recordCounts.get(i)) {
                    equalsRecordCount = false;
                }
            }
            if (!anyPresent) {
                return null;
            }
            if (allPresent && allSame) {
                return new ConstLongs(first);
            }
            if (allPresent && equalsRecordCount) {
                return new RecordCountLongs(recordCounts);
            }
            if (fitsInt) {
                int[] values = new int[fileCount];
                for (int i = 0; i < fileCount; i++) {
                    values[i] = (int) valueAt(raw, i);
                }
                return new IntLongs(values);
            }
            long[] values = Arrays.copyOf(raw, fileCount);
            if (raw.length < fileCount) {
                Arrays.fill(values, raw.length, fileCount, ABSENT);
            }
            return new ArrayLongs(values);
        }

        private static long valueAt(long[] raw, int i) {
            return i < raw.length ? raw[i] : ABSENT;
        }
    }

    private static final class ConstLongs extends Longs {
        private final long value;

        ConstLongs(long value) {
            this.value = value;
        }

        @Override
        long get(int file) {
            return value;
        }

        @Override
        long retainedBytes() {
            return 24;
        }
    }

    // value counts equal to the record count of every file (any non-nested column); shares the store's record counts
    private static final class RecordCountLongs extends Longs {
        private final Longs recordCounts;

        RecordCountLongs(Longs recordCounts) {
            this.recordCounts = recordCounts;
        }

        @Override
        long get(int file) {
            return recordCounts.get(file);
        }

        @Override
        long retainedBytes() {
            return 16;
        }
    }

    private static final class IntLongs extends Longs {
        private final int[] values;

        IntLongs(int[] values) {
            this.values = values;
        }

        @Override
        long get(int file) {
            return values[file];
        }

        @Override
        long retainedBytes() {
            return 16 + intArrayBytes(values.length);
        }
    }

    private static final class ArrayLongs extends Longs {
        private final long[] values;

        ArrayLongs(long[] values) {
            this.values = values;
        }

        @Override
        long get(int file) {
            return values[file];
        }

        @Override
        long retainedBytes() {
            return 16 + longArrayBytes(values);
        }
    }

    // Per-file bounds. Every call returns a new buffer, so callers cannot modify the shared values.
    private abstract static class Bounds {
        abstract ByteBuffer get(int file);

        abstract long retainedBytes();
    }

    // Bounds of the same length of at most 8 bytes (ints, longs, dates, timestamps, floats, doubles, short strings)
    // as little-endian integers: one value when every file has the same bound, an int[] up to 4 bytes, else a long[].
    private static final class FixedBounds extends Bounds {
        private final int width;
        private final long constant;
        private final int[] ints;
        private final long[] longs;
        // null when every file has a bound
        private final BitSet present;

        private FixedBounds(int width, long constant, int[] ints, long[] longs, BitSet present) {
            this.width = width;
            this.constant = constant;
            this.ints = ints;
            this.longs = longs;
            this.present = present;
        }

        static Bounds encode(int width, long[] values, BitSet present, int fileCount) {
            int first = present.nextSetBit(0);
            if (first < 0 || first >= fileCount) {
                return null;
            }
            boolean allSame = true;
            for (int i = present.nextSetBit(first); i >= 0 && i < fileCount; i = present.nextSetBit(i + 1)) {
                allSame &= values[i] == values[first];
            }
            // clone() trims the words to the highest set bit, which bitSetBytes() assumes
            BitSet presence = present.cardinality() == fileCount ? null : (BitSet) present.clone();
            if (allSame) {
                return new FixedBounds(width, values[first], null, null, presence);
            }
            if (width <= 4) {
                int[] ints = new int[fileCount];
                for (int i = 0; i < Math.min(values.length, fileCount); i++) {
                    ints[i] = (int) values[i];
                }
                return new FixedBounds(width, 0, ints, null, presence);
            }
            return new FixedBounds(width, 0, null, values.length == fileCount ? values :
                    Arrays.copyOf(values, fileCount), presence);
        }

        @Override
        ByteBuffer get(int file) {
            if (present != null && !present.get(file)) {
                return null;
            }
            long value = ints != null ? ints[file] & 0xFFFFFFFFL : longs != null ? longs[file] : constant;
            return decode(value, width);
        }

        @Override
        long retainedBytes() {
            return 40 + (ints == null ? 0 : intArrayBytes(ints.length)) + longArrayBytes(longs) + bitSetBytes(present);
        }
    }

    // Bounds of any length: one value when every file has the same bound, else all values packed in one byte[]
    // with an offset per file.
    private static final class VarBounds extends Bounds {
        private final byte[] constant;
        private final byte[] data;
        private final int[] offsets;
        // null when every file has a bound
        private final BitSet present;

        private VarBounds(byte[] constant, byte[] data, int[] offsets, BitSet present) {
            this.constant = constant;
            this.data = data;
            this.offsets = offsets;
            this.present = present;
        }

        static Bounds encode(byte[][] values, int fileCount) {
            if (values == null) {
                return null;
            }
            BitSet present = new BitSet();
            byte[] first = null;
            boolean allSame = true;
            int total = 0;
            for (int i = 0; i < Math.min(values.length, fileCount); i++) {
                if (values[i] != null) {
                    present.set(i);
                    if (first == null) {
                        first = values[i];
                    } else {
                        allSame &= Arrays.equals(first, values[i]);
                    }
                    total += values[i].length;
                }
            }
            if (first == null) {
                return null;
            }
            // clone() trims the words to the highest set bit, which bitSetBytes() assumes
            BitSet presence = present.cardinality() == fileCount ? null : (BitSet) present.clone();
            if (allSame) {
                return new VarBounds(first, null, null, presence);
            }
            byte[] data = new byte[total];
            int[] offsets = new int[fileCount + 1];
            int offset = 0;
            for (int i = 0; i < fileCount; i++) {
                offsets[i] = offset;
                byte[] value = i < values.length ? values[i] : null;
                if (value != null) {
                    System.arraycopy(value, 0, data, offset, value.length);
                    offset += value.length;
                }
            }
            offsets[fileCount] = offset;
            return new VarBounds(null, data, offsets, presence);
        }

        @Override
        ByteBuffer get(int file) {
            if (present != null && !present.get(file)) {
                return null;
            }
            if (constant != null) {
                return ByteBuffer.wrap(constant.clone());
            }
            return ByteBuffer.wrap(Arrays.copyOfRange(data, offsets[file], offsets[file + 1]));
        }

        @Override
        long retainedBytes() {
            return 32 + byteArrayBytes(constant) + byteArrayBytes(data) +
                    (offsets == null ? 0 : intArrayBytes(offsets.length)) + bitSetBytes(present);
        }
    }

    private abstract class ViewMap<V> extends AbstractMap<Integer, V> {
        final int file;

        ViewMap(int file) {
            this.file = file;
        }

        abstract V value(Column column);

        @Override
        public V get(Object key) {
            Column column = column(key);
            return column == null ? null : value(column);
        }

        @Override
        public boolean containsKey(Object key) {
            return get(key) != null;
        }

        @Override
        public Set<Entry<Integer, V>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<Integer, V>> iterator() {
                    return new Iterator<>() {
                        private int pos = advance(0);

                        private int advance(int from) {
                            int p = from;
                            while (p < columns.length && value(columns[p]) == null) {
                                p++;
                            }
                            return p;
                        }

                        @Override
                        public boolean hasNext() {
                            return pos < columns.length;
                        }

                        @Override
                        public Entry<Integer, V> next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            Entry<Integer, V> entry = new SimpleImmutableEntry<>(columnIds[pos], value(columns[pos]));
                            pos = advance(pos + 1);
                            return entry;
                        }
                    };
                }

                @Override
                public int size() {
                    int size = 0;
                    for (Column column : columns) {
                        if (value(column) != null) {
                            size++;
                        }
                    }
                    return size;
                }
            };
        }
    }

    private final class BoundMap extends ViewMap<ByteBuffer> {
        private final boolean isLower;

        BoundMap(int file, boolean isLower) {
            super(file);
            this.isLower = isLower;
        }

        @Override
        ByteBuffer value(Column column) {
            return column.bound(file, isLower);
        }
    }

    private final class CountMap extends ViewMap<Long> {
        private final CountKind kind;

        CountMap(int file, CountKind kind) {
            super(file);
            this.kind = kind;
        }

        @Override
        Long value(Column column) {
            return column.count(file, kind);
        }
    }

    // Shallow sizes with compressed oops: 12-byte object headers, 16-byte array headers, 8-byte alignment.
    private static final long OBJECT_BYTES = 24;
    private static final long COLUMN_BYTES = 40;

    private static long align(long bytes) {
        return (bytes + 7) & ~7L;
    }

    private static long intArrayBytes(int length) {
        return align(16 + 4L * length);
    }

    private static long refArrayBytes(int length) {
        return align(16 + 4L * length);
    }

    private static long longArrayBytes(long[] values) {
        return values == null ? 0 : align(16 + 8L * values.length);
    }

    private static long bitSetBytes(BitSet bits) {
        // object + its long[] words, which hold at least the highest set bit
        return bits == null ? 0 : 24 + align(16 + 8L * Math.max(1, (bits.length() + 63) / 64));
    }

    private static long byteArrayBytes(byte[] value) {
        return value == null ? 0 : align(16 + value.length);
    }

    private static long encode(ByteBuffer buffer) {
        long value = 0;
        int pos = buffer.position();
        for (int i = 0; i < buffer.remaining(); i++) {
            value |= (buffer.get(pos + i) & 0xFFL) << (8 * i);
        }
        return value;
    }

    private static ByteBuffer decode(long value, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = (byte) (value >>> (8 * i));
        }
        return ByteBuffer.wrap(bytes);
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    /**
     * Collects the statistics of the data files of one manifest as they are read. {@link #add} must be called once
     * per file; the returned index is what {@link DataFileWrapper} uses to read the file's statistics back.
     * <p>
     * {@link #build} keeps every priority column that has statistics, then fills up to {@code columnLimit} columns in
     * total with the other columns in rank order. While collecting, at most {@code columnLimit} non-priority
     * columns are held: the lowest ranks seen so far. A column evicted for a lower rank ranks above every remaining
     * candidate, so it can never become a final candidate again and nothing has to be re-read.
     */
    public static final class Builder {
        private final Set<Integer> priorityColumnIds;
        private final Map<Integer, Long> candidateRanks;
        private final int columnLimit;
        private final Map<Integer, ColumnBuilder> columns = new HashMap<>();
        // non-priority columns being collected (rank -> column id), at most columnLimit of them
        private final TreeMap<Long, Integer> candidates = new TreeMap<>();
        private final int expectedFileCount;
        private final Map<Integer, Type.TypeID> truncatedTypes;
        private long[] recordCounts;
        private int fileCount;

        private Builder(Set<Integer> priorityColumnIds, Map<Integer, Long> candidateRanks, int columnLimit,
                        int expectedFileCount, Map<Integer, Type.TypeID> truncatedTypes) {
            this.priorityColumnIds = priorityColumnIds == null ? Collections.emptySet() : priorityColumnIds;
            this.candidateRanks = candidateRanks;
            this.columnLimit = columnLimit;
            this.expectedFileCount = Math.max(expectedFileCount, 0);
            this.truncatedTypes = truncatedTypes == null ? Collections.emptyMap() : truncatedTypes;
        }

        public synchronized int add(ContentFile<?> file) {
            int index = fileCount++;
            recordCounts = ColumnBuilder.ensure(recordCounts, index, ABSENT, expectedFileCount);
            recordCounts[index] = file.recordCount();
            addBounds(file.lowerBounds(), index, true);
            addBounds(file.upperBounds(), index, false);
            addCounts(file.valueCounts(), index, CountKind.VALUE);
            addCounts(file.nullValueCounts(), index, CountKind.NULL);
            addCounts(file.nanValueCounts(), index, CountKind.NAN);
            addCounts(file.columnSizes(), index, CountKind.SIZE);
            return index;
        }

        // The builder of a column to collect, or null for a column build() cannot keep.
        private ColumnBuilder column(int columnId) {
            ColumnBuilder column = columns.get(columnId);
            if (column != null) {
                return column;
            }
            if (!priorityColumnIds.contains(columnId)) {
                Long rank = candidateRanks == null ? Long.valueOf(columnId) : candidateRanks.get(columnId);
                if (rank == null) {
                    return null;
                }
                if (candidates.size() >= columnLimit) {
                    if (columnLimit <= 0 || rank > candidates.lastKey()) {
                        return null;
                    }
                    columns.remove(candidates.pollLastEntry().getValue());
                }
                candidates.put(rank, columnId);
            }
            column = new ColumnBuilder(expectedFileCount);
            columns.put(columnId, column);
            return column;
        }

        private void addBounds(Map<Integer, ByteBuffer> bounds, int index, boolean isLower) {
            if (bounds == null) {
                return;
            }
            for (Map.Entry<Integer, ByteBuffer> entry : bounds.entrySet()) {
                ColumnBuilder column = entry.getValue() == null ? null : column(entry.getKey());
                ByteBuffer bound = column == null ? null : truncate(entry.getKey(), entry.getValue(), isLower);
                if (bound != null) {
                    column.setBound(index, bound, isLower);
                }
            }
        }

        private void addCounts(Map<Integer, Long> counts, int index, CountKind kind) {
            if (counts == null) {
                return;
            }
            for (Map.Entry<Integer, Long> entry : counts.entrySet()) {
                ColumnBuilder column = entry.getValue() == null || entry.getValue() < 0 ? null : column(entry.getKey());
                if (column != null) {
                    column.setCount(index, entry.getValue(), kind);
                }
            }
        }

        // A truncated lower bound is a prefix (<= the value) and a truncated upper bound is incremented past it
        // (>= the value), so pruning stays correct and only weaker; null when no upper bound can be formed.
        private ByteBuffer truncate(int columnId, ByteBuffer bound, boolean isLower) {
            Type.TypeID type = truncatedTypes.get(columnId);
            if (type == null || bound.remaining() <= TRUNCATE_LENGTH) {
                return bound;
            }
            if (type == Type.TypeID.BINARY) {
                Literal<ByteBuffer> value = Literal.of(bound);
                Literal<ByteBuffer> truncated = isLower ? BinaryUtil.truncateBinaryMin(value, TRUNCATE_LENGTH) :
                        BinaryUtil.truncateBinaryMax(value, TRUNCATE_LENGTH);
                return truncated == null ? null : truncated.toByteBuffer();
            }
            CharSequence chars;
            try {
                chars = StandardCharsets.UTF_8.newDecoder().decode(bound.duplicate());
            } catch (CharacterCodingException e) {
                return bound; // not valid UTF-8: keep it as written
            }
            Literal<CharSequence> value = Literal.of(chars);
            Literal<CharSequence> truncated = isLower ? UnicodeUtil.truncateStringMin(value, TRUNCATE_LENGTH) :
                    UnicodeUtil.truncateStringMax(value, TRUNCATE_LENGTH);
            return truncated == null ? null : truncated.toByteBuffer();
        }

        synchronized int collectedColumnCount() {
            return columns.size();
        }

        public synchronized ManifestColumnStats build() {
            List<Integer> kept = new ArrayList<>();
            for (Integer id : columns.keySet()) {
                if (priorityColumnIds.contains(id)) {
                    kept.add(id);
                }
            }
            for (Integer id : candidates.values()) {
                if (kept.size() >= columnLimit) {
                    break;
                }
                kept.add(id);
            }
            Collections.sort(kept);
            int[] ids = new int[kept.size()];
            Column[] built = new Column[kept.size()];
            Longs records = Longs.encode(recordCounts, fileCount, null);
            for (int i = 0; i < ids.length; i++) {
                ids[i] = kept.get(i);
                built[i] = new Column(columns.get(ids[i]), fileCount, records);
            }
            return new ManifestColumnStats(ids, built, records);
        }
    }

    private static final class ColumnBuilder {
        // the size arrays grow to first, so a right expected file count allocates each array once
        final int capacity;
        // -1 = no bound seen yet, 1..8 = fixed-width bounds stored as longs, 0 = variable-width bounds
        int width = -1;
        long[] lower = new long[0];
        long[] upper = new long[0];
        BitSet hasLower = new BitSet();
        BitSet hasUpper = new BitSet();
        byte[][] lowerBytes;
        byte[][] upperBytes;
        long[] valueCounts;
        long[] nullCounts;
        long[] nanCounts;
        long[] sizes;

        ColumnBuilder(int capacity) {
            this.capacity = capacity;
        }

        void setBound(int index, ByteBuffer buffer, boolean isLower) {
            int length = buffer.remaining();
            if (width == -1) {
                width = length >= 1 && length <= 8 ? length : 0;
            }
            if (width > 0 && length != width) {
                toVariableWidth();
            }
            if (width > 0) {
                long[] values = ensure(isLower ? lower : upper, index, 0L, capacity);
                values[index] = encode(buffer);
                if (isLower) {
                    lower = values;
                    hasLower.set(index);
                } else {
                    upper = values;
                    hasUpper.set(index);
                }
            } else {
                byte[][] values = ensure(isLower ? lowerBytes : upperBytes, index, capacity);
                values[index] = ManifestColumnStats.toBytes(buffer);
                if (isLower) {
                    lowerBytes = values;
                } else {
                    upperBytes = values;
                }
            }
        }

        private void toVariableWidth() {
            lowerBytes = toByteArrays(lower, hasLower);
            upperBytes = toByteArrays(upper, hasUpper);
            lower = null;
            upper = null;
            hasLower = null;
            hasUpper = null;
            width = 0;
        }

        private byte[][] toByteArrays(long[] values, BitSet present) {
            byte[][] bytes = new byte[values.length][];
            for (int i = present.nextSetBit(0); i >= 0; i = present.nextSetBit(i + 1)) {
                bytes[i] = ManifestColumnStats.toBytes(decode(values[i], width));
            }
            return bytes;
        }

        void setCount(int index, long count, CountKind kind) {
            switch (kind) {
                case VALUE:
                    valueCounts = ensure(valueCounts, index, ABSENT, capacity);
                    valueCounts[index] = count;
                    break;
                case NULL:
                    nullCounts = ensure(nullCounts, index, ABSENT, capacity);
                    nullCounts[index] = count;
                    break;
                case NAN:
                    nanCounts = ensure(nanCounts, index, ABSENT, capacity);
                    nanCounts[index] = count;
                    break;
                default:
                    sizes = ensure(sizes, index, ABSENT, capacity);
                    sizes[index] = count;
            }
        }

        static long[] ensure(long[] values, int index, long fill, int capacity) {
            if (values == null) {
                values = new long[0];
            }
            if (index < values.length) {
                return values;
            }
            int oldLength = values.length;
            long[] grown = Arrays.copyOf(values, Math.max(Math.max(index + 1, oldLength * 2), capacity));
            if (fill != 0L) {
                Arrays.fill(grown, oldLength, grown.length, fill);
            }
            return grown;
        }

        private static byte[][] ensure(byte[][] values, int index, int capacity) {
            if (values == null) {
                values = new byte[0][];
            }
            return index < values.length ? values :
                    Arrays.copyOf(values, Math.max(Math.max(index + 1, values.length * 2), capacity));
        }
    }
}
