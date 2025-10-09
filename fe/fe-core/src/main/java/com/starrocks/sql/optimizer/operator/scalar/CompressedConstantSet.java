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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Compressed storage for large constant sets in IN predicates.
 * Supports multiple compression algorithms based on data patterns.
 */
public class CompressedConstantSet {
    private static final Logger LOG = LogManager.getLogger(CompressedConstantSet.class);

    // Compression algorithm types
    public enum CompressionType {
        NONE,           // No compression
        RANGE,          // Range compression [start, end]
        DELTA,          // Delta encoding
        BITMAP,         // Bitmap compression
        HYBRID          // Hybrid compression
    }

    private final CompressionType compressionType;
    private final Type elementType;
    private final byte[] compressedData;
    private final int originalSize;
    private final long minValue;
    private final long maxValue;

    // Lazy decompression
    private transient Set<Object> decompressedSet;
    private transient boolean isDecompressed = false;

    public CompressedConstantSet(List<ConstantOperator> constants) {
        this.elementType = constants.get(0).getType();
        this.originalSize = constants.size();

        // Analyze data pattern and choose optimal compression
        CompressionResult result = analyzeAndCompress(constants);
        this.compressionType = result.type;
        this.compressedData = result.data;
        this.minValue = result.minValue;
        this.maxValue = result.maxValue;

        LOG.info("Compressed {} constants from {} bytes to {} bytes using {}", 
                originalSize, estimateOriginalSize(constants), compressedData.length, compressionType);
    }

    private CompressionResult analyzeAndCompress(List<ConstantOperator> constants) {
        if (!elementType.isNumericType()) {
            return compressAsGeneric(constants);
        }

        // Extract numeric values and sort
        List<Long> values = constants.stream()
                .map(c -> {
                    if (c.getType().isInt()) {
                        return (long) c.getInt();
                    } else if (c.getType().isBigint()) {
                        return c.getBigint();
                    } else if (c.getType().isSmallint()) {
                        return (long) c.getSmallint();
                    } else if (c.getType().isTinyint()) {
                        return (long) c.getTinyInt();
                    } else {
                        return c.getBigint();
                    }
                })
                .sorted()
                .collect(Collectors.toList());

        long min = values.get(0);
        long max = values.get(values.size() - 1);
        long range = max - min;

        // Choose optimal compression algorithm
        if (isConsecutiveRange(values)) {
            return compressAsRange(min, max);
        } else if (range < Integer.MAX_VALUE && values.size() > 1000) {
            return compressAsBitmap(values, min, max);
        } else if (isDeltaCompressible(values)) {
            return compressAsDelta(values);
        } else {
            return compressAsGeneric(constants);
        }
    }

    // Range compression: consecutive numbers [start, end]
    private CompressionResult compressAsRange(long min, long max) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(min);
            dos.writeLong(max);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new CompressionResult(CompressionType.RANGE, baos.toByteArray(), min, max);
    }

    // Bitmap compression: sparse but limited range numbers
    private CompressionResult compressAsBitmap(List<Long> values, long min, long max) {
        long range = max - min;
        if (range > Integer.MAX_VALUE) {
            return compressAsGeneric(values.stream()
                    .map(v -> ConstantOperator.createBigint(v))
                    .collect(Collectors.toList()));
        }

        BitSet bitSet = new BitSet((int) range + 1);
        for (Long value : values) {
            bitSet.set((int) (value - min));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(min);
            dos.writeLong(max);
            byte[] bitSetBytes = bitSet.toByteArray();
            dos.writeInt(bitSetBytes.length);
            dos.write(bitSetBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new CompressionResult(CompressionType.BITMAP, baos.toByteArray(), min, max);
    }

    // Delta encoding: adjacent values have small differences
    private CompressionResult compressAsDelta(List<Long> values) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(values.get(0)); // Base value
            dos.writeInt(values.size());

            for (int i = 1; i < values.size(); i++) {
                long delta = values.get(i) - values.get(i - 1);
                writeVarLong(dos, delta);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new CompressionResult(CompressionType.DELTA, baos.toByteArray(), 
                values.get(0), values.get(values.size() - 1));
    }

    // Generic compression for non-numeric or complex patterns
    private CompressionResult compressAsGeneric(List<ConstantOperator> constants) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(constants.size());
            for (ConstantOperator constant : constants) {
                String value = constant.toString();
                dos.writeUTF(value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new CompressionResult(CompressionType.NONE, baos.toByteArray(), 0, 0);
    }

    // Check if values form a consecutive range
    private boolean isConsecutiveRange(List<Long> values) {
        if (values.size() < 2) return false;

        for (int i = 1; i < values.size(); i++) {
            if (values.get(i) != values.get(i - 1) + 1) {
                return false;
            }
        }
        return true;
    }

    // Check if suitable for delta compression
    private boolean isDeltaCompressible(List<Long> values) {
        if (values.size() < 10) return false;

        long totalDelta = 0;
        int deltaCount = 0;

        for (int i = 1; i < values.size(); i++) {
            long delta = Math.abs(values.get(i) - values.get(i - 1));
            if (delta < 1000) { // Delta less than 1000 is considered suitable
                totalDelta += delta;
                deltaCount++;
            }
        }

        return deltaCount > values.size() * 0.8; // 80% of deltas are small
    }

    // Lazy decompression
    public Set<Object> getDecompressedSet() {
        if (!isDecompressed) {
            decompressedSet = decompress();
            isDecompressed = true;
        }
        return decompressedSet;
    }

    private Set<Object> decompress() {
        switch (compressionType) {
            case RANGE:
                return decompressRange();
            case BITMAP:
                return decompressBitmap();
            case DELTA:
                return decompressDelta();
            default:
                return decompressGeneric();
        }
    }

    private Set<Object> decompressRange() {
        Set<Object> result = new LinkedHashSet<>();
        for (long i = minValue; i <= maxValue; i++) {
            if (elementType.isInt()) {
                result.add((int) i);
            } else {
                result.add(i);
            }
        }
        return result;
    }

    private Set<Object> decompressBitmap() {
        Set<Object> result = new LinkedHashSet<>();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(compressedData))) {
            long min = dis.readLong();
            long max = dis.readLong();
            int bitSetLength = dis.readInt();
            byte[] bitSetBytes = new byte[bitSetLength];
            dis.readFully(bitSetBytes);

            BitSet bitSet = BitSet.valueOf(bitSetBytes);
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                long value = min + i;
                if (elementType.isInt()) {
                    result.add((int) value);
                } else {
                    result.add(value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private Set<Object> decompressDelta() {
        Set<Object> result = new LinkedHashSet<>();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(compressedData))) {
            long baseValue = dis.readLong();
            int size = dis.readInt();

            result.add(elementType.isInt() ? (int) baseValue : baseValue);

            long currentValue = baseValue;
            for (int i = 1; i < size; i++) {
                long delta = readVarLong(dis);
                currentValue += delta;
                result.add(elementType.isInt() ? (int) currentValue : currentValue);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private Set<Object> decompressGeneric() {
        Set<Object> result = new LinkedHashSet<>();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(compressedData))) {
            int size = dis.readInt();
            for (int i = 0; i < size; i++) {
                String value = dis.readUTF();
                result.add(value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    // Variable-length encoding for deltas
    private void writeVarLong(DataOutputStream dos, long value) throws IOException {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            dos.writeByte((int) (value & 0x7F) | 0x80);
            value >>>= 7;
        }
        dos.writeByte((int) value & 0x7F);
    }

    private long readVarLong(DataInputStream dis) throws IOException {
        long result = 0;
        int shift = 0;
        while (true) {
            byte b = dis.readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return result;
    }

    private int estimateOriginalSize(List<ConstantOperator> constants) {
        return constants.size() * 8; // Assume 8 bytes per constant
    }

    // Getters
    public CompressionType getCompressionType() {
        return compressionType;
    }

    public Type getElementType() {
        return elementType;
    }

    public byte[] getCompressedData() {
        return compressedData;
    }

    public int getOriginalSize() {
        return originalSize;
    }

    public long getMinValue() {
        return minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public int getCompressedSize() {
        return compressedData.length;
    }

    public double getCompressionRatio() {
        int originalSize = estimateOriginalSize(Lists.newArrayList());
        return (double) compressedData.length / (originalSize > 0 ? originalSize : 1);
    }

    static class CompressionResult {
        final CompressionType type;
        final byte[] data;
        final long minValue;
        final long maxValue;

        CompressionResult(CompressionType type, byte[] data, long minValue, long maxValue) {
            this.type = type;
            this.data = data;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }
    }
}
