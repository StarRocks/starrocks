// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.types;

import com.google.common.base.Objects;
import org.roaringbitmap.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;

/**
 * starrocks's own java version bitmap
 * try to keep compatibility with starrocks be's bitmap_value.h,but still has some difference from bitmap_value.h
 * major difference from be:
 * 1. java bitmap support integer range [0, Long.MAX],while be's bitmap support range [0, Long.MAX * 2]
 * Now Long.MAX_VALUE is enough for starrocks's spark load and support unsigned integer in java need to pay more
 * 2. getSizeInBytes method is different from fe to be, details description see method comment
 */
public class BitmapValue {

    public static final int EMPTY = 0;
    public static final int SINGLE32 = 1;
    public static final int BITMAP32 = 2;
    public static final int SINGLE64 = 3;
    public static final int BITMAP64 = 4;
    public static final int SET = 10;

    public static final int SINGLE_VALUE = 1;
    public static final int BITMAP_VALUE = 2;
    public static final int SET_VALUE = 3;

    public static final long UNSIGNED_32BIT_INT_MAX_VALUE = 4294967295L;

    private int bitmapType;
    private long singleValue;
    private Roaring64Map bitmap;
    private Set<Long> set;

    // for single value serialize and deserialize
    private final ByteBuffer buffer;

    public BitmapValue() {
        bitmapType = EMPTY;

        buffer = ByteBuffer.allocate(8);
        // be deserializes by little endian
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public BitmapValue(long v) {
        bitmapType = SINGLE_VALUE;
        singleValue = v;

        buffer = ByteBuffer.allocate(8);
        // be deserializes by little endian
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public BitmapValue(long start, long end) {
        bitmapType = EMPTY;
        buffer = ByteBuffer.allocate(8);
        // be deserializes by little endian
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        for (long i = start; i < end; i++) {
            add(i);
        }
    }

    public BitmapValue(BitmapValue other) throws IOException {
        buffer = ByteBuffer.allocate(8);
        // be deserializes by little endian
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            DataOutput output = new DataOutputStream(outputStream);
            other.serialize(output);
        } catch (IOException e) {
            throw new IOException("Error serializing bitmap: ", e);
        } finally {
            outputStream.close();
        }

        try (DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))) {
            deserialize(inputStream);
        } catch (IOException e) {
            throw new IOException("Error deserializing bitmap: ", e);
        }
    }

    public static byte[] bitmapToBytes(BitmapValue bitmap) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            bitmap.serialize(dos);
        } catch (IOException e) {
            throw new IOException("Error serializing bitmap: ", e);
        }
        return bos.toByteArray();
    }

    public static BitmapValue bitmapFromBytes(byte[] bytes) throws IOException {
        BitmapValue bitmap = new BitmapValue();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            bitmap.deserialize(in);
        } catch (IOException e) {
            throw new IOException("Error deserializing bitmap: ", e);
        }
        return bitmap;
    }

    public void add(int value) {
        add(Util.toUnsignedLong(value));
    }

    public void add(long value) {
        switch (bitmapType) {
            case EMPTY:
                singleValue = value;
                bitmapType = SINGLE_VALUE;
                break;
            case SINGLE_VALUE:
                if (this.singleValue != value) {
                    set = new HashSet<>();
                    set.add(value);
                    set.add(singleValue);
                    bitmapType = SET_VALUE;
                }
                break;
            case BITMAP_VALUE:
                bitmap.addLong(value);
                break;
            case SET_VALUE:
                if (set.size() < 32) {
                    set.add(value);
                } else {
                    fromSetToBitmap();
                    bitmap.add(value);
                }
                break;
        }
    }

    private void fromSetToBitmap() {
        bitmap = new Roaring64Map();
        for (Long v : set) {
            bitmap.add(v);
        }
        set = null;
        bitmapType = BITMAP_VALUE;
    }

    public boolean contains(int value) {
        return contains(Util.toUnsignedLong(value));
    }

    public boolean contains(long value) {
        switch (bitmapType) {
            case SINGLE_VALUE:
                return singleValue == value;
            case BITMAP_VALUE:
                return bitmap.contains(value);
            case SET_VALUE:
                return set.contains(value);
            default:
                // EMPTY
                return false;
        }
    }

    public long cardinality() {
        switch (bitmapType) {
            case EMPTY:
                return 0;
            case SINGLE_VALUE:
                return 1;
            case BITMAP_VALUE:
                return bitmap.getLongCardinality();
            case SET_VALUE:
                return set.size();
        }
        return 0;
    }

    public void serialize(DataOutput output) throws IOException {
        switch (bitmapType) {
            case EMPTY:
                output.writeByte(EMPTY);
                break;
            case SINGLE_VALUE:
                buffer.clear();
                // is 32-bit enough
                if (isLongValue32bitEnough(singleValue)) {
                    output.writeByte(SINGLE32);
                    buffer.putInt((int) singleValue);
                } else {
                    output.writeByte(SINGLE64);
                    buffer.putLong(singleValue);
                }
                buffer.flip();
                output.write(buffer.array(), 0, buffer.limit());
                break;
            case BITMAP_VALUE:
                bitmap.serialize(output);
                break;
            case SET_VALUE: {
                output.writeByte(SET);
                output.writeInt(Integer.reverseBytes(set.size()));
                for (Long v : set) {
                    output.writeLong(Long.reverseBytes(v));
                }
                break;
            }
        }
    }

    public void deserialize(DataInput input) throws IOException {
        clear();
        byte[] bytes;
        int bitmapType = input.readByte();
        switch (bitmapType) {
            case EMPTY:
                break;
            case SINGLE32:
                bytes = new byte[4];
                input.readFully(bytes);
                buffer.clear();
                buffer.put(bytes);
                buffer.flip();
                singleValue = Util.toUnsignedLong(buffer.getInt());
                this.bitmapType = SINGLE_VALUE;
                break;
            case SINGLE64:
                bytes = new byte[8];
                input.readFully(bytes);
                buffer.clear();
                buffer.put(bytes);
                buffer.flip();
                singleValue = buffer.getLong();
                this.bitmapType = SINGLE_VALUE;
                break;
            case BITMAP32:
            case BITMAP64:
                bitmap = bitmap == null ? new Roaring64Map() : bitmap;
                bitmap.deserialize(input, bitmapType);
                this.bitmapType = BITMAP_VALUE;
                break;
            case SET:
                if (set == null) {
                    set = new HashSet<>();
                }
                int size = Integer.reverseBytes(input.readInt());
                for (int i = 0; i < size; i++) {
                    Long v = Long.reverseBytes(input.readLong());
                    set.add(v);
                }
                this.bitmapType = SET_VALUE;
                break;
            default:
                throw new RuntimeException(String.format("unknown bitmap type %s ", bitmapType));
        }
    }

    // In-place bitwise AND (intersection) operation. The current bitmap is modified.
    public void and(BitmapValue other) {
        switch (other.bitmapType) {
            case EMPTY:
                clear();
                break;
            case SINGLE_VALUE:
                switch (this.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (this.singleValue != other.singleValue) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        if (!this.bitmap.contains(other.singleValue)) {
                            clear();
                        } else {
                            clear();
                            this.singleValue = other.singleValue;
                            this.bitmapType = SINGLE_VALUE;
                        }
                        break;
                    case SET_VALUE:
                        if (!this.set.contains(other.singleValue)) {
                            clear();
                        } else {
                            clear();
                            this.singleValue = other.singleValue;
                            this.bitmapType = SINGLE_VALUE;
                        }
                        break;
                }
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (!other.bitmap.contains(this.singleValue)) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.and(other.bitmap);
                        convertBitmapToSmallerType();
                        break;
                    case SET_VALUE:
                        Set<Long> newSet = new HashSet<>();
                        for (Long v : set) {
                            if (other.bitmap.contains(v)) {
                                newSet.add(v);
                            }
                        }
                        set = newSet;
                        break;
                }
                break;
            case SET_VALUE:
                switch (this.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (!other.set.contains(this.singleValue)) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE: {
                        Set<Long> newSet = new HashSet<>();
                        for (Long v : other.set) {
                            if (this.bitmap.contains(v)) {
                                newSet.add(v);
                            }
                        }
                        set = newSet;
                        bitmapType = SET_VALUE;
                        break;
                    }
                    case SET_VALUE: {
                        Set<Long> newSet = new HashSet<>();
                        for (Long v : other.set) {
                            if (this.set.contains(v)) {
                                newSet.add(v);
                            }
                        }
                        set = newSet;
                        break;
                    }
                }
                break;
        }
    }

    // In-place bitwise OR (union) operation. The current bitmap is modified.
    public void or(BitmapValue other) {
        switch (other.bitmapType) {
            case EMPTY:
                break;
            case SINGLE_VALUE:
                add(other.singleValue);
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) {
                    case EMPTY:
                        this.bitmap = new Roaring64Map();
                        this.bitmap.or(other.bitmap);
                        this.bitmapType = BITMAP_VALUE;
                        break;
                    case SINGLE_VALUE:
                        this.bitmap = new Roaring64Map();
                        this.bitmap.or(other.bitmap);
                        this.bitmap.add(this.singleValue);
                        this.bitmapType = BITMAP_VALUE;
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.or(other.bitmap);
                        break;
                    case SET_VALUE: {
                        this.bitmap = new Roaring64Map();
                        this.bitmap.or(other.bitmap);
                        for (Long v : this.set) {
                            this.bitmap.add(v);
                        }
                        this.bitmapType = BITMAP_VALUE;
                        this.set = null;
                        break;
                    }
                }
                break;
            case SET_VALUE:
                switch (this.bitmapType) {
                    case EMPTY: {
                        this.set = new HashSet<>();
                        this.set.addAll(other.set);
                        this.bitmapType = SET_VALUE;
                        break;
                    }
                    case SINGLE_VALUE: {
                        this.set = new HashSet<>(other.set);
                        if (other.set.size() < 32) {
                            this.set.add(singleValue);
                            this.bitmapType = SET_VALUE;
                        } else {
                            fromSetToBitmap();
                            this.bitmap.add(singleValue);
                        }
                        break;
                    }
                    case SET_VALUE: {
                        for (Long v : other.set) {
                            add(v);
                        }
                        break;
                    }
                    case BITMAP_VALUE: {
                        for (Long v : other.set) {
                            bitmap.add(v);
                        }
                        break;
                    }
                }
                break;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(bitmapType);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BitmapValue)) {
            return false;
        }
        BitmapValue otherBitmap = (BitmapValue) other;
        boolean ret = false;
        if (this.bitmapType != otherBitmap.bitmapType) {
            return false;
        }
        switch (otherBitmap.bitmapType) {
            case EMPTY:
                ret = true;
                break;
            case SINGLE_VALUE:
                ret = this.singleValue == otherBitmap.singleValue;
                break;
            case BITMAP_VALUE:
                ret = bitmap.equals(otherBitmap.bitmap);
                break;
            case SET_VALUE: {
                ret = set.equals(otherBitmap.set);
                break;
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        String toStringStr = "{}";
        switch (bitmapType) {
            case EMPTY:
                break;
            case SINGLE_VALUE:
                toStringStr = String.format("{%s}", singleValue);
                break;
            case BITMAP_VALUE:
                toStringStr = this.bitmap.toString();
                break;
            case SET_VALUE:
                toStringStr = String.format("{%s}", setToString());
                break;

        }
        return toStringStr;
    }

    public String setToString() {
        final StringBuilder answer = new StringBuilder();
        for (Long v : this.set) {
            if (answer.length() > 0) {
                answer.append(",");
            }
            answer.append(v);
        }
        return answer.toString();
    }

    public String serializeToString() {
        switch (bitmapType) {
            case EMPTY:
                break;
            case SINGLE_VALUE:
                return String.format("%s", singleValue);
            case BITMAP_VALUE:
                return this.bitmap.serializeToString();
            case SET_VALUE:
                return setToString();
        }
        return "";
    }

    public void clear() {
        this.bitmapType = EMPTY;
        this.singleValue = -1;
        this.bitmap = null;
        this.set = null;
    }

    private void convertBitmapToSmallerType() {
        if (bitmap.getLongCardinality() == 0) {
            this.bitmap = null;
            this.bitmapType = EMPTY;
        } else if (bitmap.getLongCardinality() == 1) {
            this.singleValue = bitmap.select(0);
            this.bitmapType = SINGLE_VALUE;
            this.bitmap = null;
        }
    }

    private boolean isLongValue32bitEnough(long value) {
        return value <= UNSIGNED_32BIT_INT_MAX_VALUE;
    }

    // just for ut
    public int getBitmapType() {
        return bitmapType;
    }

    // just for ut
    public boolean is32BitsEnough() {
        switch (bitmapType) {
            case EMPTY:
                return true;
            case SINGLE_VALUE:
                return isLongValue32bitEnough(singleValue);
            case BITMAP_VALUE:
                return bitmap.is32BitsEnough();
            case SET_VALUE: {
                for (Long v : set) {
                    if (!isLongValue32bitEnough(v)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return false;
        }
    }

}
