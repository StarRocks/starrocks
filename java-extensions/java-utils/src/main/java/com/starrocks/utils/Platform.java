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

package com.starrocks.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Reference to Apache Spark with some customization
 */
public final class Platform {

    private static final Unsafe _UNSAFE;

    public static final int BOOLEAN_ARRAY_OFFSET;

    public static final int BYTE_ARRAY_OFFSET;

    public static final int SHORT_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET;

    public static final int DOUBLE_ARRAY_OFFSET;

    public static int getInt(Object object, long offset) {
        return _UNSAFE.getInt(object, offset);
    }

    public static void putInt(Object object, long offset, int value) {
        _UNSAFE.putInt(object, offset, value);
    }

    public static boolean getBoolean(Object object, long offset) {
        return _UNSAFE.getBoolean(object, offset);
    }

    public static void putBoolean(Object object, long offset, boolean value) {
        _UNSAFE.putBoolean(object, offset, value);
    }

    public static byte getByte(Object object, long offset) {
        return _UNSAFE.getByte(object, offset);
    }

    public static void putByte(Object object, long offset, byte value) {
        _UNSAFE.putByte(object, offset, value);
    }

    public static short getShort(Object object, long offset) {
        return _UNSAFE.getShort(object, offset);
    }

    public static void putShort(Object object, long offset, short value) {
        _UNSAFE.putShort(object, offset, value);
    }

    public static long getLong(Object object, long offset) {
        return _UNSAFE.getLong(object, offset);
    }

    public static void putLong(Object object, long offset, long value) {
        _UNSAFE.putLong(object, offset, value);
    }

    public static float getFloat(Object object, long offset) {
        return _UNSAFE.getFloat(object, offset);
    }

    public static void putFloat(Object object, long offset, float value) {
        _UNSAFE.putFloat(object, offset, value);
    }

    public static double getDouble(Object object, long offset) {
        return _UNSAFE.getDouble(object, offset);
    }

    public static void putDouble(Object object, long offset, double value) {
        _UNSAFE.putDouble(object, offset, value);
    }

    public static boolean isTesting() {
        return System.getProperties().containsKey("starrocks.fe.test") &&
                System.getProperty("starrocks.fe.test").equals("1");
    }

    public static void freeMemory(long address) {
        if (isTesting()) {
            _UNSAFE.freeMemory(address);
        } else {
            com.starrocks.utils.NativeMethodHelper.memoryTrackerFree(address);
        }
    }

    public static long allocateMemory(long size) {
        if (isTesting()) {
            return _UNSAFE.allocateMemory(size);
        } else {
            return com.starrocks.utils.NativeMethodHelper.memoryTrackerMalloc(size);
        }
    }

    public static long reallocateMemory(long address, long oldSize, long newSize) {
        long newMemory = allocateMemory(newSize);
        copyMemory(null, address, null, newMemory, oldSize);
        freeMemory(address);
        return newMemory;
    }

    public static void setMemory(long address, byte value, long size) {
        _UNSAFE.setMemory(address, size, value);
    }

    public static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
        // Check if dstOffset is before or after srcOffset to determine if we should copy
        // forward or backwards. This is necessary in case src and dst overlap.
        if (dstOffset < srcOffset) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
            }
        }
    }

    /**
     * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
     * allow safepoint polling during a large copy.
     */
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    static {
        Unsafe unsafe;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
        } catch (Throwable cause) {
            unsafe = null;
        }
        _UNSAFE = unsafe;

        if (_UNSAFE != null) {
            BOOLEAN_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(boolean[].class);
            BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
            SHORT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(short[].class);
            INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
            LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
            FLOAT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(float[].class);
            DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
        } else {
            BOOLEAN_ARRAY_OFFSET = 0;
            BYTE_ARRAY_OFFSET = 0;
            SHORT_ARRAY_OFFSET = 0;
            INT_ARRAY_OFFSET = 0;
            LONG_ARRAY_OFFSET = 0;
            FLOAT_ARRAY_OFFSET = 0;
            DOUBLE_ARRAY_OFFSET = 0;
        }
    }
}
