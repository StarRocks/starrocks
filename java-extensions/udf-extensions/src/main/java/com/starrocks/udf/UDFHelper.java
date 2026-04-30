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

package com.starrocks.udf;

import com.starrocks.utils.Platform;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static com.starrocks.utils.NativeMethodHelper.getAddrs;
import static com.starrocks.utils.NativeMethodHelper.getColumnLogicalType;
import static com.starrocks.utils.NativeMethodHelper.resize;
import static com.starrocks.utils.NativeMethodHelper.resizeStringData;

public class UDFHelper {
    public static final int TYPE_TINYINT = 1;
    public static final int TYPE_SMALLINT = 3;
    public static final int TYPE_INT = 5;
    public static final int TYPE_BIGINT = 7;
    public static final int TYPE_FLOAT = 10;
    public static final int TYPE_DOUBLE = 11;
    public static final int TYPE_VARCHAR = 17;
    public static final int TYPE_ARRAY = 19;
    public static final int TYPE_MAP = 20;
    public static final int TYPE_BOOLEAN = 24;
    public static final int TYPE_TIME = 44;
    public static final int TYPE_VARBINARY = 46;
    public static final int TYPE_DECIMAL32 = 47;
    public static final int TYPE_DECIMAL64 = 48;
    public static final int TYPE_DECIMAL128 = 49;
    public static final int TYPE_DECIMAL256 = 26;
    public static final int TYPE_DATE = 50;
    public static final int TYPE_DATETIME = 51;

    public static final HashMap<Integer, Class<?>> clazzs = new HashMap<Integer, Class<?>>() {
        {
            put(TYPE_BOOLEAN, Boolean.class);
            put(TYPE_TINYINT, Byte.class);
            put(TYPE_SMALLINT, Short.class);
            put(TYPE_INT, Integer.class);
            put(TYPE_FLOAT, Float.class);
            put(TYPE_DOUBLE, Double.class);
            put(TYPE_BIGINT, Long.class);
            put(TYPE_VARCHAR, String.class);
            put(TYPE_ARRAY, List.class);
            put(TYPE_MAP, Map.class);
            put(TYPE_DECIMAL32, BigDecimal.class);
            put(TYPE_DECIMAL64, BigDecimal.class);
            put(TYPE_DECIMAL128, BigDecimal.class);
            put(TYPE_DECIMAL256, BigDecimal.class);
            put(TYPE_DATE, LocalDate.class);
            put(TYPE_DATETIME, LocalDateTime.class);
        }
    };

    // StarRocks DateValue stores the int32 Julian-day. DateValue '1970-01-01' -> 2440588.
    private static final int UNIX_EPOCH_JULIAN = 2440588;
    // StarRocks TimestampValue packs (julian_days << 40) | microseconds_of_day.
    private static final int TIMESTAMP_BITS = 40;
    private static final long TIMESTAMP_MICROS_OF_DAY_MASK = (1L << TIMESTAMP_BITS) - 1L;

    private static final byte[] emptyBytes = new byte[0];

    private static final ThreadLocal<DateFormat> formatter =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // TODO: Use the time zone set by session variables?
    private static final TimeZone timeZone = TimeZone.getDefault();

    private static void getBooleanBoxedResult(int numRows, Boolean[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        byte[] dataArr = new byte[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = (byte) (boxedArr[i] ? 1 : 0);
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to int array
        Platform.copyMemory(dataArr, Platform.BYTE_ARRAY_OFFSET, null, addrs[1], numRows);
    }

    private static void getByteBoxedResult(int numRows, Byte[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        byte[] dataArr = new byte[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = boxedArr[i];
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to int array
        Platform.copyMemory(dataArr, Platform.BYTE_ARRAY_OFFSET, null, addrs[1], numRows);
    }

    private static void getShortBoxedResult(int numRows, Short[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        short[] dataArr = new short[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = boxedArr[i];
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to int array
        Platform.copyMemory(dataArr, Platform.SHORT_ARRAY_OFFSET, null, addrs[1], numRows * 2L);
    }

    // getIntBoxedResult
    private static void getIntBoxedResult(int numRows, Integer[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] dataArr = new int[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = boxedArr[i];
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to int array
        Platform.copyMemory(dataArr, Platform.INT_ARRAY_OFFSET, null, addrs[1], numRows * 4L);
    }

    // getIntBoxedResult
    private static void getBigIntBoxedResult(int numRows, Long[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        long[] dataArr = new long[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = boxedArr[i];
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to int array
        Platform.copyMemory(dataArr, Platform.LONG_ARRAY_OFFSET, null, addrs[1], numRows * 8L);
    }

    public static void getStringLargeIntResult(int numRows, BigInteger[] column, long columnAddr) {
        String[] results = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] != null) {
                results[i] = column[i].toString();
            }
        }
        getStringBoxedResult(numRows, results, columnAddr);
    }

    private static void getFloatBoxedResult(int numRows, Float[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        float[] dataArr = new float[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = boxedArr[i];
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to int array
        Platform.copyMemory(dataArr, Platform.FLOAT_ARRAY_OFFSET, null, addrs[1], numRows * 4L);
    }

    private static void getDoubleBoxedResult(int numRows, Double[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        double[] dataArr = new double[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = boxedArr[i];
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to int array
        Platform.copyMemory(dataArr, Platform.DOUBLE_ARRAY_OFFSET, null, addrs[1], numRows * 8L);
    }

    // TODO: Why not use BigInt instead of double?
    private static void getDoubleTimeResult(int numRows, Time[] boxedArr, long columnAddr) {
        byte[] nulls = new byte[numRows];
        double[] dataArr = new double[numRows];
        for (int i = 0; i < numRows; i++) {
            if (boxedArr[i] == null) {
                nulls[i] = 1;
            } else {
                // Note: add the timezone offset back because Time#getTime() returns the GMT timestamp
                long v = boxedArr[i].getTime();
                double secondOfDay = (v + timeZone.getOffset(v)) / 1000.0;
                secondOfDay %= 24 * 3600;
                if (secondOfDay < 0) {
                    secondOfDay += 24 * 3600;
                }
                dataArr[i] = secondOfDay;
            }
        }

        final long[] addrs = getAddrs(columnAddr);
        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to double array
        Platform.copyMemory(dataArr, Platform.DOUBLE_ARRAY_OFFSET, null, addrs[1], numRows * 8L);
    }

    private static void getStringDateResult(int numRows, Date[] column, long columnAddr) {
        // TODO: return timestamp
        String[] results = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] != null) {
                results[i] = formatter.get().format(column[i]);
            }
        }
        getStringBoxedResult(numRows, results, columnAddr);
    }

    private static void getStringLocalDateResult(int numRows, LocalDate[] column, long columnAddr) {
        // TODO: return timestamp
        String[] results = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] != null) {
                results[i] = dateFormatter.format(column[i]);
            }
        }
        getStringBoxedResult(numRows, results, columnAddr);
    }

    private static void getStringTimeStampResult(int numRows, Timestamp[] column, long columnAddr) {
        // TODO: return timestamp
        String[] results = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] != null) {
                results[i] = column[i].toString();
            }
        }
        getStringBoxedResult(numRows, results, columnAddr);
    }

    private static void getStringTimeResult(int numRows, Time[] column, long columnAddr) {
        String[] results = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] != null) {
                results[i] = column[i].toString();
            }
        }
        getStringBoxedResult(numRows, results, columnAddr);
    }

    public static void getStringDateTimeResult(int numRows, LocalDateTime[] column, long columnAddr) {
        // TODO:
        String[] results = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] != null) {
                results[i] = column[i].toString();
            }
        }
        getStringBoxedResult(numRows, results, columnAddr);
    }

    // Decode StarRocks DateValue (int32 Julian day) into a LocalDate.
    private static LocalDate localDateFromJulian(int julian) {
        return LocalDate.ofEpochDay((long) julian - UNIX_EPOCH_JULIAN);
    }

    // Encode a LocalDate as StarRocks DateValue (int32 Julian day).
    private static int julianFromLocalDate(LocalDate ld) {
        return (int) (ld.toEpochDay() + UNIX_EPOCH_JULIAN);
    }

    // Decode StarRocks TimestampValue (packed int64) into a LocalDateTime.
    // Layout: high 24 bits = Julian day, low 40 bits = microseconds-of-day.
    public static LocalDateTime localDateTimeFromPackedTimestamp(long packed) {
        long julian = packed >>> TIMESTAMP_BITS;
        long microsOfDay = packed & TIMESTAMP_MICROS_OF_DAY_MASK;
        return LocalDateTime.of(LocalDate.ofEpochDay(julian - UNIX_EPOCH_JULIAN),
                LocalTime.ofNanoOfDay(microsOfDay * 1000L));
    }

    // Encode a LocalDateTime as StarRocks TimestampValue (packed int64).
    public static long packedTimestampFromLocalDateTime(LocalDateTime ldt) {
        long julian = ldt.toLocalDate().toEpochDay() + UNIX_EPOCH_JULIAN;
        long microsOfDay = ldt.toLocalTime().toNanoOfDay() / 1000L;
        return (julian << TIMESTAMP_BITS) | microsOfDay;
    }

    // Output batch: write LocalDate[] into a TYPE_DATE column as int32 Julian days.
    private static void getLocalDateBoxedResult(int numRows, LocalDate[] column, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] dataArr = new int[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = julianFromLocalDate(column[i]);
            }
        }
        final long[] addrs = getAddrs(columnAddr);
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        Platform.copyMemory(dataArr, Platform.INT_ARRAY_OFFSET, null, addrs[1], numRows * 4L);
    }

    // Output batch: write LocalDateTime[] into a TYPE_DATETIME column as packed int64.
    private static void getLocalDateTimeBoxedResult(int numRows, LocalDateTime[] column, long columnAddr) {
        byte[] nulls = new byte[numRows];
        long[] dataArr = new long[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                nulls[i] = 1;
            } else {
                dataArr[i] = packedTimestampFromLocalDateTime(column[i]);
            }
        }
        final long[] addrs = getAddrs(columnAddr);
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        Platform.copyMemory(dataArr, Platform.LONG_ARRAY_OFFSET, null, addrs[1], numRows * 8L);
    }

    public static void getStringDecimalResult(int numRows, BigDecimal[] column, long columnAddr) {
        String[] results = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] != null) {
                results[i] = column[i].toString();
            }
        }
        getStringBoxedResult(numRows, results, columnAddr);
    }

    public static void getDecimalResultFromBoxedArray(int type, int precision, int scale, int numRows,
                                                      Object boxedResult, long columnAddr, boolean errorIfOverflow) {
        if (numRows == 0) {
            return;
        }
        BigDecimal[] column = (BigDecimal[]) boxedResult;
        byte[] nulls = new byte[numRows];
        final long[] addrs = getAddrs(columnAddr);
        // A valid DECIMAL(p, s) value satisfies |unscaled| < 10^p.
        BigInteger precisionLimit = BigInteger.TEN.pow(precision);
        switch (type) {
            case TYPE_DECIMAL32: {
                int[] dataArr = new int[numRows];
                for (int i = 0; i < numRows; i++) {
                    if (column[i] == null) {
                        nulls[i] = 1;
                        continue;
                    }
                    try {
                        BigInteger unscaled = rescaleAndCheck(column[i], precision, scale, precisionLimit);
                        dataArr[i] = unscaled.intValueExact();
                    } catch (ArithmeticException e) {
                        if (errorIfOverflow) throw e;
                        nulls[i] = 1;
                    }
                }
                Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
                Platform.copyMemory(dataArr, Platform.INT_ARRAY_OFFSET, null, addrs[1], numRows * 4L);
                break;
            }
            case TYPE_DECIMAL64: {
                long[] dataArr = new long[numRows];
                for (int i = 0; i < numRows; i++) {
                    if (column[i] == null) {
                        nulls[i] = 1;
                        continue;
                    }
                    try {
                        BigInteger unscaled = rescaleAndCheck(column[i], precision, scale, precisionLimit);
                        dataArr[i] = unscaled.longValueExact();
                    } catch (ArithmeticException e) {
                        if (errorIfOverflow) throw e;
                        nulls[i] = 1;
                    }
                }
                Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
                Platform.copyMemory(dataArr, Platform.LONG_ARRAY_OFFSET, null, addrs[1], numRows * 8L);
                break;
            }
            case TYPE_DECIMAL128:
                writeWideDecimalResult(numRows, precision, scale, precisionLimit, column, nulls, addrs, 16,
                        errorIfOverflow);
                break;
            case TYPE_DECIMAL256:
                writeWideDecimalResult(numRows, precision, scale, precisionLimit, column, nulls, addrs, 32,
                        errorIfOverflow);
                break;
            default:
                throw new UnsupportedOperationException("unsupported decimal type:" + type);
        }
    }

    // Round `v` to `scale` fractional digits (HALF_UP) and verify the rescaled unscaled
    // value fits into DECIMAL(precision, scale). Throws ArithmeticException on overflow so
    // the caller can choose to rethrow (REPORT_ERROR) or mark the row null (OUTPUT_NULL).
    // Package-private for direct unit testing.
    static BigInteger rescaleAndCheck(BigDecimal v, int precision, int scale, BigInteger precisionLimit) {
        BigInteger unscaled = v.setScale(scale, RoundingMode.HALF_UP).unscaledValue();
        if (unscaled.abs().compareTo(precisionLimit) >= 0) {
            throw new ArithmeticException("Value " + v + " out of DECIMAL(" + precision + "," + scale + ") range");
        }
        return unscaled;
    }

    // Single-value helper used by the BE's per-row path (assign_jvalue / cast_to_jvalue).
    // Rescales `v` to DECIMAL(precision, scale) and returns the unscaled integer as a long
    // for DECIMAL32/64. Throws ArithmeticException on precision overflow or if the unscaled
    // value does not fit in a Java long (DECIMAL128/256 should use unscaledLEBytes instead).
    public static long unscaledLong(BigDecimal v, int precision, int scale) {
        BigInteger unscaled = rescaleAndCheck(v, precision, scale, BigInteger.TEN.pow(precision));
        return unscaled.longValueExact();
    }

    // Single-value helper used by the BE for DECIMAL128/256 per-row paths. Returns `byteWidth`
    // little-endian sign-extended bytes corresponding to the unscaled integer of
    // setScale(scale, HALF_UP). Throws ArithmeticException on precision overflow or width
    // overflow.
    public static byte[] unscaledLEBytes(BigDecimal v, int precision, int scale, int byteWidth) {
        BigInteger unscaled = rescaleAndCheck(v, precision, scale, BigInteger.TEN.pow(precision));
        byte[] be = unscaled.toByteArray();
        if (be.length > byteWidth) {
            throw new ArithmeticException("DECIMAL" + (byteWidth * 8) + " overflow: " + v);
        }
        byte[] le = new byte[byteWidth];
        byte signByte = (be[0] < 0) ? (byte) 0xFF : 0;
        // sign-extend high (little-endian: high bytes at end)
        for (int j = be.length; j < byteWidth; ++j) {
            le[j] = signByte;
        }
        // copy big-endian magnitude into little-endian positions
        for (int j = 0; j < be.length; ++j) {
            le[j] = be[be.length - 1 - j];
        }
        return le;
    }

    // Pack `numRows` BigDecimal values into `byteWidth` little-endian signed-integer bytes.
    private static void writeWideDecimalResult(int numRows, int precision, int scale, BigInteger precisionLimit,
                                               BigDecimal[] column, byte[] nulls, long[] addrs, int byteWidth,
                                               boolean errorIfOverflow) {
        byte[] dataArr = new byte[numRows * byteWidth];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                nulls[i] = 1;
                continue;
            }
            try {
                BigInteger unscaled = rescaleAndCheck(column[i], precision, scale, precisionLimit);
                byte[] be = unscaled.toByteArray();
                if (be.length > byteWidth) {
                    throw new ArithmeticException("DECIMAL" + (byteWidth * 8) + " overflow: " + column[i]);
                }
                int base = i * byteWidth;
                byte signByte = (be[0] < 0) ? (byte) 0xFF : 0;
                // sign-extend high (little-endian: high bytes at end)
                for (int j = be.length; j < byteWidth; ++j) {
                    dataArr[base + j] = signByte;
                }
                // copy big-endian magnitude into little-endian positions
                for (int j = 0; j < be.length; ++j) {
                    dataArr[base + j] = be[be.length - 1 - j];
                }
            } catch (ArithmeticException e) {
                if (errorIfOverflow) throw e;
                nulls[i] = 1;
            }
        }
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        Platform.copyMemory(dataArr, Platform.BYTE_ARRAY_OFFSET, null, addrs[1], numRows * (long) byteWidth);
    }

    private static void copyDataToBinaryColumn(int numRows, byte[][] byteRes, int[] offsets, byte[] nulls,
                                               long columnAddr) {
        byte[] bytes = new byte[offsets[numRows - 1]];
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        final long bytesAddr = resizeStringData(columnAddr, offsets[numRows - 1]);
        final long[] addrs = getAddrs(columnAddr);
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);

        Platform.copyMemory(offsets, Platform.INT_ARRAY_OFFSET, null, addrs[1] + 4, numRows * 4L);

        Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private static void getStringBoxedResult(int numRows, String[] column, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                byteRes[i] = emptyBytes;
                nulls[i] = 1;
            } else {
                byteRes[i] = column[i].getBytes(StandardCharsets.UTF_8);
            }
            offset += byteRes[i].length;
            offsets[i] = offset;
        }
        copyDataToBinaryColumn(numRows, byteRes, offsets, nulls, columnAddr);
    }

    private static void getBinaryBoxedResult(int numRows, byte[][] column, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                byteRes[i] = emptyBytes;
                nulls[i] = 1;
            } else {
                byteRes[i] = column[i];
            }
            offset += byteRes[i].length;
            offsets[i] = offset;
        }
        copyDataToBinaryColumn(numRows, byteRes, offsets, nulls, columnAddr);
    }

    private static void getUUIDBoxedResult(int numRows, UUID[] column, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                byteRes[i] = emptyBytes;
                nulls[i] = 1;
            } else {
                byteRes[i] = column[i].toString().getBytes(StandardCharsets.UTF_8);
            }
            offset += byteRes[i].length;
            offsets[i] = offset;
        }
        copyDataToBinaryColumn(numRows, byteRes, offsets, nulls, columnAddr);
    }

    private static void getBinaryBoxedBlobResult(int numRows, Blob[] column, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                byteRes[i] = emptyBytes;
                nulls[i] = 1;
            } else {
                try {
                    int len = (int) column[i].length();
                    if (len == 0) {
                        byteRes[i] = emptyBytes;
                    } else {
                        byteRes[i] = column[i].getBytes(1, len);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
            offset += byteRes[i].length;
            offsets[i] = offset;
        }
        copyDataToBinaryColumn(numRows, byteRes, offsets, nulls, columnAddr);
    }

    public static void getResultFromListArray(int numRows, List<?>[] lists, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] offsets = new int[numRows];
        int offset = 0;

        for (int i = 0; i < numRows; i++) {
            if (lists[i] == null) {
                nulls[i] = 1;
            } else {
                offset += lists[i].size();
            }
            offsets[i] = offset;
        }

        final long[] addrs = getAddrs(columnAddr);
        long elementAddr = addrs[2];
        // get_type
        int elementType = getColumnLogicalType(elementAddr);
        // get clazz array
        Object[] elements = (Object[]) Array.newInstance(clazzs.get(elementType), offsets[numRows - 1]);
        offset = 0;
        for (int i = 0; i < numRows; i++) {
            if (lists[i] != null) {
                for (Object o : lists[i]) {
                    elements[offset++] = o;
                }
            }
            lists[i] = null;
        }

        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to offset array
        Platform.copyMemory(offsets, Platform.INT_ARRAY_OFFSET, null, addrs[1] + 4, numRows * 4L);

        resize(elementAddr, offsets[numRows - 1]);
        writeElementsByType(elementType, offsets[numRows - 1], elements, elementAddr);
    }

    public static void getResultFromMapArray(int numRows, Map<?, ?>[] maps, long columnAddr) {
        byte[] nulls = new byte[numRows];
        int[] offsets = new int[numRows];
        int offset = 0;

        for (int i = 0; i < numRows; i++) {
            if (maps[i] == null) {
                nulls[i] = 1;
            } else {
                offset += maps[i].size();
            }
            offsets[i] = offset;
        }
        final long[] addrs = getAddrs(columnAddr);
        long keyAddr = addrs[2];
        long valueAddr = addrs[3];

        int keyType = getColumnLogicalType(keyAddr);
        int valueType = getColumnLogicalType(valueAddr);
        // get clazz array
        Object[] keys = (Object[]) Array.newInstance(clazzs.get(keyType), offsets[numRows - 1]);
        Object[] values = (Object[]) Array.newInstance(clazzs.get(valueType), offsets[numRows - 1]);

        offset = 0;
        for (int i = 0; i < numRows; i++) {
            if (maps[i] != null) {
                for (Map.Entry<?, ?> entry : maps[i].entrySet()) {
                    keys[offset] = entry.getKey();
                    values[offset++] = entry.getValue();
                }
            }
            maps[i] = null;
        }

        resize(keyAddr, offsets[numRows - 1]);
        resize(valueAddr, offsets[numRows - 1]);

        // memcpy to uint8_t array
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null, addrs[0], numRows);
        // memcpy to offset array
        Platform.copyMemory(offsets, Platform.INT_ARRAY_OFFSET, null, addrs[1] + 4, numRows * 4L);

        writeElementsByType(keyType, offsets[numRows - 1], keys, keyAddr);
        writeElementsByType(valueType, offsets[numRows - 1], values, valueAddr);
    }

    // Element-write dispatcher used by ARRAY / MAP outputs. For DECIMAL element columns
    // we look up precision/scale from the column's getAddrs() tail slots (filled by the
    // BE-side GetColumnAddrVistor for DecimalV3Column) and route through the scale-aware
    // helper. Everything else falls back to the regular getResultFromBoxedArray.
    private static void writeElementsByType(int elementType, int numRows, Object elements, long elementAddr) {
        if (elementType == TYPE_DECIMAL32 || elementType == TYPE_DECIMAL64 || elementType == TYPE_DECIMAL128
                || elementType == TYPE_DECIMAL256) {
            long[] elemAddrs = getAddrs(elementAddr);
            int precision = (int) elemAddrs[2];
            int scale = (int) elemAddrs[3];
            // Nested-element overflow always reports an error: collection writers cannot
            // null individual elements without breaking the parent offsets, and the FE has
            // already validated declared precision/scale.
            getDecimalResultFromBoxedArray(elementType, precision, scale, numRows, elements, elementAddr,
                    /*errorIfOverflow=*/true);
            return;
        }
        getResultFromBoxedArray(elementType, numRows, elements, elementAddr);
    }

    public static void getResultFromBoxedArray(int type, int numRows, Object boxedResult, long columnAddr) {
        if (numRows == 0) {
            return;
        }
        switch (type) {
            case TYPE_BOOLEAN: {
                getBooleanBoxedResult(numRows, (Boolean[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_TINYINT: {
                getByteBoxedResult(numRows, (Byte[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_SMALLINT: {
                getShortBoxedResult(numRows, (Short[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_INT: {
                getIntBoxedResult(numRows, (Integer[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_FLOAT: {
                getFloatBoxedResult(numRows, (Float[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_DOUBLE: {
                getDoubleBoxedResult(numRows, (Double[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_BIGINT: {
                getBigIntBoxedResult(numRows, (Long[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_DATE: {
                getLocalDateBoxedResult(numRows, (LocalDate[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_DATETIME: {
                getLocalDateTimeBoxedResult(numRows, (LocalDateTime[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_TIME: {
                getDoubleTimeResult(numRows, (Time[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_VARCHAR: {
                if (boxedResult instanceof Date[]) {
                    getStringDateResult(numRows, (Date[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof LocalDate[]) {
                    getStringLocalDateResult(numRows, (LocalDate[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof LocalDateTime[]) {
                    getStringDateTimeResult(numRows, (LocalDateTime[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof Timestamp[]) {
                    getStringTimeStampResult(numRows, (Timestamp[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof Time[]) {
                    getStringTimeResult(numRows, (Time[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof BigDecimal[]) {
                    getStringDecimalResult(numRows, (BigDecimal[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof BigInteger[]) {
                    getStringLargeIntResult(numRows, (BigInteger[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof String[]) {
                    getStringBoxedResult(numRows, (String[]) boxedResult, columnAddr);
                } else {
                    throw new UnsupportedOperationException("unsupported type:" + boxedResult);
                }
                break;
            }
            case TYPE_VARBINARY: {
                if (boxedResult instanceof byte[][]) {
                    getBinaryBoxedResult(numRows, (byte[][]) boxedResult, columnAddr);
                } else if (boxedResult instanceof Blob[]) {
                    getBinaryBoxedBlobResult(numRows, (Blob[]) boxedResult, columnAddr);
                } else if (boxedResult instanceof UUID[]) {
                    getUUIDBoxedResult(numRows, (UUID[]) boxedResult, columnAddr);
                } else {
                    throw new UnsupportedOperationException("unsupported type:" + boxedResult);
                }
                break;
            }
            case TYPE_ARRAY: {
                getResultFromListArray(numRows, (List<?>[]) boxedResult, columnAddr);
                break;
            }
            case TYPE_MAP: {
                getResultFromMapArray(numRows, (Map<?, ?>[]) boxedResult, columnAddr);
                break;
            }
            default:
                throw new UnsupportedOperationException("unsupported type:" + type);
        }
    }

    // create boxed array
    //
    public static Object[] createBoxedArray(int type, int numRows, boolean nullable, ByteBuffer... buffer) {
        switch (type) {
            case TYPE_BOOLEAN: {
                if (!nullable) {
                    return createBoxedBoolArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedBoolArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_TINYINT: {
                if (!nullable) {
                    return createBoxedByteArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedByteArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_SMALLINT: {
                if (!nullable) {
                    return createBoxedShortArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedShortArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_INT: {
                if (!nullable) {
                    return createBoxedIntegerArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedIntegerArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_BIGINT: {
                if (!nullable) {
                    return createBoxedLongArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedLongArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_FLOAT: {
                if (!nullable) {
                    return createBoxedFloatArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedFloatArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_DOUBLE: {
                if (!nullable) {
                    return createBoxedDoubleArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedDoubleArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_VARCHAR: {
                if (!nullable) {
                    return createBoxedStringArray(numRows, null, buffer[0], buffer[1]);
                } else {
                    return createBoxedStringArray(numRows, buffer[0], buffer[1], buffer[2]);
                }
            }
            case TYPE_DATE: {
                if (!nullable) {
                    return createBoxedLocalDateArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedLocalDateArray(numRows, buffer[0], buffer[1]);
                }
            }
            case TYPE_DATETIME: {
                if (!nullable) {
                    return createBoxedLocalDateTimeArray(numRows, null, buffer[0]);
                } else {
                    return createBoxedLocalDateTimeArray(numRows, buffer[0], buffer[1]);
                }
            }
            default:
                throw new RuntimeException("Unsupported UDF TYPE:" + type);
        }
    }

    public static Object[] createBoxedIntegerArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        int[] dataArr = new int[numRows];
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(dataArr);
        dataBuffer.asIntBuffer().get(dataArr);
        if (nullBuffer != null) {
            byte[] nullArr = getNullData(nullBuffer, numRows);
            Integer[] result = new Integer[numRows];
            for (int i = 0; i < numRows; ++i) {
                if (nullArr[i] == 0) {
                    result[i] = dataArr[i];
                }
            }
            return result;
        } else {
            Integer[] result = new Integer[numRows];
            for (int i = 0; i < numRows; ++i) {
                result[i] = dataArr[i];
            }
            return result;
        }
    }

    public static Object[] createBoxedBoolArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        byte[] dataArr = new byte[numRows];
        dataBuffer.get(dataArr);
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            Boolean[] res = new Boolean[numRows];
            for (int i = 0; i < res.length; i++) {
                if (nullArr[i] == 0) {
                    res[i] = dataArr[i] == 1;
                }
            }
            return res;
        } else {
            Boolean[] res = new Boolean[numRows];
            for (int i = 0; i < res.length; i++) {
                res[i] = dataArr[i] == 1;
            }
            return res;
        }
    }

    public static Object[] createBoxedByteArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        byte[] dataArr = new byte[numRows];
        dataBuffer.get(dataArr);
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            Byte[] res = new Byte[numRows];
            for (int i = 0; i < res.length; i++) {
                if (nullArr[i] == 0) {
                    res[i] = dataArr[i];
                }
            }
            return res;
        } else {
            Byte[] res = new Byte[numRows];
            for (int i = 0; i < res.length; i++) {
                res[i] = dataArr[i];
            }
            return res;
        }
    }

    public static Object[] createBoxedShortArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        short[] dataArr = new short[numRows];
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(dataArr);
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            Short[] res = new Short[numRows];
            for (int i = 0; i < res.length; i++) {
                if (nullArr[i] == 0) {
                    res[i] = dataArr[i];
                }
            }
            return res;
        } else {
            Short[] res = new Short[numRows];
            for (int i = 0; i < res.length; i++) {
                res[i] = dataArr[i];
            }
            return res;
        }
    }

    public static Object[] createBoxedLongArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        long[] dataArr = new long[numRows];
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(dataArr);
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            Long[] res = new Long[numRows];
            for (int i = 0; i < res.length; i++) {
                if (nullArr[i] == 0) {
                    res[i] = dataArr[i];
                }
            }
            return res;
        } else {
            return Arrays.stream(dataArr).boxed().toArray(Long[]::new);
        }
    }

    public static Object[] createBoxedFloatArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        float[] dataArr = new float[numRows];
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(dataArr);
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            Float[] res = new Float[numRows];
            for (int i = 0; i < res.length; i++) {
                if (nullArr[i] == 0) {
                    res[i] = dataArr[i];
                }
            }
            return res;
        } else {
            Float[] res = new Float[numRows];
            for (int i = 0; i < res.length; i++) {
                res[i] = dataArr[i];
            }
            return res;
        }
    }

    // Read a TYPE_DATE column (int32 Julian-day per row) into a LocalDate[].
    public static Object[] createBoxedLocalDateArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        int[] dataArr = new int[numRows];
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(dataArr);
        LocalDate[] res = new LocalDate[numRows];
        if (nullBuffer != null) {
            byte[] nullArr = getNullData(nullBuffer, numRows);
            for (int i = 0; i < numRows; i++) {
                if (nullArr[i] == 0) {
                    res[i] = localDateFromJulian(dataArr[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                res[i] = localDateFromJulian(dataArr[i]);
            }
        }
        return res;
    }

    // Read a TYPE_DATETIME column (packed int64 per row) into a LocalDateTime[].
    public static Object[] createBoxedLocalDateTimeArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        long[] dataArr = new long[numRows];
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(dataArr);
        LocalDateTime[] res = new LocalDateTime[numRows];
        if (nullBuffer != null) {
            byte[] nullArr = getNullData(nullBuffer, numRows);
            for (int i = 0; i < numRows; i++) {
                if (nullArr[i] == 0) {
                    res[i] = localDateTimeFromPackedTimestamp(dataArr[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                res[i] = localDateTimeFromPackedTimestamp(dataArr[i]);
            }
        }
        return res;
    }

    public static Object[] createBoxedDoubleArray(int numRows, ByteBuffer nullBuffer, ByteBuffer dataBuffer) {
        double[] dataArr = new double[numRows];
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get(dataArr);
        if (nullBuffer != null) {
            byte[] nullBytes = getNullData(nullBuffer, numRows);
            Double[] res = new Double[numRows];
            for (int i = 0; i < numRows; i++) {
                if (nullBytes[i] == 0) {
                    res[i] = dataArr[i];
                }
            }
            return res;
        } else {
            return Arrays.stream(dataArr).boxed().toArray(Double[]::new);
        }
    }

    private static byte[] getNullData(ByteBuffer nullBuff, int rows) {
        byte[] nullBytes = new byte[rows];
        nullBuff.get(nullBytes);
        return nullBytes;
    }

    // Single JNI entry point for all DECIMAL input conversions. The BE passes the logical
    // type; DECIMAL32/64 use BigDecimal.valueOf(long, scale) via the narrow helper, while
    // DECIMAL128/256 reverse their little-endian byte layout into a BigInteger via the wide
    // helper.
    public static Object[] createBoxedDecimalArray(int type, int numRows, int scale, ByteBuffer nullBuffer,
                                                   ByteBuffer dataBuffer) {
        byte[] nullArr = (nullBuffer != null) ? getNullData(nullBuffer, numRows) : null;
        switch (type) {
            case TYPE_DECIMAL32: {
                int[] dataArr = new int[numRows];
                dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(dataArr);
                BigDecimal[] res = new BigDecimal[numRows];
                for (int i = 0; i < numRows; ++i) {
                    if (nullArr == null || nullArr[i] == 0) {
                        res[i] = BigDecimal.valueOf(dataArr[i], scale);
                    }
                }
                return res;
            }
            case TYPE_DECIMAL64: {
                long[] dataArr = new long[numRows];
                dataBuffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(dataArr);
                BigDecimal[] res = new BigDecimal[numRows];
                for (int i = 0; i < numRows; ++i) {
                    if (nullArr == null || nullArr[i] == 0) {
                        res[i] = BigDecimal.valueOf(dataArr[i], scale);
                    }
                }
                return res;
            }
            case TYPE_DECIMAL128:
                return readWideDecimalArray(numRows, scale, nullArr, dataBuffer, 16);
            case TYPE_DECIMAL256:
                return readWideDecimalArray(numRows, scale, nullArr, dataBuffer, 32);
            default:
                throw new UnsupportedOperationException("unsupported decimal type: " + type);
        }
    }

    // Turn `numRows` little-endian signed-integer values of width `byteWidth` into BigDecimals.
    private static BigDecimal[] readWideDecimalArray(int numRows, int scale, byte[] nullArr, ByteBuffer dataBuffer,
                                                     int byteWidth) {
        byte[] raw = new byte[numRows * byteWidth];
        dataBuffer.get(raw);
        BigDecimal[] res = new BigDecimal[numRows];
        byte[] be = new byte[byteWidth];
        for (int i = 0; i < numRows; ++i) {
            if (nullArr != null && nullArr[i] != 0) {
                continue;
            }
            int base = i * byteWidth;
            // little-endian -> big-endian for BigInteger (sign is preserved from the MSB).
            for (int j = 0; j < byteWidth; ++j) {
                be[j] = raw[base + byteWidth - 1 - j];
            }
            res[i] = new BigDecimal(new BigInteger(be), scale);
        }
        return res;
    }

    public static Object[] createBoxedStringArray(int numRows, ByteBuffer nullBuffer, ByteBuffer offsetBuffer,
                                                  ByteBuffer dataBuffer) {
        final IntBuffer intBuffer = offsetBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        int[] offsets = new int[numRows + 1];
        intBuffer.get(offsets);
        final int byteSize = offsets[offsets.length - 1];
        byte[] bytes = new byte[byteSize];
        dataBuffer.get(bytes);

        String[] strings = new String[numRows];
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            for (int i = 0; i < numRows; i++) {
                if (nullArr[i] == 0) {
                    strings[i] = new String(bytes, offsets[i], offsets[i + 1] - offsets[i], StandardCharsets.UTF_8);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                strings[i] = new String(bytes, offsets[i], offsets[i + 1] - offsets[i], StandardCharsets.UTF_8);
            }
        }

        return strings;
    }

    public static Object[] createBoxedListArray(int numRows, ByteBuffer nullBuffer, ByteBuffer offsetBuffer, Object[] elements) {
        final IntBuffer intBuffer = offsetBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        int[] offsets = new int[numRows + 1];
        intBuffer.get(offsets);
        final int byteSize = offsets[offsets.length - 1];

        List<?>[] lists = new List[numRows];
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            for (int i = 0; i < numRows; i++) {
                if (nullArr[i] == 0) {
                    lists[i] = new ImmutableList<>(elements, offsets[i], offsets[i + 1] - offsets[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                lists[i] = new ImmutableList<>(elements, offsets[i], offsets[i + 1] - offsets[i]);
            }
        }
        return lists;
    }

    public static Object[] createBoxedMapArray(int numRows, ByteBuffer nullBuffer, ByteBuffer offsetBuffer, Object[] keys,
                                               Object[] values) {
        final IntBuffer intBuffer = offsetBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        int[] offsets = new int[numRows + 1];
        intBuffer.get(offsets);

        Map<?, ?>[] maps = new Map[numRows];
        if (nullBuffer != null) {
            byte[] nullArr = new byte[numRows];
            nullBuffer.get(nullArr);
            for (int i = 0; i < numRows; i++) {
                if (nullArr[i] == 0) {
                    ImmutableList<?> k = new ImmutableList<>(keys, offsets[i], offsets[i + 1] - offsets[i]);
                    ImmutableList<?> v = new ImmutableList<>(values, offsets[i], offsets[i + 1] - offsets[i]);
                    maps[i] = new ImmutableMap<>(k, v);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                ImmutableList<?> k = new ImmutableList<>(keys, offsets[i], offsets[i + 1] - offsets[i]);
                ImmutableList<?> v = new ImmutableList<>(values, offsets[i], offsets[i + 1] - offsets[i]);
                maps[i] = new ImmutableMap<>(k, v);
            }
        }
        return maps;
    }

    // batch call void(Object...)
    public static void batchUpdate(Object o, Method method, FunctionStates ctx, int[] states, Object[] column)
            throws Throwable {
        Object[][] inputs = (Object[][]) column;
        boolean isVarArgs = method.isVarArgs();
        int numRows = states.length;
        
        try {
            if (isVarArgs) {
                // For varargs methods, collect all input columns into a single varargs array parameter
                int numVarArgs = inputs.length;
                for (int i = 0; i < numRows; ++i) {
                    Object state = ctx.get(states[i]);
                    Object[] varargsParam = new Object[numVarArgs];
                    for (int j = 0; j < numVarArgs; ++j) {
                        varargsParam[j] = inputs[j][i];
                    }
                    method.invoke(o, state, varargsParam);
                }
            } else {
                // Original logic for non-varargs methods
                Object[] parameter = new Object[inputs.length + 1];
                for (int i = 0; i < numRows; ++i) {
                    parameter[0] = ctx.get(states[i]);
                    for (int j = 0; j < column.length; ++j) {
                        parameter[j + 1] = inputs[j][i];
                    }
                    method.invoke(o, parameter);
                }
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public static void batchUpdateState(Object o, Method method, Object[] column)
            throws Throwable {
        Object[][] inputs = (Object[][]) column;
        boolean isVarArgs = method.isVarArgs();
        int numRows = inputs[0].length;
        
        try {
            if (isVarArgs) {
                // For varargs methods, collect all input columns into a single varargs array parameter
                int numVarArgs = inputs.length;
                for (int i = 0; i < numRows; ++i) {
                    Object[] varargsParam = new Object[numVarArgs];
                    for (int j = 0; j < numVarArgs; ++j) {
                        varargsParam[j] = inputs[j][i];
                    }
                    method.invoke(o, (Object) varargsParam);
                }
            } else {
                // Original logic for non-varargs methods
                Object[] parameter = new Object[inputs.length];
                for (int i = 0; i < numRows; ++i) {
                    for (int j = 0; j < column.length; ++j) {
                        parameter[j] = inputs[j][i];
                    }
                    method.invoke(o, parameter);
                }
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public static void batchUpdateIfNotNull(Object o, Method method, FunctionStates ctx, int[] states, Object[] column)
            throws Throwable {
        Object[][] inputs = (Object[][]) column;
        boolean isVarArgs = method.isVarArgs();
        int numRows = states.length;
        
        try {
            if (isVarArgs) {
                // For varargs methods, collect all input columns into a single varargs array parameter
                int numVarArgs = inputs.length;
                for (int i = 0; i < numRows; ++i) {
                    if (states[i] != -1) {
                        Object state = ctx.get(states[i]);
                        Object[] varargsParam = new Object[numVarArgs];
                        for (int j = 0; j < numVarArgs; ++j) {
                            varargsParam[j] = inputs[j][i];
                        }
                        method.invoke(o, state, varargsParam);
                    }
                }
            } else {
                // Original logic for non-varargs methods
                Object[] parameter = new Object[inputs.length + 1];
                for (int i = 0; i < numRows; ++i) {
                    if (states[i] != -1) {
                        parameter[0] = ctx.get(states[i]);
                        for (int j = 0; j < column.length; ++j) {
                            parameter[j + 1] = inputs[j][i];
                        }
                        method.invoke(o, parameter);
                    }
                }
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public static Object[] batchCreateDirectBuffer(long data, int[] offsets, int size) throws Exception {
        Class<?> directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
        Constructor<?> constructor = directByteBufferClass.getDeclaredConstructor(long.class, int.class);
        constructor.setAccessible(true);

        Object[] res = new Object[size];
        int nums = 0;
        for (int i = 0; i < size; i++) {
            long address = data + offsets[i];
            int length = offsets[i + 1] - offsets[i];
            res[nums++] = constructor.newInstance(address, length);
        }
        return res;
    }

    // batch call Object(Object...)
    public static Object[] batchCall(Object o, Method method, int batchSize, Object[] column)
            throws Throwable {
        Object[][] inputs = (Object[][]) column;
        boolean isVarArgs = method.isVarArgs();
        Object[] res = (Object[]) Array.newInstance(method.getReturnType(), batchSize);

        try {
            if (isVarArgs) {
                // For varargs methods, collect all input columns into a single varargs array parameter
                int numVarArgs = inputs.length;
                for (int i = 0; i < batchSize; ++i) {
                    Object[] varargsParam = new Object[numVarArgs];
                    for (int j = 0; j < numVarArgs; ++j) {
                        varargsParam[j] = inputs[j][i];
                    }
                    res[i] = method.invoke(o, (Object) varargsParam);
                }
            } else {
                // Original logic for non-varargs methods
                Object[] parameter = new Object[inputs.length];
                for (int i = 0; i < batchSize; ++i) {
                    for (int j = 0; j < column.length; ++j) {
                        parameter[j] = inputs[j][i];
                    }
                    res[i] = method.invoke(o, parameter);
                }
            }
            return res;
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    // batch call no arguments function
    public static Object[] batchCall(Object o, Method method, int batchSize)
            throws Throwable {
        try {
            Object[] res = (Object[]) Array.newInstance(method.getReturnType(), batchSize);
            for (int i = 0; i < batchSize; ++i) {
                res[i] = method.invoke(o);
            }
            return res;
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    // batch call int()
    public static int[] batchCall(Object[] o, Method method, int batchSize)
            throws Throwable {
        try {
            int[] res = new int[batchSize];
            for (int i = 0; i < batchSize; ++i) {
                res[i] = (int) method.invoke(o[i]);
            }
            return res;
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public static List<?> extractKeyList(Map<?, ?> map) {
        final ArrayList<Object> res = new ArrayList<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            res.add(entry.getKey());
        }
        return res;
    }

    public static List<?> extractValueList(Map<?, ?> map) {
        final ArrayList<Object> res = new ArrayList<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            res.add(entry.getValue());
        }
        return res;
    }
}
