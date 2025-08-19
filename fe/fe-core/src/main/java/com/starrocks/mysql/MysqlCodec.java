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

package com.starrocks.mysql;

import com.google.common.base.Strings;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.starrocks.catalog.Type.CHARSET_BINARY;
import static com.starrocks.catalog.Type.CHARSET_UTF8;

public class MysqlCodec {
    public static byte readByte(ByteBuffer buffer) {
        return buffer.get();
    }

    public static int readInt1(ByteBuffer buffer) {
        return readByte(buffer) & 0XFF;
    }

    public static int readInt2(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8);
    }

    public static int readInt3(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16);
    }

    public static int readInt4(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16) | ((readByte(buffer) & 0XFF) << 24);
    }

    public static long readInt6(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt2(buffer)) << 32);
    }

    public static long readInt8(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt4(buffer)) << 32);
    }

    public static long readVInt(ByteBuffer buffer) {
        int b = readInt1(buffer);

        if (b < 251) {
            return b;
        }
        if (b == 252) {
            return readInt2(buffer);
        }
        if (b == 253) {
            return readInt3(buffer);
        }
        if (b == 254) {
            return readInt8(buffer);
        }
        if (b == 251) {
            throw new NullPointerException();
        }
        return 0;
    }

    public static byte[] readFixedString(ByteBuffer buffer, int len) {
        byte[] buf = new byte[len];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readEofString(ByteBuffer buffer) {
        byte[] buf = new byte[buffer.remaining()];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readLenEncodedString(ByteBuffer buffer) {
        long length = readVInt(buffer);
        byte[] buf = new byte[(int) length];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readNulTerminateString(ByteBuffer buffer) {
        int oldPos = buffer.position();
        int nullPos;
        for (nullPos = oldPos; nullPos < buffer.limit(); ++nullPos) {
            if (buffer.get(nullPos) == 0) {
                break;
            }
        }
        byte[] buf = new byte[nullPos - oldPos];
        buffer.get(buf);
        // skip null byte.
        buffer.get();
        return buf;
    }

    public static void writeNull(ByteArrayOutputStream out) {
        writeByte(out, (byte) (251 & 0xff));
    }

    public static void writeByte(ByteArrayOutputStream out, byte value) {
        out.write(value);
    }

    public static void writeBytes(ByteArrayOutputStream out, byte[] value, int offset, int length) {
        out.write(value, offset, length);
    }

    public static void writeBytes(ByteArrayOutputStream out, byte[] value) {
        writeBytes(out, value, 0, value.length);
    }

    public static void writeInt1(ByteArrayOutputStream out, int value) {
        writeByte(out, (byte) (value & 0XFF));
    }

    public static void writeInt2(ByteArrayOutputStream out, int value) {
        writeByte(out, (byte) value);
        writeByte(out, (byte) (value >> 8));
    }

    public static void writeInt3(ByteArrayOutputStream out, int value) {
        writeInt2(out, value);
        writeByte(out, (byte) (value >> 16));
    }

    public static void writeInt4(ByteArrayOutputStream out, int value) {
        writeInt3(out, value);
        writeByte(out, (byte) (value >> 24));
    }

    public static void writeInt6(ByteArrayOutputStream out, long value) {
        writeInt4(out, (int) value);
        writeInt2(out, (byte) (value >> 32));
    }

    public static void writeInt8(ByteArrayOutputStream out, long value) {
        writeInt4(out, (int) value);
        writeInt4(out, (int) (value >> 32));
    }

    public static void writeVInt(ByteArrayOutputStream out, long value) {
        if (value < 251) {
            writeByte(out, (byte) value);
        } else if (value < 0x10000) {
            writeInt1(out, 252);
            writeInt2(out, (int) value);
        } else if (value < 0x1000000) {
            writeInt1(out, 253);
            writeInt3(out, (int) value);
        } else {
            writeInt1(out, 254);
            writeInt8(out, value);
        }
    }

    public static void writeLenEncodedString(ByteArrayOutputStream out, String value) {
        byte[] buf = value.getBytes(StandardCharsets.UTF_8);
        writeVInt(out, buf.length);
        writeBytes(out, buf);
    }

    public static void writeEofString(ByteArrayOutputStream out, String value) {
        byte[] buf = value.getBytes(StandardCharsets.UTF_8);
        writeBytes(out, buf);
    }

    public static void writeNulTerminateString(ByteArrayOutputStream out, String value) {
        writeEofString(out, value);
        writeByte(out, (byte) 0);
    }

    public static void writeField(ByteArrayOutputStream out, String colName, Type type) {
        writeField(out, "", "", colName, type, false, "");
    }

    /**
     * Format field with name and type using Protocol::ColumnDefinition41
     * <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition">...</a>
     */
    public static void writeField(ByteArrayOutputStream out, String db, String table,
                                  String colName, Type type, boolean sendDefault, String defaultValue) {
        // GlobalStateMgr Name: length encoded string
        writeLenEncodedString(out, "def");
        // Schema: length encoded string
        writeLenEncodedString(out, db);
        // Table: length encoded string
        writeLenEncodedString(out, table);
        // Origin Table: length encoded string
        writeLenEncodedString(out, table);
        // Name: length encoded string
        writeLenEncodedString(out, colName);
        // Original Name: length encoded string
        writeLenEncodedString(out, colName);
        // length of the following fields(always 0x0c)
        writeVInt(out, 0x0c);
        // Character set: two byte integer
        writeInt2(out, getMysqlResultSetFieldCharsetIndex(type));
        // Column length: four byte integer
        writeInt4(out, getMysqlResultSetFieldLength(type));
        // Column type: one byte integer
        writeInt1(out, toMysqlType(type.getPrimitiveType()).getCode());
        // Flags: two byte integer
        writeInt2(out, 0);
        // Decimals: one byte integer
        writeInt1(out, type.getMysqlResultSetFieldDecimals());
        // filler: two byte integer
        writeInt2(out, 0);

        if (sendDefault) {
            // Sending default value.
            writeLenEncodedString(out, Strings.nullToEmpty(defaultValue));
        }
    }

    public static MysqlColType toMysqlType(PrimitiveType type) {
        switch (type) {
            // MySQL use Tinyint(1) to represent boolean
            case BOOLEAN:
            case TINYINT:
                return MysqlColType.MYSQL_TYPE_TINY;
            case SMALLINT:
                return MysqlColType.MYSQL_TYPE_SHORT;
            case INT:
                return MysqlColType.MYSQL_TYPE_LONG;
            case BIGINT:
                return MysqlColType.MYSQL_TYPE_LONGLONG;
            case FLOAT:
                return MysqlColType.MYSQL_TYPE_FLOAT;
            case DOUBLE:
                return MysqlColType.MYSQL_TYPE_DOUBLE;
            case TIME:
                return MysqlColType.MYSQL_TYPE_TIME;
            case DATE:
                return MysqlColType.MYSQL_TYPE_DATE;
            case DATETIME:
                return MysqlColType.MYSQL_TYPE_DATETIME;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return MysqlColType.MYSQL_TYPE_NEWDECIMAL;
            case VARCHAR:
                return MysqlColType.MYSQL_TYPE_VAR_STRING;
            case VARBINARY:
                return MysqlColType.MYSQL_TYPE_BLOB;
            default:
                return MysqlColType.MYSQL_TYPE_STRING;
        }
    }

    /**
     * <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition">...</a>
     * column_length (4) -- maximum length of the field
     * <p>
     * Maximum length of result of evaluating this item, in number of bytes.
     * - For character or blob data types, max char length multiplied by max
     * character size (collation.mbmaxlen).
     * - For decimal type, it is the precision in digits plus sign (unless
     * unsigned) plus decimal point (unless it has zero decimals).
     * - For other numeric types, the default or specific display length.
     * - For date/time types, the display length (10 for DATE, 10 + optional FSP
     * for TIME, 19 + optional fsp for datetime/timestamp). fsp is the fractional seconds precision.
     */
    public static int getMysqlResultSetFieldLength(Type type) {
        switch (type.getPrimitiveType()) {
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 4;
            case SMALLINT:
                return 6;
            case INT:
                return 11;
            case BIGINT:
                return 20;
            case LARGEINT:
                return 40;
            case DATE:
                return 10;
            case DATETIME:
                return 19;
            case FLOAT:
                return 12;
            case DOUBLE:
                return 22;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                // precision + (scale > 0 ? 1 : 0) + (unsigned_flag || !precision ? 0 : 1));
                ScalarType decimalType = (ScalarType) type;
                int length = decimalType.getScalarPrecision();
                // when precision is 0 it means that original length was also 0.
                if (length == 0) {
                    return 0;
                }
                // scale > 0
                if (decimalType.getScalarScale() > 0) {
                    length += 1;
                }
                // one byte for sign
                // one byte for zero, if precision == scale
                // one byte for overflow but valid decimal
                return length + 3;
            case CHAR:
            case VARCHAR:
            case HLL:
            case BITMAP:
            case VARBINARY:
                ScalarType charType = ((ScalarType) type);
                int charLength = charType.getLength();
                if (charLength == -1) {
                    charLength = 64;
                }
                // utf8 charset
                return charLength * 3;
            default:
                // Treat ARRAY/MAP/STRUCT as VARCHAR(-1)
                return 60;
        }
    }

    /**
     * @return 33 (utf8_general_ci) if type is char varchar hll or bitmap
     * 63 (binary) others
     * <p>
     * <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition">...</a>
     * character_set (2) -- is the column character set and is defined in Protocol::CharacterSet.
     */
    public static int getMysqlResultSetFieldCharsetIndex(Type type) {
        if (type instanceof ArrayType || type instanceof MapType || type instanceof StructType) {
            return CHARSET_UTF8;
        } else {
            return switch (type.getPrimitiveType()) {
                // Because mysql does not have a large int type, mysql will treat it as hex after exceeding bigint
                case CHAR, VARCHAR, HLL, BITMAP, LARGEINT, JSON -> CHARSET_UTF8;
                default -> CHARSET_BINARY;
            };
        }
    }
}
