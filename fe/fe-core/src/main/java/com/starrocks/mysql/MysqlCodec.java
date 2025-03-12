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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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

    public static void writeField(ByteArrayOutputStream out, String db, String table, Column column, boolean sendDefault) {
        Type columnType = column.getType();
        // GlobalStateMgr Name: length encoded string
        writeLenEncodedString(out, "def");
        // Schema: length encoded string
        writeLenEncodedString(out, db);
        // Table: length encoded string
        writeLenEncodedString(out, table);
        // Origin Table: length encoded string
        writeLenEncodedString(out, table);
        // Name: length encoded string
        writeLenEncodedString(out, column.getName());
        // Original Name: length encoded string
        writeLenEncodedString(out, column.getName());
        // length of the following fields(always 0x0c)
        writeVInt(out, 0x0c);
        // Character set: two byte integer
        writeInt2(out, columnType.getMysqlResultSetFieldCharsetIndex());
        // Column length: four byte integer
        writeInt4(out, columnType.getMysqlResultSetFieldLength());
        // Column type: one byte integer
        writeInt1(out, column.getPrimitiveType().toMysqlType().getCode());
        // Flags: two byte integer
        writeInt2(out, 0);
        // Decimals: one byte integer
        writeInt1(out, columnType.getMysqlResultSetFieldDecimals());
        // filler: two byte integer
        writeInt2(out, 0);

        if (sendDefault) {
            // Sending default value.
            writeLenEncodedString(out, Strings.nullToEmpty(column.getDefaultValue()));
        }
    }

    /**
     * Format field with name and type using Protocol::ColumnDefinition41
     * <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition">...</a>
     */
    public static void writeField(ByteArrayOutputStream out, String colName, Type type) {
        // GlobalStateMgr Name: length encoded string
        writeLenEncodedString(out, "def");
        // Schema: length encoded string
        writeLenEncodedString(out, "");
        // Table: length encoded string
        writeLenEncodedString(out, "");
        // Origin Table: length encoded string
        writeLenEncodedString(out, "");
        // Name: length encoded string
        writeLenEncodedString(out, colName);
        // Original Name: length encoded string
        writeLenEncodedString(out, colName);
        // length of the following fields(always 0x0c)
        writeVInt(out, 0x0c);
        // Character set: two byte integer
        writeInt2(out, type.getMysqlResultSetFieldCharsetIndex());
        // Column length: four byte integer
        writeInt4(out, type.getMysqlResultSetFieldLength());
        // Column type: one byte integer
        writeInt1(out, type.getMysqlResultType().getCode());
        // Flags: two byte integer
        writeInt2(out, 0);
        // Decimals: one byte integer
        writeInt1(out, type.getMysqlResultSetFieldDecimals());
        // filler: two byte integer
        writeInt2(out, 0);
    }
}
