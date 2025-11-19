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

package com.starrocks.common.util;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.io.Text;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Utility class for serializing and deserializing PartitionKey instances.
 * This class provides centralized serialization logic for partition keys,
 * handling both the key structure and individual literal values.
 */
public class PartitionKeySerializer {

    private static final int DATE_LITERAL_TYPE_DATETIME = 0;
    private static final int DATE_LITERAL_TYPE_DATE = 1;

    /**
     * Write a PartitionKey to DataOutput.
     *
     * @param out the DataOutput to write to
     * @param partitionKey the PartitionKey to serialize
     * @throws IOException if an I/O error occurs
     */
    public static void write(DataOutput out, PartitionKey partitionKey) throws IOException {
        List<LiteralExpr> keys = partitionKey.getKeys();
        List<PrimitiveType> types = partitionKey.getTypes();
        
        int count = keys.size();
        if (count != types.size()) {
            throw new IOException("Size of keys and types are not equal");
        }

        out.writeInt(count);
        for (int i = 0; i < count; i++) {
            PrimitiveType type = types.get(i);
            Text.writeString(out, type.toString());
            if (keys.get(i) == MaxLiteral.MAX_VALUE) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                writeLiteralExpr(out, keys.get(i), type);
            }
        }
    }

    /**
     * Read a PartitionKey from DataInput.
     *
     * @param in the DataInput to read from
     * @return the deserialized PartitionKey
     * @throws IOException if an I/O error occurs
     */
    public static PartitionKey read(DataInput in) throws IOException {
        PartitionKey partitionKey = new PartitionKey();
        
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            PrimitiveType type = PrimitiveType.valueOf(Text.readString(in));
            partitionKey.getTypes().add(type);

            LiteralExpr literal;
            boolean isMax = in.readBoolean();
            if (isMax) {
                literal = MaxLiteral.MAX_VALUE;
            } else {
                literal = readLiteralExpr(in, type);
            }
            literal.setType(TypeFactory.createType(type));
            partitionKey.getKeys().add(literal);
        }
        
        return partitionKey;
    }

    private static void writeLiteralExpr(DataOutput out, LiteralExpr literal, PrimitiveType type) throws IOException {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                writeIntLiteral(out, (IntLiteral) literal);
                break;
            case LARGEINT:
                writeLargeIntLiteral(out, (LargeIntLiteral) literal);
                break;
            case DATE:
            case DATETIME:
                writeDateLiteral(out, (DateLiteral) literal);
                break;
            case CHAR:
            case VARCHAR:
                writeStringLiteral(out, (StringLiteral) literal);
                break;
            default:
                throw new IOException("Unsupported literal type for serialization: " + type);
        }
    }

    private static LiteralExpr readLiteralExpr(DataInput in, PrimitiveType type) throws IOException {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return readIntLiteral(in);
            case LARGEINT:
                return readLargeIntLiteral(in);
            case DATE:
            case DATETIME:
                return readDateLiteral(in);
            case CHAR:
            case VARCHAR:
                return readStringLiteral(in);
            default:
                throw new IOException("Unsupported literal type for deserialization: " + type);
        }
    }

    private static void writeIntLiteral(DataOutput out, IntLiteral literal) throws IOException {
        out.writeLong(literal.getValue());
    }

    private static IntLiteral readIntLiteral(DataInput in) throws IOException {
        long value = in.readLong();
        return new IntLiteral(value);
    }

    private static void writeLargeIntLiteral(DataOutput out, LargeIntLiteral literal) throws IOException {
        Text.writeString(out, literal.getValue().toString());
    }

    private static LargeIntLiteral readLargeIntLiteral(DataInput in) throws IOException {
        String valueStr = Text.readString(in);
        try {
            return new LargeIntLiteral(valueStr);
        } catch (Exception e) {
            throw new IOException("Failed to read LargeIntLiteral", e);
        }
    }

    private static void writeDateLiteral(DataOutput out, DateLiteral literal) throws IOException {
        if (literal.getType().isDatetime()) {
            out.writeShort(DATE_LITERAL_TYPE_DATETIME);
        } else if (literal.getType().isDate()) {
            out.writeShort(DATE_LITERAL_TYPE_DATE);
        } else {
            throw new IOException("Error date literal type : " + literal.getType());
        }
        out.writeLong(makePackedDatetime(literal));
    }

    private static DateLiteral readDateLiteral(DataInput in) throws IOException {
        short dateLiteralType = in.readShort();
        long packedTime = in.readLong();
        
        DateLiteral literal = unpackDatetime(packedTime);
        
        if (dateLiteralType == DATE_LITERAL_TYPE_DATETIME) {
            literal.setType(DateType.DATETIME);
        } else if (dateLiteralType == DATE_LITERAL_TYPE_DATE) {
            literal.setType(DateType.DATE);
        } else {
            throw new IOException("Error date literal type: " + dateLiteralType);
        }
        
        return literal;
    }

    private static void writeStringLiteral(DataOutput out, StringLiteral literal) throws IOException {
        Text.writeString(out, literal.getStringValue());
    }

    private static StringLiteral readStringLiteral(DataInput in) throws IOException {
        String value = Text.readString(in);
        return new StringLiteral(value);
    }

    private static long makePackedDatetime(DateLiteral literal) {
        long year = literal.getYear();
        long month = literal.getMonth();
        long day = literal.getDay();
        long hour = literal.getHour();
        long minute = literal.getMinute();
        long second = literal.getSecond();
        long microsecond = literal.getMicrosecond();

        long ymd = ((year * 13 + month) << 5) | day;
        long hms = (hour << 12) | (minute << 6) | second;
        return (((ymd << 17) | hms) << 24) + microsecond;
    }

    private static DateLiteral unpackDatetime(long packedTime) {
        long microsecond = packedTime % (1L << 24);
        long ymdhms = packedTime >> 24;
        long ymd = ymdhms >> 17;
        long hms = ymdhms % (1 << 17);

        long day = ymd % (1 << 5);
        long ym = ymd >> 5;
        long month = ym % 13;
        long year = (ym / 13) % 10000;
        long second = hms % (1 << 6);
        long minute = (hms >> 6) % (1 << 6);
        long hour = hms >> 12;

        return new DateLiteral((int) year, (int) month, (int) day, 
                              (int) hour, (int) minute, (int) second, (int) microsecond);
    }
}
