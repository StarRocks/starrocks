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

package com.starrocks.odps.reader;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.table.arrow.accessor.ArrowBigIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowBitAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowDateDayAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowDecimalAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowFloat4Accessor;
import com.aliyun.odps.table.arrow.accessor.ArrowFloat8Accessor;
import com.aliyun.odps.table.arrow.accessor.ArrowIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowSmallIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowTimestampAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowTinyIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVarBinaryAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVarCharAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsTypeUtils {
    public static ArrowVectorAccessor createColumnVectorAccessor(ValueVector vector, TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return new ArrowBitAccessor((BitVector) vector);
            case TINYINT:
                return new ArrowTinyIntAccessor((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowSmallIntAccessor((SmallIntVector) vector);
            case INT:
                return new ArrowIntAccessor((IntVector) vector);
            case BIGINT:
                return new ArrowBigIntAccessor((BigIntVector) vector);
            case FLOAT:
                return new ArrowFloat4Accessor((Float4Vector) vector);
            case DOUBLE:
                return new ArrowFloat8Accessor((Float8Vector) vector);
            case DECIMAL:
                return new ArrowDecimalAccessor((DecimalVector) vector);
            case STRING:
            case VARCHAR:
            case CHAR:
            case JSON:
                return new ArrowVarCharAccessor((VarCharVector) vector);
            case BINARY:
                return new ArrowVarBinaryAccessor((VarBinaryVector) vector);
            case DATE:
                return new ArrowDateDayAccessor((DateDayVector) vector);
            case DATETIME:
                return new ArrowTimestampAccessor((TimeStampVector) vector);
            case TIMESTAMP:
                return new ArrowTimestampAccessor((TimeStampVector) vector);
            default:
                throw new UnsupportedOperationException(
                        "Datatype not supported: " + typeInfo.getTypeName());
        }
    }

    public static Object getData(ArrowVectorAccessor dataAccessor, TypeInfo typeInfo, int rowId)
            throws IOException {
        if (dataAccessor.isNullAt(rowId)) {
            return null;
        }
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return ((ArrowBitAccessor) dataAccessor).getBoolean(rowId);
            case TINYINT:
                return ((ArrowTinyIntAccessor) dataAccessor).getByte(rowId);
            case SMALLINT:
                return ((ArrowSmallIntAccessor) dataAccessor).getShort(rowId);
            case INT:
                return ((ArrowIntAccessor) dataAccessor).getInt(rowId);
            case BIGINT:
                return ((ArrowBigIntAccessor) dataAccessor).getLong(rowId);
            case FLOAT:
                return ((ArrowFloat4Accessor) dataAccessor).getFloat(rowId);
            case DOUBLE:
                return ((ArrowFloat8Accessor) dataAccessor).getDouble(rowId);
            case DECIMAL:
                return ((ArrowDecimalAccessor) dataAccessor).getDecimal(rowId);
            case STRING:
                return ((ArrowVarCharAccessor) dataAccessor).getBytes(rowId);
            case VARCHAR:
                return new Varchar(new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId),
                        ConfigConstants.DEFAULT_CHARSET));
            case CHAR:
                return new Char(new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId),
                        ConfigConstants.DEFAULT_CHARSET));
            case BINARY:
                return new Binary(((ArrowVarBinaryAccessor) dataAccessor).getBinary(rowId));
            case DATE:
                return LocalDate.ofEpochDay(((ArrowDateDayAccessor) dataAccessor).getEpochDay(rowId));
            case DATETIME:
                return convertToTimeStamp(((ArrowTimestampAccessor) dataAccessor).getType(),
                        ((ArrowTimestampAccessor) dataAccessor).getEpochTime(
                                rowId)).atZone(ZoneId.systemDefault());
            case TIMESTAMP:
                return convertToTimeStamp(((ArrowTimestampAccessor) dataAccessor).getType(),
                        ((ArrowTimestampAccessor) dataAccessor).getEpochTime(rowId));
            default:
                throw new UnsupportedOperationException(
                        "Datatype not supported: " + typeInfo.getTypeName());
        }
    }

    private static Instant convertToTimeStamp(ArrowType.Timestamp timestampType, long epochTime) {
        switch (timestampType.getUnit()) {
            case SECOND:
                return Instant.ofEpochSecond(epochTime);
            case MILLISECOND:
                return Instant.ofEpochMilli(epochTime);
            case MICROSECOND:
                return microsToInstant(epochTime);
            case NANOSECOND:
                return nanosToInstant(epochTime);
            default:
                throw new UnsupportedOperationException("Unit not supported: " + timestampType.getUnit());
        }
    }

    private static LocalDateTime convertToTimeStampNtz(ArrowType.Timestamp timestampType,
                                                       long epochTime) {
        return LocalDateTime.ofInstant(convertToTimeStamp(timestampType, epochTime), ZoneOffset.UTC);
    }

    private static Instant microsToInstant(long micros) {
        long secs = Math.floorDiv(micros, MICROS_PER_SECOND);
        long mos = micros - secs * MICROS_PER_SECOND;
        return Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS);
    }

    private static Instant nanosToInstant(long nanos) {
        long secs = Math.floorDiv(nanos, NANOS_PER_SECOND);
        long nos = nanos - secs * NANOS_PER_SECOND;
        return Instant.ofEpochSecond(secs, nos);
    }

    public static final long MILLIS_PER_SECOND = 1000L;

    public static final long MICROS_PER_MILLIS = 1000L;
    public static final long MICROS_PER_SECOND = MILLIS_PER_SECOND * MICROS_PER_MILLIS;

    public static final long NANOS_PER_MICROS = 1000L;
    public static final long NANOS_PER_MILLIS = MICROS_PER_MILLIS * NANOS_PER_MICROS;
    public static final long NANOS_PER_SECOND = MILLIS_PER_SECOND * NANOS_PER_MILLIS;

    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static String formatDateTime(LocalDateTime dateTime) {
        return dateTime.format(DATETIME_FORMATTER);
    }

    public static String formatDate(LocalDate date) {
        return date.format(DATE_FORMATTER);
    }
}
