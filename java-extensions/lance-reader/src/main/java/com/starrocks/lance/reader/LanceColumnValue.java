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

package com.starrocks.lance.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class LanceColumnValue implements ColumnValue {
    private final FieldVector vector;
    private final int rowIndex;

    public LanceColumnValue(FieldVector vector, int rowIndex) {
        this.vector = vector;
        this.rowIndex = rowIndex;
    }

    @Override
    public boolean getBoolean() {
        return ((BitVector) vector).get(rowIndex) == 1;
    }

    @Override
    public byte getByte() {
        return ((TinyIntVector) vector).get(rowIndex);
    }

    @Override
    public short getShort() {
        return ((SmallIntVector) vector).get(rowIndex);
    }

    @Override
    public int getInt() {
        return ((IntVector) vector).get(rowIndex);
    }

    @Override
    public long getLong() {
        return ((BigIntVector) vector).get(rowIndex);
    }

    @Override
    public float getFloat() {
        return ((Float4Vector) vector).get(rowIndex);
    }

    @Override
    public double getDouble() {
        return ((Float8Vector) vector).get(rowIndex);
    }

    @Override
    public String getString(ColumnType.TypeValue type) {
        if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(rowIndex);
            return new String(bytes);
        }
        return vector.getObject(rowIndex).toString();
    }

    @Override
    public byte[] getBytes() {
        if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).get(rowIndex);
        }
        return new byte[0];
    }

    @Override
    public BigDecimal getDecimal() {
        if (vector instanceof DecimalVector) {
            return ((DecimalVector) vector).getObject(rowIndex);
        }
        return BigDecimal.ZERO;
    }

    @Override
    public LocalDate getDate() {
        if (vector instanceof DateDayVector) {
            int daysSinceEpoch = ((DateDayVector) vector).get(rowIndex);
            return LocalDate.ofEpochDay(daysSinceEpoch);
        }
        return LocalDate.ofEpochDay(0);
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        if (vector instanceof TimeStampMicroVector) {
            long micros = ((TimeStampMicroVector) vector).get(rowIndex);
            long epochSecond = micros / 1_000_000;
            int nanoAdjustment = (int) ((micros % 1_000_000) * 1000);
            return LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(epochSecond, nanoAdjustment), ZoneOffset.UTC);
        }
        return LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        throw new UnsupportedOperationException("Lance array type is not yet supported.");
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        throw new UnsupportedOperationException("Lance map type is not yet supported.");
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        throw new UnsupportedOperationException("Lance struct type is not yet supported.");
    }
}
