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

package com.starrocks.paimon.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataSetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.apache.paimon.data.Timestamp.fromEpochMillis;

public final class StarRocksRow implements InternalRow, DataSetters, Serializable {

    private static final Logger LOG = LogManager.getLogger(StarRocksRow.class);
    private static final long serialVersionUID = 1L;
    private RowKind rowKind = RowKind.INSERT;
    private int rowCount;
    private final List<Object[]> data;

    public StarRocksRow(List<Object[]> objectArrays) {
        this(objectArrays, 0);
    }

    public StarRocksRow(List<Object[]> objectArrays, int rowCount) {
        this.data = objectArrays;
        this.rowCount = rowCount;
    }

    @Override
    public int getFieldCount() {
        return data.size();
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    @Override
    public boolean isNullAt(int pos) {
        return null == data.get(pos)[rowCount];
    }

    @Override
    public boolean getBoolean(int pos) {
        return (Boolean) data.get(pos)[rowCount];
    }

    @Override
    public byte getByte(int pos) {
        return (Byte) data.get(pos)[rowCount];
    }

    @Override
    public short getShort(int pos) {
        return (Short) data.get(pos)[rowCount];
    }

    @Override
    public int getInt(int pos) {
        return (Integer) data.get(pos)[rowCount];
    }

    @Override
    public long getLong(int pos) {
        return (Long) data.get(pos)[rowCount];
    }

    @Override
    public float getFloat(int pos) {
        return (Float) data.get(pos)[rowCount];
    }

    @Override
    public double getDouble(int pos) {
        return (Double) data.get(pos)[rowCount];
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromString((String) data.get(pos)[rowCount]);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        String[] parts = ((String) (data.get(pos)[rowCount])).split(" ");
        BigDecimal bd = new BigDecimal(new BigInteger(parts[0]), Integer.parseInt(parts[2]));
        return Decimal.fromBigDecimal(bd, Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    }

    @Override
    public Timestamp getTimestamp(int pos, int i1) {
        return fromEpochMillis((Long) data.get(pos)[rowCount]);
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) (data.get(pos)[rowCount]);
    }

    @Override
    public Variant getVariant(int i) {
        // todo: do not support variant now
        return null;
    }

    @Override
    public InternalArray getArray(int pos) {
        return null;
    }

    @Override
    public InternalMap getMap(int pos) {
        return null;
    }

    @Override
    public InternalRow getRow(int pos, int i1) {
        return null;
    }

    @Override
    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setBoolean(int pos, boolean b) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setByte(int pos, byte b) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setShort(int pos, short i1) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setInt(int pos, int i1) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setLong(int pos, long l) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setFloat(int pos, float v) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDouble(int pos, double v) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDecimal(int pos, Decimal decimal, int i1) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setTimestamp(int pos, Timestamp timestamp, int i1) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public static StarRocksRow of(List<Object[]> objectArrays) {
        return new StarRocksRow(objectArrays);
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }
}
