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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/PartitionKey.java

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

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.PartitionValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;

public class PartitionKey implements Comparable<PartitionKey>, Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionKey.class);
    private List<LiteralExpr> keys;
    private List<PrimitiveType> types;
    // Records the string corresponding to partition value when the partition value is null
    // for hive, it's __HIVE_DEFAULT_PARTITION__
    // for hudi， it's __HIVE_DEFAULT_PARTITION__ or default
    private String nullPartitionValue = "";

    private static final DateLiteral SHADOW_DATE_LITERAL = new DateLiteral(0, 0, 0);
    private static final DateLiteral SHADOW_DATETIME_LITERAL = new DateLiteral(0, 0, 0, 0, 0, 0, 0);

    // constructor for partition prune
    public PartitionKey() {
        keys = Lists.newArrayList();
        types = Lists.newArrayList();
    }

    // used for UT
    public PartitionKey(List<LiteralExpr> keyValue, List<PrimitiveType> keyType) {
        keys = keyValue;
        types = keyType;
    }

    public void setNullPartitionValue(String nullPartitionValue) {
        this.nullPartitionValue = nullPartitionValue;
    }

    public String getNullPartitionValue() {
        return nullPartitionValue;
    }

    // Factory methods
    public static PartitionKey createInfinityPartitionKey(List<Column> columns, boolean isMax)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        for (Column column : columns) {
            partitionKey.keys.add(LiteralExpr.createInfinity(Type.fromPrimitiveType(column.getPrimitiveType()), isMax));
            partitionKey.types.add(column.getPrimitiveType());
        }
        return partitionKey;
    }

    public static PartitionKey createInfinityPartitionKeyWithType(List<PrimitiveType> types, boolean isMax)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        for (PrimitiveType type : types) {
            partitionKey.keys.add(LiteralExpr.createInfinity(Type.fromPrimitiveType(type), isMax));
            partitionKey.types.add(type);
        }
        return partitionKey;
    }

    public static PartitionKey createShadowPartitionKey(List<Column> columns)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        for (Column column : columns) {
            PrimitiveType primitiveType = column.getPrimitiveType();
            LiteralExpr shadowLiteral;
            switch (primitiveType) {
                case DATE:
                    shadowLiteral = SHADOW_DATE_LITERAL;
                    break;
                case DATETIME:
                    shadowLiteral = SHADOW_DATETIME_LITERAL;
                    break;
                default:
                    throw new AnalysisException("Unsupported shadow partition type:" + primitiveType);
            }
            partitionKey.keys.add(shadowLiteral);
            partitionKey.types.add(column.getPrimitiveType());
        }
        return partitionKey;
    }

    public static PartitionKey createPartitionKey(List<PartitionValue> keys, List<Column> columns)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        Preconditions.checkArgument(keys.size() <= columns.size());
        int i;
        for (i = 0; i < keys.size(); ++i) {
            partitionKey.keys.add(keys.get(i).getValue(
                    Type.fromPrimitiveType(columns.get(i).getPrimitiveType())));
            partitionKey.types.add(columns.get(i).getPrimitiveType());
        }

        // fill the vacancy with MIN
        for (; i < columns.size(); ++i) {
            Type type = Type.fromPrimitiveType(columns.get(i).getPrimitiveType());
            partitionKey.keys.add(LiteralExpr.createInfinity(type, false));
            partitionKey.types.add(columns.get(i).getPrimitiveType());
        }

        Preconditions.checkState(partitionKey.keys.size() == columns.size());
        return partitionKey;
    }

    public static PartitionKey ofDateTime(LocalDateTime dateTime) throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.keys.add(new DateLiteral(dateTime, Type.DATETIME));
        partitionKey.types.add(PrimitiveType.DATETIME);
        return partitionKey;
    }

    public static PartitionKey ofDate(LocalDate date) throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.keys.add(new DateLiteral(LocalDateTime.of(date, LocalTime.MIN), Type.DATE));
        partitionKey.types.add(PrimitiveType.DATE);
        return partitionKey;
    }

    public static PartitionKey ofString(String str) {
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.keys.add(new StringLiteral(str));
        partitionKey.types.add(PrimitiveType.VARCHAR);
        return partitionKey;
    }

    public void pushColumn(LiteralExpr keyValue, PrimitiveType keyType) {
        keys.add(keyValue);
        types.add(keyType);
    }

    public void popColumn() {
        keys.remove(keys.size() - 1);
        types.remove(types.size() - 1);
    }

    public List<LiteralExpr> getKeys() {
        return keys;
    }

    public List<PrimitiveType> getTypes() {
        return types;
    }

    public boolean isMinValue() {
        for (LiteralExpr literalExpr : keys) {
            if (!literalExpr.isMinValue()) {
                return false;
            }
        }
        return true;
    }

    public boolean isMaxValue() {
        for (LiteralExpr literalExpr : keys) {
            if (literalExpr != MaxLiteral.MAX_VALUE) {
                return false;
            }
        }
        return true;
    }

    public static int compareLiteralExpr(LiteralExpr key1, LiteralExpr key2) {
        int ret = 0;
        if (key1 instanceof MaxLiteral || key2 instanceof MaxLiteral) {
            ret = key1.compareLiteral(key2);
        } else {
            final Type destType = Type.getAssignmentCompatibleType(key1.getType(), key2.getType(), false);
            try {
                LiteralExpr newKey = key1;
                if (key1.getType() != destType) {
                    newKey = (LiteralExpr) key1.castTo(destType);
                }
                LiteralExpr newOtherKey = key2;
                if (key2.getType() != destType) {
                    newOtherKey = (LiteralExpr) key2.castTo(destType);
                }
                ret = newKey.compareLiteral(newOtherKey);
            } catch (AnalysisException e) {
                throw new RuntimeException("Cast error in partition");
            }
        }
        return ret;
    }

    // compare with other PartitionKey. used for partition prune
    @Override
    public int compareTo(PartitionKey other) {
        int thisKeyLen = this.keys.size();
        int otherKeyLen = other.keys.size();
        int minLen = Math.min(thisKeyLen, otherKeyLen);
        for (int i = 0; i < minLen; ++i) {
            int ret = compareLiteralExpr(this.getKeys().get(i), other.getKeys().get(i));
            if (0 != ret) {
                return ret;
            }
        }
        return Integer.compare(thisKeyLen, otherKeyLen);
    }

    public PartitionKey predecessor() {
        Preconditions.checkArgument(keys.size() == 1);
        if (isMinValue() || isMaxValue()) {
            return this;
        }
        LiteralExpr literal = keys.get(0);
        PrimitiveType type = types.get(0);
        PartitionKey key = new PartitionKey();

        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT: {
                IntLiteral intLiteral = (IntLiteral) literal;
                final long minValue = -(1L << ((type.getSlotSize() << 3) - 1));
                long pred = intLiteral.getValue();
                pred -= pred > minValue ? 1L : 0L;
                key.pushColumn(new IntLiteral(pred, Type.fromPrimitiveType(type)), type);
                return key;
            }
            case LARGEINT: {
                LargeIntLiteral largeIntLiteral = (LargeIntLiteral) literal;
                final BigInteger minValue = BigInteger.ONE.shiftLeft(127).negate();
                BigInteger pred = largeIntLiteral.getValue();
                pred = pred.subtract(pred.compareTo(minValue) > 0 ? BigInteger.ONE : BigInteger.ZERO);
                try {
                    key.pushColumn(new LargeIntLiteral(pred.toString()), type);
                    return key;
                } catch (Exception ignored) {
                    Preconditions.checkArgument(false, "Never reach here");
                }
            }
            case DATE:
            case DATETIME: {
                DateLiteral dateLiteral = (DateLiteral) literal;
                Calendar calendar = Calendar.getInstance();
                int year = (int) dateLiteral.getYear();
                int mon = (int) dateLiteral.getMonth() - 1;
                int day = (int) dateLiteral.getDay();
                int hour = (int) dateLiteral.getHour();
                int min = (int) dateLiteral.getMinute();
                int sec = (int) dateLiteral.getSecond();
                calendar.set(year, mon, day, hour, min, sec);
                calendar.add(type == PrimitiveType.DATE ? Calendar.DATE : Calendar.SECOND, -1);
                year = calendar.get(Calendar.YEAR);
                mon = calendar.get(Calendar.MONTH) + 1;
                day = calendar.get(Calendar.DATE);
                hour = calendar.get(Calendar.HOUR_OF_DAY);
                min = calendar.get(Calendar.MINUTE);
                sec = calendar.get(Calendar.SECOND);
                if (type == PrimitiveType.DATE) {
                    dateLiteral = new DateLiteral(year, mon, day);
                } else {
                    dateLiteral = new DateLiteral(year, mon, day, hour, min, sec, 0);
                }
                key.pushColumn(dateLiteral, type);
                return key;
            }
            case CHAR:
            case VARCHAR: {
                StringLiteral stringLiteral = (StringLiteral) literal;
                key.pushColumn(new StringLiteral(stringLiteral.getStringValue()), type);
                return key;
            }
            default:
                Preconditions.checkArgument(false, "Never reach here");
                return null;
        }
    }

    public PartitionKey successor() {
        Preconditions.checkArgument(keys.size() == 1);
        if (isMaxValue()) {
            return this;
        }
        LiteralExpr literal = keys.get(0);
        PrimitiveType type = types.get(0);
        PartitionKey key = new PartitionKey();

        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT: {
                IntLiteral intLiteral = (IntLiteral) literal;
                final long maxValue = (1L << ((type.getSlotSize() << 3) - 1)) - 1L;
                long succ = intLiteral.getValue();
                succ += succ < maxValue ? 1L : 0L;
                key.pushColumn(new IntLiteral(succ, Type.fromPrimitiveType(type)), type);
                return key;
            }
            case LARGEINT: {
                LargeIntLiteral largeIntLiteral = (LargeIntLiteral) literal;
                final BigInteger maxValue = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
                BigInteger succ = largeIntLiteral.getValue();
                succ = succ.add(succ.compareTo(maxValue) < 0 ? BigInteger.ONE : BigInteger.ZERO);
                try {
                    key.pushColumn(new LargeIntLiteral(succ.toString()), type);
                    return key;
                } catch (Exception ignored) {
                    Preconditions.checkArgument(false, "Never reach here");
                }
            }
            case DATE:
            case DATETIME: {
                DateLiteral dateLiteral = (DateLiteral) literal;
                Calendar calendar = Calendar.getInstance();
                int year = (int) dateLiteral.getYear();
                int mon = (int) dateLiteral.getMonth() - 1;
                int day = (int) dateLiteral.getDay();
                int hour = (int) dateLiteral.getHour();
                int min = (int) dateLiteral.getMinute();
                int sec = (int) dateLiteral.getSecond();
                calendar.set(year, mon, day, hour, min, sec);
                calendar.add(type == PrimitiveType.DATE ? Calendar.DATE : Calendar.SECOND, 1);
                year = calendar.get(Calendar.YEAR);
                mon = calendar.get(Calendar.MONTH) + 1;
                day = calendar.get(Calendar.DATE);
                hour = calendar.get(Calendar.HOUR_OF_DAY);
                min = calendar.get(Calendar.MINUTE);
                sec = calendar.get(Calendar.SECOND);
                if (type == PrimitiveType.DATE) {
                    dateLiteral = new DateLiteral(year, mon, day);
                } else {
                    dateLiteral = new DateLiteral(year, mon, day, hour, min, sec, 0);
                }
                key.pushColumn(dateLiteral, type);
                return key;
            }
            case CHAR:
            case VARCHAR: {
                StringLiteral stringLiteral = (StringLiteral) literal;
                key.pushColumn(new StringLiteral(stringLiteral.getStringValue()), type);
                return key;
            }
            default:
                Preconditions.checkArgument(false, "Never reach here");
                return null;
        }
    }

    // return: ("100", "200", "300")
    public String toSql() {
        StringBuilder sb = new StringBuilder("(");
        int i = 0;
        for (LiteralExpr expr : keys) {
            Object value = null;
            if (expr == MaxLiteral.MAX_VALUE) {
                value = expr.toSql();
                sb.append(value);
                continue;
            } else {
                value = "\"" + expr.getStringValue() + "\"";
            }
            sb.append(value);

            if (keys.size() - 1 != i) {
                sb.append(", ");
            }
            i++;
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("types: [");
        builder.append(Joiner.on(", ").join(types));
        builder.append("]; ");

        builder.append("keys: [");
        int i = 0;
        for (LiteralExpr expr : keys) {
            Object value = null;
            if (expr == MaxLiteral.MAX_VALUE) {
                value = expr.toSql();
            } else {
                value = expr.getStringValue();
            }
            if (keys.size() - 1 == i) {
                builder.append(value);
            } else {
                builder.append(value + ", ");
            }
            ++i;
        }
        builder.append("]; ");

        return builder.toString();
    }

    public static PartitionKey fromString(String partitionKey) {
        String[] partitionKeyArray = partitionKey.split(";");
        String types = partitionKeyArray[0];
        String keys = partitionKeyArray[1];

        String typeContent = types.substring(types.indexOf("[") + 1, types.indexOf("]"));
        String keyContent = keys.substring(keys.indexOf("[") + 1, keys.indexOf("]"));
        if (typeContent.isEmpty() || keyContent.isEmpty()) {
            return new PartitionKey();
        }

        String[] typeArray = typeContent.split(",");
        String[] keyArray = keyContent.split(",");
        Preconditions.checkState(typeArray.length == keyArray.length);

        List<LiteralExpr> keyList = Lists.newArrayList();
        List<PrimitiveType> typeList = Lists.newArrayList();
        for (int index = 0; index < typeArray.length; ++index) {
            PrimitiveType type = PrimitiveType.valueOf(typeArray[index].toUpperCase());
            LiteralExpr expr = NullLiteral.create(Type.fromPrimitiveType(type));
            try {
                expr = LiteralExpr.create(keyArray[index], Type.fromPrimitiveType(type));
            } catch (AnalysisException ignored) {
            }
            typeList.add(type);
            keyList.add(expr);
        }
        return new PartitionKey(keyList, typeList);
    }

    @Override
    public void write(DataOutput out) throws IOException {
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
                keys.get(i).write(out);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            PrimitiveType type = PrimitiveType.valueOf(Text.readString(in));
            types.add(type);

            LiteralExpr literal = null;
            boolean isMax = in.readBoolean();
            if (isMax) {
                literal = MaxLiteral.MAX_VALUE;
            } else {
                switch (type) {
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                        literal = IntLiteral.read(in);
                        break;
                    case LARGEINT:
                        literal = LargeIntLiteral.read(in);
                        break;
                    case DATE:
                    case DATETIME:
                        literal = DateLiteral.read(in);
                        break;
                    case CHAR:
                    case VARCHAR:
                        literal =  StringLiteral.read(in);
                        break;
                    default:
                        throw new IOException("type[" + type.name() + "] not supported: ");
                }
            }
            literal.setType(Type.fromPrimitiveType(type));
            keys.add(literal);
        }
    }

    public static PartitionKey read(DataInput in) throws IOException {
        PartitionKey key = new PartitionKey();
        key.readFields(in);
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionKey that = (PartitionKey) o;
        assert Objects.equals(types, that.types);
        return Objects.equals(keys, that.keys) && Objects.equals(types, that.types);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keys, types);
    }
}
