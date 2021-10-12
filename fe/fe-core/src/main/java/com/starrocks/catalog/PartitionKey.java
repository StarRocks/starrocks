// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.PartitionValue;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class PartitionKey implements Comparable<PartitionKey>, Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionKey.class);
    private List<LiteralExpr> keys;
    private List<PrimitiveType> types;

    // constructor for partition prune
    public PartitionKey() {
        keys = Lists.newArrayList();
        types = Lists.newArrayList();
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
                value = "\"" + expr.getRealValue() + "\"";
                if (expr instanceof DateLiteral) {
                    DateLiteral dateLiteral = (DateLiteral) expr;
                    value = dateLiteral.toSql();
                }
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
                value = expr.getRealValue();
                if (expr instanceof DateLiteral) {
                    DateLiteral dateLiteral = (DateLiteral) expr;
                    value = dateLiteral.getStringValue();
                }
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PartitionKey)) {
            return false;
        }

        PartitionKey partitionKey = (PartitionKey) obj;

        // Check keys
        if (keys != partitionKey.keys) {
            if (keys.size() != partitionKey.keys.size()) {
                return false;
            }
            for (int i = 0; i < keys.size(); i++) {
                if (!keys.get(i).equals(partitionKey.keys.get(i))) {
                    return false;
                }
            }
        }

        // Check types
        if (types != partitionKey.types) {
            if (types.size() != partitionKey.types.size()) {
                return false;
            }
            for (int i = 0; i < types.size(); i++) {
                if (!types.get(i).equals(partitionKey.types.get(i))) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int code = 0;
        for (LiteralExpr expr : keys) {
            code += code * 31 + expr.hashCode();
        }
        return code;
    }
}
