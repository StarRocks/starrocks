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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.proto.VariantPB;
import com.starrocks.proto.VariantTypePB;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.thrift.TVariant;
import com.starrocks.thrift.TVariantType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeSerializer;

import java.util.List;

/*
 * Variant is used to store a value ​​of various types,
 * currently supporting type: BOOLEAN, INT (TINYINT, SMALLINT, INT, BIGINT and LARGEINT),
 * DATETIME (DATE, DATETIME and TIME), STRING (CHAR, VARCHAR, BINARY, VARBINARY and HLL)
 */
public abstract class Variant implements Comparable<Variant> {

    @SerializedName(value = "type")
    protected final Type type;

    public Variant(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    public abstract String getStringValue();

    public abstract long getLongValue();

    public TVariant toThrift() {
        TVariant variant = new TVariant();
        variant.setType(TypeSerializer.toThrift(type));
        if (this instanceof NullVariant) {
            variant.setVariant_type(TVariantType.NULL_VALUE);
        } else if (this instanceof MinVariant) {
            variant.setVariant_type(TVariantType.MINIMUM);
        } else if (this instanceof MaxVariant) {
            variant.setVariant_type(TVariantType.MAXIMUM);
        } else {
            variant.setVariant_type(TVariantType.NORMAL_VALUE);
            variant.setValue(getStringValue());
        }
        return variant;
    }

    public VariantPB toProto() {
        VariantPB variant = new VariantPB();
        variant.type = TypeSerializer.toProtobuf(type);
        if (this instanceof NullVariant) {
            variant.variantType = VariantTypePB.NULL_VALUE;
        } else if (this instanceof MinVariant) {
            variant.variantType = VariantTypePB.MINIMUM;
        } else if (this instanceof MaxVariant) {
            variant.variantType = VariantTypePB.MAXIMUM;
        } else {
            variant.variantType = VariantTypePB.NORMAL_VALUE;
            variant.value = getStringValue();
        }
        return variant;
    }

    protected abstract int compareToImpl(Variant other);

    @Override
    public int compareTo(Variant other) {
        if (this instanceof MinVariant) {
            return (other instanceof MinVariant) ? 0 : -1;
        }
        if (other instanceof MinVariant) {
            return 1;
        }
        if (this instanceof MaxVariant) {
            return (other instanceof MaxVariant) ? 0 : 1;
        }
        if (other instanceof MaxVariant) {
            return -1;
        }
        if (this instanceof NullVariant) {
            return (other instanceof NullVariant) ? 0 : -1;
        }
        if (other instanceof NullVariant) {
            return 1;
        }
        return compareToImpl(other);
    }

    public static Variant of(Type type, String value) {
        Preconditions.checkArgument(type.isValid());
        switch (type.getPrimitiveType()) {
            case BOOLEAN:
                return new BoolVariant(value);
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return new IntVariant(type, value);
            case LARGEINT:
                return new LargeIntVariant(value);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case HLL:
                return new StringVariant(type, value);
            case DATE:
            case DATETIME:
            case TIME:
                return new DateVariant(type, value);
            default:
                throw new IllegalArgumentException("Type[" + type.toSql() + "] not supported.");
        }
    }

    public static Variant nullVariant(Type type) {
        Preconditions.checkArgument(type.isValid(), "Invalid type for Variant.nullVariant");
        return new NullVariant(type);
    }

    public static Variant minVariant(Type type) {
        Preconditions.checkArgument(type.isValid(), "Invalid type for Variant.minVariant");
        return new MinVariant(type);
    }

    public static Variant maxVariant(Type type) {
        Preconditions.checkArgument(type.isValid(), "Invalid type for Variant.maxVariant");
        return new MaxVariant(type);
    }

    public static Variant fromThrift(TVariant tVariant) {
        Type type = TypeDeserializer.fromThrift(tVariant.type);
        if (tVariant.isSetVariant_type()) {
            if (tVariant.getVariant_type() == TVariantType.NULL_VALUE) {
                return new NullVariant(type);
            } else if (tVariant.getVariant_type() == TVariantType.MINIMUM) {
                return new MinVariant(type);
            } else if (tVariant.getVariant_type() == TVariantType.MAXIMUM) {
                return new MaxVariant(type);
            }
        }
        return Variant.of(type, tVariant.getValue());
    }

    public static Variant fromProto(VariantPB variantPB) {
        Type type = TypeDeserializer.fromProtobuf(variantPB.type);
        if (variantPB.variantType != null) {
            if (variantPB.variantType == VariantTypePB.NULL_VALUE) {
                return new NullVariant(type);
            } else if (variantPB.variantType == VariantTypePB.MINIMUM) {
                return new MinVariant(type);
            } else if (variantPB.variantType == VariantTypePB.MAXIMUM) {
                return new MaxVariant(type);
            }
        }
        return Variant.of(type, variantPB.value);
    }

    public static int compatibleCompare(Variant key1, Variant key2) {
        if (key1.getClass() == key2.getClass()) {
            return key1.compareTo(key2);
        }

        // If any side is an infinity sentinel (MIN/MAX) or NULL, rely directly on Variant.compareTo().
        // The compareTo implementation already encodes global ordering for MinVariant/MaxVariant
        // across all underlying types, so we must NOT route them through Variant.of(...) again,
        // otherwise their string representation ("MIN"/"MAX") would be parsed as normal values.
        if (key1 instanceof NullVariant || key1 instanceof MinVariant || key1 instanceof MaxVariant
                || key2 instanceof NullVariant || key2 instanceof MinVariant || key2 instanceof MaxVariant) {
            return key1.compareTo(key2);
        }

        Type destType = TypeManager.getAssignmentCompatibleType(key1.getType(), key2.getType(), false);
        Preconditions.checkArgument(destType.isValid());
        Variant newKey1 = key1;
        if (key1.getType() != destType) {
            newKey1 = Variant.of(destType, key1.getStringValue());
        }
        Variant newKey2 = key2;
        if (key2.getType() != destType) {
            newKey2 = Variant.of(destType, key2.getStringValue());
        }
        return newKey1.compareTo(newKey2);
    }

    public static int compatibleCompare(List<Variant> key1, List<Variant> key2) {
        int key1Length = key1.size();
        int key2Length = key2.size();
        int minLength = Math.min(key1Length, key2Length);
        for (int i = 0; i < minLength; ++i) {
            int ret = Variant.compatibleCompare(key1.get(i), key2.get(i));
            if (0 != ret) {
                return ret;
            }
        }
        return Integer.compare(key1Length, key2Length);
    }
}
