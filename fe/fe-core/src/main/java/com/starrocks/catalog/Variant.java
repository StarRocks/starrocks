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
import com.starrocks.sql.common.TypeManager;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;

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

    public abstract String getStringValue();

    public abstract long getLongValue();

    public abstract TVariant toThrift();

    // public abstract VariantPB toProto();

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

    public static Variant fromThrift(TVariant tVariant) {
        return Variant.of(TypeDeserializer.fromThrift(tVariant.type), tVariant.getValue());
    }

    public static Variant fromProto(VariantPB variantPB) {
        return Variant.of(TypeDeserializer.fromProtobuf(variantPB.type), variantPB.value);
    }

    public static int compatibleCompare(Variant key1, Variant key2) {
        if (key1.getClass() == key2.getClass()) {
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
