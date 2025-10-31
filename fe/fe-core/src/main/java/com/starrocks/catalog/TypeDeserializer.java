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
import com.starrocks.common.Pair;
import com.starrocks.proto.PScalarType;
import com.starrocks.proto.PTypeDesc;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScalarType;
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;

import java.util.ArrayList;

/**
 * Utility class for deserializing Type objects from Thrift and Protobuf formats.
 * This class centralizes all type deserialization logic.
 */
public class TypeDeserializer {

    /**
     * Deserializes a Type from its Thrift representation.
     *
     * @param thrift The TTypeDesc to deserialize
     * @return The deserialized Type
     */
    public static Type fromThrift(TTypeDesc thrift) {
        Preconditions.checkState(thrift.types.size() > 0);
        Pair<Type, Integer> t = fromThrift(thrift, 0);
        Preconditions.checkState(t.second.equals(thrift.getTypesSize()));
        return t.first;
    }

    public static PrimitiveType fromThrift(TPrimitiveType tPrimitiveType) {
        switch (tPrimitiveType) {
            case NULL_TYPE:
                return PrimitiveType.NULL_TYPE;
            case BOOLEAN:
                return PrimitiveType.BOOLEAN;
            case TINYINT:
                return PrimitiveType.TINYINT;
            case SMALLINT:
                return PrimitiveType.SMALLINT;
            case INT:
                return PrimitiveType.INT;
            case BIGINT:
                return PrimitiveType.BIGINT;
            case LARGEINT:
                return PrimitiveType.LARGEINT;
            case FLOAT:
                return PrimitiveType.FLOAT;
            case DOUBLE:
                return PrimitiveType.DOUBLE;
            case VARCHAR:
                return PrimitiveType.VARCHAR;
            case CHAR:
                return PrimitiveType.CHAR;
            case HLL:
                return PrimitiveType.HLL;
            case OBJECT:
                return PrimitiveType.BITMAP;
            case PERCENTILE:
                return PrimitiveType.PERCENTILE;
            case DECIMAL32:
                return PrimitiveType.DECIMAL32;
            case DECIMAL64:
                return PrimitiveType.DECIMAL64;
            case DECIMAL128:
                return PrimitiveType.DECIMAL128;
            case DECIMAL256:
                return PrimitiveType.DECIMAL256;
            case DATE:
                return PrimitiveType.DATE;
            case DATETIME:
                return PrimitiveType.DATETIME;
            case TIME:
                return PrimitiveType.TIME;
            case VARBINARY:
                return PrimitiveType.VARBINARY;
            case JSON:
                return PrimitiveType.JSON;
            case FUNCTION:
                return PrimitiveType.FUNCTION;
            case VARIANT:
                return PrimitiveType.VARIANT;
            case INVALID_TYPE:
            default:
                return PrimitiveType.INVALID_TYPE;
        }
    }

    /**
     * Constructs a Type rooted at the TTypeNode at nodeIdx in TTypeDesc.
     * Returned pair: The resulting Type and the next nodeIdx that is not a child
     * type of the result.
     */
    private static Pair<Type, Integer> fromThrift(TTypeDesc col, int nodeIdx) {
        TTypeNode node = col.getTypes().get(nodeIdx);
        Type type = null;
        int tmpNodeIdx = nodeIdx;
        switch (node.getType()) {
            case SCALAR: {
                type = scalarTypeFromThrift(node);
                ++tmpNodeIdx;
                break;
            }
            case ARRAY: {
                Preconditions.checkState(tmpNodeIdx + 1 < col.getTypesSize());
                Pair<Type, Integer> childType = fromThrift(col, tmpNodeIdx + 1);
                type = new ArrayType(childType.first);
                tmpNodeIdx = childType.second;
                break;
            }
            case MAP: {
                Preconditions.checkState(tmpNodeIdx + 2 < col.getTypesSize());
                Pair<Type, Integer> keyType = fromThrift(col, tmpNodeIdx + 1);
                Pair<Type, Integer> valueType = fromThrift(col, keyType.second);
                type = new MapType(keyType.first, valueType.first);
                tmpNodeIdx = valueType.second;
                break;
            }
            case STRUCT: {
                Preconditions.checkState(tmpNodeIdx + node.getStruct_fieldsSize() < col.getTypesSize());
                ArrayList<StructField> structFields = new ArrayList<>();
                ++tmpNodeIdx;
                for (int i = 0; i < node.getStruct_fieldsSize(); ++i) {
                    TStructField thriftField = node.getStruct_fields().get(i);
                    String name = thriftField.getName();
                    String comment = null;
                    if (thriftField.isSetComment()) {
                        comment = thriftField.getComment();
                    }
                    Pair<Type, Integer> res = fromThrift(col, tmpNodeIdx);
                    tmpNodeIdx = res.second;
                    structFields.add(new StructField(name, res.first, comment));
                }
                type = new StructType(structFields);
                break;
            }
        }
        return new Pair<Type, Integer>(type, tmpNodeIdx);
    }

    private static Type scalarTypeFromThrift(TTypeNode node) {
        Preconditions.checkState(node.isSetScalar_type());
        TScalarType scalarType = node.getScalar_type();
        
        if (scalarType.getType() == TPrimitiveType.CHAR) {
            Preconditions.checkState(scalarType.isSetLen());
            return ScalarType.createCharType(scalarType.getLen());
        } else if (scalarType.getType() == TPrimitiveType.VARCHAR) {
            Preconditions.checkState(scalarType.isSetLen());
            return ScalarType.createVarcharType(scalarType.getLen());
        } else if (scalarType.getType() == TPrimitiveType.VARBINARY) {
            return ScalarType.createVarbinary(scalarType.getLen());
        } else if (scalarType.getType() == TPrimitiveType.HLL) {
            return ScalarType.createHllType();
        } else if (scalarType.getType() == TPrimitiveType.DECIMAL) {
            Preconditions.checkState(scalarType.isSetPrecision() && scalarType.isSetScale());
            return ScalarType.createDecimalV2Type(scalarType.getPrecision(), scalarType.getScale());
        } else if (scalarType.getType() == TPrimitiveType.DECIMALV2) {
            Preconditions.checkState(scalarType.isSetPrecision() && scalarType.isSetScale());
            return ScalarType.createDecimalV2Type(scalarType.getPrecision(), scalarType.getScale());
        } else if (scalarType.getType() == TPrimitiveType.DECIMAL32 ||
                scalarType.getType() == TPrimitiveType.DECIMAL64 ||
                scalarType.getType() == TPrimitiveType.DECIMAL128 ||
                scalarType.getType() == TPrimitiveType.DECIMAL256) {
            Preconditions.checkState(scalarType.isSetPrecision() && scalarType.isSetScale());
            return ScalarType.createDecimalV3Type(
                    TypeDeserializer.fromThrift(scalarType.getType()),
                    scalarType.getPrecision(),
                    scalarType.getScale());
        } else {
            return ScalarType.createType(TypeDeserializer.fromThrift(scalarType.getType()));
        }
    }

    /**
     * Deserializes a Type from its Protobuf representation.
     *
     * @param pTypeDesc The PTypeDesc to deserialize
     * @return The deserialized Type
     */
    public static Type fromProtobuf(PTypeDesc pTypeDesc) {
        return fromProtobuf(pTypeDesc, 0).first;
    }

    private static Pair<Type, Integer> fromProtobuf(PTypeDesc pTypeDesc, int nodeIndex) {
        Preconditions.checkState(pTypeDesc.types.size() > nodeIndex);
        TTypeNodeType tTypeNodeType = TTypeNodeType.findByValue(pTypeDesc.types.get(nodeIndex).type);
        switch (tTypeNodeType) {
            case SCALAR: {
                PScalarType scalarType = pTypeDesc.types.get(nodeIndex).scalarType;
                return new Pair<>(createType(scalarType), 1);
            }
            case ARRAY: {
                Preconditions.checkState(pTypeDesc.types.size() > nodeIndex + 1);
                Pair<Type, Integer> res = fromProtobuf(pTypeDesc, nodeIndex + 1);
                return new Pair<>(new ArrayType(res.first), 1 + res.second);
            }
            case MAP: {
                Preconditions.checkState(pTypeDesc.types.size() > nodeIndex + 2);
                Pair<Type, Integer> keyRes = fromProtobuf(pTypeDesc, nodeIndex + 1);
                int keyStep = keyRes.second;

                Pair<Type, Integer> valueRes = fromProtobuf(pTypeDesc, nodeIndex + 1 + keyStep);
                int valueStep = valueRes.second;
                return new Pair<>(new MapType(keyRes.first, valueRes.first), 1 + keyStep + valueStep);
            }
            case STRUCT: {
                Preconditions.checkState(pTypeDesc.types.size() >=
                        nodeIndex + 1 + pTypeDesc.types.get(nodeIndex).structFields.size());
                ArrayList<StructField> fields = new ArrayList<>();

                int totalStep = 0;
                for (int i = 0; i < pTypeDesc.types.get(nodeIndex).structFields.size(); ++i) {
                    String fieldName = pTypeDesc.types.get(nodeIndex).structFields.get(i).name;
                    Pair<Type, Integer> res = fromProtobuf(pTypeDesc, nodeIndex + 1 + totalStep);
                    fields.add(new StructField(fieldName, res.first));
                    totalStep += res.second;
                }
                return new Pair<>(new StructType(fields), 1 + totalStep);
            }
        }
        // NEVER REACH.
        Preconditions.checkState(false);
        return null;
    }

    public static ScalarType createType(PScalarType ptype) {
        TPrimitiveType tPrimitiveType = TPrimitiveType.findByValue(ptype.type);

        switch (tPrimitiveType) {
            case CHAR:
                return ScalarType.createCharType(ptype.len);
            case VARCHAR:
                return ScalarType.createVarcharType(ptype.len);
            case VARBINARY:
                return ScalarType.createVarbinary(ptype.len);
            case DECIMALV2:
                return ScalarType.createDecimalV2Type(ptype.precision, ptype.precision);
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return ScalarType.createDecimalV3Type(TypeDeserializer.fromThrift(tPrimitiveType), ptype.precision, ptype.scale);
            default:
                return ScalarType.createType(TypeDeserializer.fromThrift(tPrimitiveType));
        }
    }
}
