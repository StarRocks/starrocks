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
import com.google.common.collect.Lists;
import com.starrocks.thrift.TScalarType;
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;

/**
 * Utility class for serializing Type objects to Thrift format.
 * This class centralizes all type serialization logic.
 * For deserialization, see {@link TypeDeserializer}.
 */
public class TypeSerializer {

    /**
     * Converts a Type to its Thrift representation.
     *
     * @param type      The Type to serialize
     * @param container The TTypeDesc container to populate
     */
    public static void toThrift(Type type, TTypeDesc container) {
        if (type instanceof ScalarType) {
            scalarTypeToThrift((ScalarType) type, container);
        } else if (type instanceof ArrayType) {
            arrayTypeToThrift((ArrayType) type, container);
        } else if (type instanceof MapType) {
            mapTypeToThrift((MapType) type, container);
        } else if (type instanceof StructType) {
            structTypeToThrift((StructType) type, container);
        } else if (type instanceof PseudoType) {
            pseudoTypeToThrift((PseudoType) type, container);
        } else {
            throw new IllegalArgumentException("Unknown type: " + type.getClass().getName());
        }
    }

    private static void scalarTypeToThrift(ScalarType type, TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        PrimitiveType primitiveType = type.getPrimitiveType();

        switch (primitiveType) {
            case CHAR:
            case VARCHAR:
            case VARBINARY:
            case HLL: {
                node.setType(TTypeNodeType.SCALAR);
                TScalarType scalarType = new TScalarType();
                scalarType.setType(primitiveType.toThrift());
                scalarType.setLen(type.getLength());
                node.setScalar_type(scalarType);
                break;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256: {
                node.setType(TTypeNodeType.SCALAR);
                TScalarType scalarType = new TScalarType();
                scalarType.setType(primitiveType.toThrift());
                scalarType.setScale(type.getScalarScale());
                scalarType.setPrecision(type.getScalarPrecision());
                node.setScalar_type(scalarType);
                break;
            }
            default: {
                node.setType(TTypeNodeType.SCALAR);
                TScalarType scalarType = new TScalarType();
                scalarType.setType(primitiveType.toThrift());
                node.setScalar_type(scalarType);
                break;
            }
        }
    }

    private static void arrayTypeToThrift(ArrayType type, TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(type.getItemType());
        node.setType(TTypeNodeType.ARRAY);
        toThrift(type.getItemType(), container);
    }

    private static void mapTypeToThrift(MapType type, TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        node.setType(TTypeNodeType.MAP);
        toThrift(type.getKeyType(), container);
        toThrift(type.getValueType(), container);
    }

    private static void structTypeToThrift(StructType type, TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(type.getFields());
        Preconditions.checkState(!type.getFields().isEmpty(),
                "StructType must contains at least one StructField.");
        node.setType(TTypeNodeType.STRUCT);
        node.setStruct_fields(Lists.newArrayList());
        node.setIs_named(type.isNamed());
        for (StructField field : type.getFields()) {
            structFieldToThrift(field, container, node);
        }
    }

    private static void structFieldToThrift(StructField field, TTypeDesc container, TTypeNode node) {
        TStructField tfield = new TStructField();
        tfield.setName(field.getName());
        tfield.setComment(field.getComment());
        tfield.setId(field.getFieldId());
        tfield.setPhysical_name(field.getFieldPhysicalName());
        node.struct_fields.add(tfield);
        toThrift(field.getType(), container);
    }

    private static void pseudoTypeToThrift(PseudoType type, TTypeDesc container) {
        Preconditions.checkArgument(false, "PseudoType should not exposed to external");
    }
}
