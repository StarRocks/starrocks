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

package com.starrocks.type;

import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.AggStateDescPB;
import com.starrocks.persist.proto.ArrayTypePB;
import com.starrocks.persist.proto.MapTypePB;
import com.starrocks.persist.proto.ScalarTypePB;
import com.starrocks.persist.proto.StructFieldPB;
import com.starrocks.persist.proto.StructTypePB;
import com.starrocks.persist.proto.TypePB;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts the fe-type classes to and from their image v3 messages.
 *
 * <p>The classes themselves live in fe-type, which cannot see the generated proto classes, so the conversion sits
 * here in fe-core. It shares the package with {@link TypeFactory} so that it can use the package-private
 * {@link ScalarType} setters and rebuild a scalar type field by field.
 */
public final class TypeProtoConverter {
    private TypeProtoConverter() {
    }

    public static TypePB toProto(Type type) {
        TypePB.Builder builder = TypePB.newBuilder();
        if (type instanceof ScalarType scalarType) {
            // Covers the ScalarType subclasses (IntegerType, DecimalType, ...) as well: they persist the same four
            // fields and, like GSON, come back as a plain ScalarType.
            builder.setKind(TypePB.Kind.SCALAR).setScalar(toScalarProto(scalarType));
        } else if (type instanceof ArrayType arrayType) {
            ArrayTypePB.Builder array = ArrayTypePB.newBuilder();
            if (arrayType.getItemType() != null) {
                array.setItemType(toProto(arrayType.getItemType()));
            }
            builder.setKind(TypePB.Kind.ARRAY).setArray(array);
        } else if (type instanceof MapType mapType) {
            MapTypePB.Builder map = MapTypePB.newBuilder();
            if (mapType.getKeyType() != null) {
                map.setKeyType(toProto(mapType.getKeyType()));
            }
            if (mapType.getValueType() != null) {
                map.setValueType(toProto(mapType.getValueType()));
            }
            builder.setKind(TypePB.Kind.MAP).setMap(map);
        } else if (type instanceof StructType structType) {
            StructTypePB.Builder struct = StructTypePB.newBuilder().setNamed(structType.isNamed());
            for (StructField field : structType.getFields()) {
                struct.addFields(toProto(field));
            }
            builder.setKind(TypePB.Kind.STRUCT).setStruct(struct);
        } else if (type instanceof AnyElementType) {
            builder.setKind(TypePB.Kind.ANY_ELEMENT);
        } else if (type instanceof AnyArrayType) {
            builder.setKind(TypePB.Kind.ANY_ARRAY);
        } else if (type.getClass() == PseudoType.class) {
            builder.setKind(TypePB.Kind.PSEUDO);
        } else {
            throw new IllegalArgumentException("type class " + type.getClass().getName() + " cannot be persisted");
        }
        return builder.build();
    }

    public static Type fromProto(TypePB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        switch (pb.getKind()) {
            case SCALAR:
                return fromScalarProto(pb.getScalar());
            case ARRAY:
                return new ArrayType(pb.getArray().hasItemType() ? fromProto(pb.getArray().getItemType()) : null);
            case MAP:
                return new MapType(pb.getMap().hasKeyType() ? fromProto(pb.getMap().getKeyType()) : null,
                        pb.getMap().hasValueType() ? fromProto(pb.getMap().getValueType()) : null);
            case STRUCT: {
                List<StructField> fields = new ArrayList<>(pb.getStruct().getFieldsCount());
                for (StructFieldPB field : pb.getStruct().getFieldsList()) {
                    fields.add(fromProto(field));
                }
                return new StructType(fields, pb.getStruct().getNamed());
            }
            case PSEUDO:
                return new PseudoType();
            case ANY_ELEMENT:
                return new AnyElementType();
            case ANY_ARRAY:
                return new AnyArrayType();
            default:
                throw new IllegalArgumentException("unknown type kind " + pb.getKind());
        }
    }

    public static AggStateDescPB toProto(AggStateDesc desc) {
        AggStateDescPB.Builder builder = AggStateDescPB.newBuilder()
                .setFunctionName(desc.getFunctionName())
                .setReturnType(toProto(desc.getReturnType()))
                .setResultNullable(desc.getResultNullable());
        for (Type argType : desc.getArgTypes()) {
            builder.addArgTypes(toProto(argType));
        }
        return builder.build();
    }

    public static AggStateDesc fromProto(AggStateDescPB pb) {
        List<Type> argTypes = new ArrayList<>(pb.getArgTypesCount());
        for (TypePB argType : pb.getArgTypesList()) {
            argTypes.add(fromProto(argType));
        }
        return new AggStateDesc(pb.getFunctionName(), fromProto(pb.getReturnType()), argTypes, pb.getResultNullable());
    }

    private static ScalarTypePB toScalarProto(ScalarType type) {
        return ScalarTypePB.newBuilder()
                .setType(ProtoEnums.toProto(type.getPrimitiveType(), ScalarTypePB.PrimitiveType.class))
                .setLen(type.getLength())
                .setPrecision(type.getScalarPrecision())
                .setScale(type.getScalarScale())
                .build();
    }

    private static ScalarType fromScalarProto(ScalarTypePB pb) {
        ScalarType type = new ScalarType(ProtoEnums.fromProto(pb.getType(), PrimitiveType.class));
        type.setLength(pb.getLen());
        type.setPrecision(pb.getPrecision());
        type.setScale(pb.getScale());
        return type;
    }

    private static StructFieldPB toProto(StructField field) {
        StructFieldPB.Builder builder = StructFieldPB.newBuilder()
                .setName(field.getName())
                .setFieldId(field.getFieldId());
        if (field.getType() != null) {
            builder.setType(toProto(field.getType()));
        }
        if (field.getComment() != null) {
            builder.setComment(field.getComment());
        }
        if (field.getFieldPhysicalName() != null) {
            builder.setFieldPhysicalName(field.getFieldPhysicalName());
        }
        return builder.build();
    }

    private static StructField fromProto(StructFieldPB pb) {
        return new StructField(pb.getName(), pb.getFieldId(),
                pb.hasFieldPhysicalName() ? pb.getFieldPhysicalName() : null,
                pb.hasType() ? fromProto(pb.getType()) : null,
                pb.hasComment() ? pb.getComment() : null);
    }
}
