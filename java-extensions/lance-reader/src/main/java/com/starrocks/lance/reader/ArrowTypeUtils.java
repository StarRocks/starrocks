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

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.stream.Collectors;

public class ArrowTypeUtils {
    private ArrowTypeUtils() {
    }

    public static String fromArrowField(Field field) {
        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Int:
                int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
                if (bitWidth == 8) {
                    return "tinyint";
                } else if (bitWidth == 16) {
                    return "smallint";
                } else if (bitWidth == 32) {
                    return "int";
                } else if (bitWidth == 64) {
                    return "bigint";
                }
                break;
            case FloatingPoint:
                return ((ArrowType.FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.SINGLE
                        ? "float" : "double";
            case Bool:
                return "boolean";
            case Utf8:
            case LargeUtf8:
                return "string";
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return "binary";
            case Date:
                return "date";
            case Timestamp:
                return "timestamp";
            case Decimal:
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                return String.format("decimal(%d,%d)", decimal.getPrecision(), decimal.getScale());
            case List:
            case LargeList:
            case FixedSizeList:
                return "array<" + fromArrowField(field.getChildren().get(0)) + ">";
            case Map:
                return fromArrowMap(field);
            case Struct:
                return "struct<" + field.getChildren().stream()
                        .map(child -> child.getName() + ":" + fromArrowField(child))
                        .collect(Collectors.joining(",")) + ">";
            default:
                break;
        }
        throw new UnsupportedOperationException(
                "Unsupported lance/arrow type: " + arrowType + " for column " + field.getName());
    }

    private static String fromArrowMap(Field field) {
        List<Field> children = field.getChildren();
        if (children.isEmpty() || children.get(0).getChildren().size() < 2) {
            throw new UnsupportedOperationException("Invalid map field: " + field.getName());
        }
        List<Field> keyValue = children.get(0).getChildren();
        return "map<" + fromArrowField(keyValue.get(0)) + "," + fromArrowField(keyValue.get(1)) + ">";
    }
}
