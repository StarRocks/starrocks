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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;


public class ArrowTypeUtils {
    public static String fromArrowType(ArrowType arrowType) {
        switch (arrowType.getTypeID()) {
            case Int:
                Int intType = (Int) arrowType;
                if (intType.getBitWidth() == 32) {
                    return "int";
                } else if (intType.getBitWidth() == 64) {
                    return "bigint";
                }
                break;
            case FloatingPoint:
                return "double";
            case Bool:
                return "boolean";
            case Utf8:
                return "string";
            case Timestamp:
                return "timestamp";
            case Date:
                return "date";
            case Decimal:
                Decimal decimalType = (Decimal) arrowType;
                return String.format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            case Binary:
                return "binary";
            default:
                return "unknown";
        }
        return "unknown";
    }
}
