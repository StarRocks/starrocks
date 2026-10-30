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

package com.starrocks.fluss.reader;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeVisitor;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import java.util.stream.Collectors;

public class FlussTypeUtils {
    private FlussTypeUtils() {
    }

    public static String fromFlussType(DataType type) {
        return type.accept(FlussToHiveTypeVisitor.INSTANCE);
    }

    private static class FlussToHiveTypeVisitor implements DataTypeVisitor<String> {

        private static final FlussToHiveTypeVisitor INSTANCE = new FlussToHiveTypeVisitor();

        @Override
        public String visit(CharType charType) {
            return "string";
        }

        @Override
        public String visit(StringType stringType) {
            return "string";
        }

        @Override
        public String visit(BooleanType booleanType) {
            return "boolean";
        }

        @Override
        public String visit(BinaryType binaryType) {
            return "binary";
        }

        @Override
        public String visit(BytesType bytesType) {
            return "binary";
        }

        @Override
        public String visit(DecimalType decimalType) {
            return String.format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return "tinyint";
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return "short";
        }

        @Override
        public String visit(IntType intType) {
            return "int";
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return "bigint";
        }

        @Override
        public String visit(FloatType floatType) {
            return "float";
        }

        @Override
        public String visit(DoubleType doubleType) {
            return "double";
        }

        @Override
        public String visit(DateType dateType) {
            return "date";
        }

        @Override
        public String visit(TimeType timeType) {
            return "time";
        }

        @Override
        public String visit(TimestampType timestampType) {
            return "timestamp-millis";
        }

        @Override
        public String visit(LocalZonedTimestampType localZonedTimestampType) {
            return "timestamp-millis";
        }

        @Override
        public String visit(ArrayType arrayType) {
            return String.format("array<%s>", arrayType.getElementType().accept(this));
        }

        @Override
        public String visit(MapType mapType) {
            return String.format("map<%s,%s>",
                    mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this));
        }

        @Override
        public String visit(RowType rowType) {
            String type = rowType.getFields().stream()
                    .map(f -> f.getName() + ":" + f.getType().accept(this))
                    .collect(Collectors.joining(","));
            return String.format("struct<%s>", type);
        }
    }
}
