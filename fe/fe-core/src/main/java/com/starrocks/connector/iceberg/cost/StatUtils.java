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


package com.starrocks.connector.iceberg.cost;



import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.util.Optional;

public class StatUtils {
    private StatUtils() {}

    public static Optional<Double> convertObjectToOptionalDouble(Type.PrimitiveType type, Object value) {
        double valueConvert = 0;
        if (type instanceof Types.BooleanType) {
            valueConvert = (boolean) value ? 1 : 0;
        } else if (type instanceof Types.IntegerType) {
            valueConvert = (int) value;
        } else if (type instanceof Types.LongType) {
            valueConvert = (long) value;
        } else if (type instanceof Types.FloatType) {
            valueConvert = (float) value;
        } else if (type instanceof Types.DoubleType) {
            valueConvert = (double) value;
        } else if (type instanceof Types.TimestampType) {
            // we deal iceberg TimestampType as seconds in columnstatistics
            // in iceberg it's microsecond
            valueConvert = ((long) value) / 1000000;
        } else if (type instanceof Types.DateType) {
            // we deal iceberg DateType as seconds in columnstatistics
            // in iceberg it's num of day from 1970-01-01
            valueConvert = ((long) ((int) value)) * 86400;
        } else if (type instanceof Types.DecimalType) {
            valueConvert = ((BigDecimal) value).doubleValue();
        } else {
            return Optional.empty();
        }

        return Optional.of(valueConvert);
    }
}
