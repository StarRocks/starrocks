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

package com.starrocks.connector.adbc;

import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Concrete Arrow-to-StarRocks type mapper for the FlightSQL ADBC driver.
 * Implements the inverse of {@code ArrowUtils.convertToScalarArrowType()} —
 * maps Arrow types received from a remote FlightSQL server back to StarRocks types.
 *
 * <p>Unsigned integer types are promoted to the next-wider signed type to prevent overflow:
 * uint8->SMALLINT, uint16->INT, uint32->BIGINT, uint64->LARGEINT.
 *
 * <p>Complex types (List, Struct, Map, Union) are not supported and return null,
 * causing the column to be excluded from the schema by {@link ADBCSchemaResolver#convertToSRTable}.
 */
public class FlightSQLSchemaResolver extends ADBCSchemaResolver {

    private static final Logger LOG = LogManager.getLogger(FlightSQLSchemaResolver.class);

    @Override
    public Type convertArrowFieldToSRType(Field field) {
        ArrowType arrowType = field.getType();

        if (arrowType instanceof ArrowType.Bool) {
            return BooleanType.BOOLEAN;
        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            return convertIntType(intType);
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
            return convertFloatingPointType(fpType);
        } else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
            return new DecimalType(PrimitiveType.DECIMAL128, decimalType.getPrecision(), decimalType.getScale());
        } else if (arrowType instanceof ArrowType.Utf8) {
            return new VarcharType(StringType.DEFAULT_STRING_LENGTH);
        } else if (arrowType instanceof ArrowType.LargeUtf8) {
            return new VarcharType(StringType.DEFAULT_STRING_LENGTH);
        } else if (arrowType instanceof ArrowType.Utf8View) {
            // TODO: add BE arrow converter support for STRING_VIEW, then map to VarcharType here
            LOG.warn("Unsupported Arrow type: Utf8View for column '{}'. "
                    + "Column will be excluded from schema.", field.getName());
            return null;
        } else if (arrowType instanceof ArrowType.Binary) {
            return VarbinaryType.VARBINARY;
        } else if (arrowType instanceof ArrowType.LargeBinary) {
            return VarbinaryType.VARBINARY;
        } else if (arrowType instanceof ArrowType.BinaryView) {
            // TODO: add BE arrow converter support for BINARY_VIEW, then map to VarbinaryType here
            LOG.warn("Unsupported Arrow type: BinaryView for column '{}'. "
                    + "Column will be excluded from schema.", field.getName());
            return null;
        } else if (arrowType instanceof ArrowType.Date) {
            return DateType.DATE;
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return DateType.DATETIME;
        } else if (arrowType instanceof ArrowType.Null) {
            return NullType.NULL;
        } else {
            LOG.warn("Unsupported Arrow type: {}", arrowType.getClass().getSimpleName());
            return null;
        }
    }

    private Type convertIntType(ArrowType.Int intType) {
        int bitWidth = intType.getBitWidth();
        boolean signed = intType.getIsSigned();

        if (signed) {
            switch (bitWidth) {
                case 8:
                    return IntegerType.TINYINT;
                case 16:
                    return IntegerType.SMALLINT;
                case 32:
                    return IntegerType.INT;
                case 64:
                    return IntegerType.BIGINT;
                default:
                    LOG.warn("Unsupported signed int bit width: {}", bitWidth);
                    return null;
            }
        } else {
            // Unsigned types promoted to next-wider signed type
            switch (bitWidth) {
                case 8:
                    return IntegerType.SMALLINT;
                case 16:
                    return IntegerType.INT;
                case 32:
                    return IntegerType.BIGINT;
                case 64:
                    return IntegerType.LARGEINT;
                default:
                    LOG.warn("Unsupported unsigned int bit width: {}", bitWidth);
                    return null;
            }
        }
    }

    private Type convertFloatingPointType(ArrowType.FloatingPoint fpType) {
        if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
            return FloatType.FLOAT;
        } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
            return FloatType.DOUBLE;
        } else {
            LOG.warn("Unsupported floating point precision: {}", fpType.getPrecision());
            return null;
        }
    }
}
