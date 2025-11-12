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

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;

/**
 * Factory class for creating Type instances.
 * This class centralizes all type creation logic that was previously scattered
 * across Type and ScalarType classes, following the Factory pattern.
 */
public class TypeFactory {

    private TypeFactory() {
        // Private constructor to prevent instantiation
    }

    /**
     * Create a scalar type with specified parameters.
     * 
     * @param type the primitive type
     * @param len length for CHAR/VARCHAR types
     * @param precision precision for DECIMAL types
     * @param scale scale for DECIMAL types
     * @return the created ScalarType
     */
    public static ScalarType createType(PrimitiveType type, int len, int precision, int scale) {
        switch (type) {
            case CHAR:
                return createCharType(len);
            case VARCHAR:
                return createVarcharType(len);
            case VARBINARY:
                return createVarbinary(len);
            case DECIMALV2:
                return createDecimalV2Type(precision, scale);
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return createDecimalV3Type(type, precision, scale);
            default:
                return createType(type);
        }
    }

    /**
     * Create a scalar type from a primitive type.
     * 
     * @param type the primitive type
     * @return the created ScalarType
     */
    public static ScalarType createType(PrimitiveType type) {
        ScalarType res = Type.PRIMITIVE_TYPE_SCALAR_TYPE_MAP.get(type);
        Preconditions.checkNotNull(res, "unknown type " + type);
        return res;
    }

    /**
     * Create a scalar type from a string type name.
     * 
     * @param type the type name
     * @return the created ScalarType
     */
    public static ScalarType createType(String type) {
        ScalarType res = Type.STATIC_TYPE_MAP.get(type);
        Preconditions.checkNotNull(res, "unknown type " + type);
        return res;
    }

    /**
     * Create a CHAR type with specified length.
     * 
     * @param len the length of the CHAR type
     * @return the created CHAR type
     */
    public static ScalarType createCharType(int len) {
        ScalarType type = new ScalarType(PrimitiveType.CHAR);
        type.setLength(len);
        return type;
    }

    private static boolean isDecimalV3Enabled() {
        return Config.enable_decimal_v3;
    }

    /**
     * Create a unified decimal type with default precision and scale.
     * Unified decimal is used for parser, which creating default decimal from name.
     * For mysql compatibility, uses precision=10, scale=0 as default.
     * 
     * @return the created decimal type
     */
    public static ScalarType createUnifiedDecimalType() {
        // for mysql compatibility
        return createUnifiedDecimalType(10, 0);
    }

    /**
     * Create a unified decimal type with specified precision.
     * For mysql compatibility, uses scale=0 as default.
     * 
     * @param precision the precision
     * @return the created decimal type
     */
    public static ScalarType createUnifiedDecimalType(int precision) {
        // for mysql compatibility
        return createUnifiedDecimalType(precision, 0);
    }

    /**
     * Create a unified decimal type with specified precision and scale.
     * Automatically chooses between DecimalV2 and DecimalV3 based on configuration.
     * 
     * @param precision the precision
     * @param scale the scale
     * @return the created decimal type
     */
    public static ScalarType createUnifiedDecimalType(int precision, int scale) {
        if (isDecimalV3Enabled()) {
            // use decimal64 even if precision <= 9, because decimal32 is vulnerable to overflow
            // and casted to decimal64 before expression evaluations are performed on it in BE, so
            // decimal32 has a performance penalty.
            PrimitiveType pt;
            if (precision <= 18) {
                pt = PrimitiveType.DECIMAL64;
            } else if (precision <= 38) {
                pt = PrimitiveType.DECIMAL128;
            } else {
                pt = PrimitiveType.DECIMAL256;
            }

            return createDecimalV3Type(pt, precision, scale);
        } else {
            return createDecimalV2Type(precision, scale);
        }
    }

    /**
     * Create a DecimalV2 type with default precision and scale.
     * 
     * @return the default DecimalV2 type
     */
    public static ScalarType createDecimalV2Type() {
        return Type.DEFAULT_DECIMALV2;
    }

    /**
     * Create a DecimalV2 type with specified precision and default scale.
     * 
     * @param precision the precision
     * @return the created DecimalV2 type
     */
    public static ScalarType createDecimalV2Type(int precision) {
        return createDecimalV2Type(precision, ScalarType.DEFAULT_SCALE);
    }

    /**
     * Create a DecimalV2 type with specified precision and scale.
     * 
     * @param precision the precision
     * @param scale the scale
     * @return the created DecimalV2 type
     */
    public static ScalarType createDecimalV2Type(int precision, int scale) {
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.setPrecision(precision);
        type.setScale(scale);
        return type;
    }

    /**
     * Create a DecimalV3 type with specified primitive type, precision and scale.
     * 
     * @param type the decimal primitive type (DECIMAL32/64/128/256)
     * @param precision the precision
     * @param scale the scale
     * @return the created DecimalV3 type
     */
    public static ScalarType createDecimalV3Type(PrimitiveType type, int precision, int scale) {
        int maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL256);
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null ||
                ctx.getSessionVariable() == null ||
                ctx.getSessionVariable().getLargeDecimalUnderlyingType() == null ||
                ctx.getSessionVariable().getLargeDecimalUnderlyingType().equals(SessionVariableConstants.PANIC)) {
            Preconditions.checkArgument(0 <= precision &&
                            precision <= PrimitiveType.getMaxPrecisionOfDecimal(type),
                    "DECIMAL's precision should range from 1 to %s",
                    PrimitiveType.getMaxPrecisionOfDecimal(type));
            Preconditions.checkArgument(0 <= scale && scale <= precision,
                    "DECIMAL(P[,S]) type P must be greater than or equal to the value of S");
        }
        if (precision > maxPrecision) {
            String underlyingType = ConnectContext.get().getSessionVariable().getLargeDecimalUnderlyingType();
            if (underlyingType.equals(SessionVariableConstants.DOUBLE)) {
                return Type.DOUBLE;
            } else {
                precision = maxPrecision;
                type = PrimitiveType.DECIMAL256;
            }
        }
        ScalarType scalarType = new ScalarType(type);
        scalarType.setPrecision(Math.max(precision, 1));
        scalarType.setScale(scale);
        return scalarType;
    }

    /**
     * Create a DecimalV3 type for representing zero with specified scale.
     * The precision is set equal to the scale.
     * 
     * @param scale the scale
     * @return the created DecimalV3 type
     */
    public static ScalarType createDecimalV3TypeForZero(int scale) {
        final ScalarType scalarType;
        if (scale <= PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32)) {
            scalarType = new ScalarType(PrimitiveType.DECIMAL32);
        } else if (scale <= PrimitiveType.getDefaultScaleOfDecimal(PrimitiveType.DECIMAL64)) {
            scalarType = new ScalarType(PrimitiveType.DECIMAL64);
        } else if (scale <= PrimitiveType.getDefaultScaleOfDecimal(PrimitiveType.DECIMAL128)) {
            scalarType = new ScalarType(PrimitiveType.DECIMAL128);
        } else {
            scalarType = new ScalarType(PrimitiveType.DECIMAL256);
            scale = PrimitiveType.getDefaultScaleOfDecimal(PrimitiveType.DECIMAL256);
        }
        scalarType.setPrecision(scale);
        scalarType.setScale(scale);
        return scalarType;
    }

    /**
     * Create a wildcard DecimalV3 type for function matching.
     * Wildcard decimal uses precision=-1 and scale=-1.
     * 
     * @param type the decimal primitive type (DECIMAL32/64/128/256)
     * @return the created wildcard DecimalV3 type
     */
    public static ScalarType createWildcardDecimalV3Type(PrimitiveType type) {
        Preconditions.checkArgument(type.isDecimalV3Type());
        ScalarType scalarType = new ScalarType(type);
        scalarType.setPrecision(-1);
        scalarType.setScale(-1);
        return scalarType;
    }

    /**
     * Create a DecimalV3 type with specified primitive type and precision.
     * Scale is set to the minimum of precision and the default scale for the type.
     * 
     * @param type the decimal primitive type (DECIMAL32/64/128/256)
     * @param precision the precision
     * @return the created DecimalV3 type
     */
    public static ScalarType createDecimalV3Type(PrimitiveType type, int precision) {
        int defaultScale = PrimitiveType.getDefaultScaleOfDecimal(type);
        return createDecimalV3Type(type, precision, Math.min(precision, defaultScale));
    }

    /**
     * Create a DecimalV3 type with specified primitive type.
     * Uses the maximum precision and default scale for the type.
     * 
     * @param type the decimal primitive type (DECIMAL32/64/128/256)
     * @return the created DecimalV3 type
     */
    public static ScalarType createDecimalV3Type(PrimitiveType type) {
        int maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(type);
        int defaultScale = PrimitiveType.getDefaultScaleOfDecimal(type);
        return createDecimalV3Type(type, maxPrecision, defaultScale);
    }

    /**
     * Create the narrowest DecimalV3 type that can hold the specified precision and scale.
     * Chooses the smallest DecimalV3 type (DECIMAL32 -> DECIMAL64 -> DECIMAL128 -> DECIMAL256)
     * that can accommodate the given precision.
     * 
     * @param precision the precision
     * @param scale the scale
     * @return the created narrowest DecimalV3 type
     */
    public static ScalarType createDecimalV3NarrowestType(int precision, int scale) {
        if (precision == 0 && scale == 0) {
            return createDecimalV3TypeForZero(0);
        }
        final int decimal32MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32);
        final int decimal64MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL64);
        final int decimal128MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
        final int decimal256MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL256);
        if (0 < precision && precision <= decimal32MaxPrecision) {
            return createDecimalV3Type(PrimitiveType.DECIMAL32, precision, scale);
        } else if (decimal32MaxPrecision < precision && precision <= decimal64MaxPrecision) {
            return createDecimalV3Type(PrimitiveType.DECIMAL64, precision, scale);
        } else if (decimal64MaxPrecision < precision && precision <= decimal128MaxPrecision) {
            return createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale);
        } else if (decimal128MaxPrecision < precision && precision <= decimal256MaxPrecision) {
            return createDecimalV3Type(PrimitiveType.DECIMAL256, precision, scale);
        } else {
            Preconditions.checkState(false,
                    "Illegal decimal precision(1 to 76): precision=" + precision);
            return Type.INVALID;
        }
    }

    /**
     * Get the maximum varchar length for OLAP tables.
     * 
     * @return the maximum varchar length
     */
    public static int getOlapMaxVarcharLength() {
        return Config.max_varchar_length;
    }

    /**
     * Create a default string type.
     * Uses default string length.
     * 
     * @return the created string type
     */
    public static ScalarType createDefaultString() {
        ScalarType stringType = createVarcharType(ScalarType.DEFAULT_STRING_LENGTH);
        return stringType;
    }

    /**
     * Create a default catalog string type.
     * Uses maximum catalog varchar length for external catalogs like Hive.
     * 
     * @return the created catalog string type
     */
    public static ScalarType createDefaultCatalogString() {
        return createVarcharType(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);
    }

    /**
     * Create a VARCHAR type with maximum OLAP varchar length.
     * 
     * @return the created VARCHAR type
     */
    public static ScalarType createOlapMaxVarcharType() {
        ScalarType stringType = createVarcharType(getOlapMaxVarcharLength());
        return stringType;
    }

    /**
     * Create a VARCHAR type with specified length.
     * 
     * @param len the length of the VARCHAR type
     * @return the created VARCHAR type
     */
    public static ScalarType createVarcharType(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.setLength(len);
        return type;
    }

    /**
     * Create a VARCHAR type with specified length.
     * Alias for createVarcharType(int).
     * 
     * @param len the length of the VARCHAR type
     * @return the created VARCHAR type
     */
    public static ScalarType createVarchar(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.setLength(len);
        return type;
    }

    /**
     * Create a VARBINARY type with specified length.
     * 
     * @param len the length of the VARBINARY type
     * @return the created VARBINARY type
     */
    public static ScalarType createVarbinary(int len) {
        ScalarType type = new ScalarType(PrimitiveType.VARBINARY);
        type.setLength(len);
        return type;
    }

    /**
     * Create a VARCHAR type with wildcard length.
     * 
     * @return the VARCHAR type with wildcard length
     */
    public static ScalarType createVarcharType() {
        return Type.VARCHAR;
    }

    /**
     * Create an HLL type.
     * HLL is used for HyperLogLog approximate counting.
     * 
     * @return the created HLL type
     */
    public static ScalarType createHllType() {
        ScalarType type = new ScalarType(PrimitiveType.HLL);
        type.setLength(ScalarType.MAX_HLL_LENGTH);
        return type;
    }

    /**
     * Create an UNKNOWN type.
     * Used for unresolved type references.
     * 
     * @return the created UNKNOWN type
     */
    public static ScalarType createUnknownType() {
        return new ScalarType(PrimitiveType.UNKNOWN_TYPE);
    }

    /**
     * Create a JSON type.
     * 
     * @return the created JSON type
     */
    public static ScalarType createJsonType() {
        return new ScalarType(PrimitiveType.JSON);
    }
}
