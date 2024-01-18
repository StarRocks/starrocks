// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ScalarType.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.thrift.TColumnType;
import com.starrocks.thrift.TScalarType;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;

import java.util.Objects;

/**
 * Describes a scalar type. For most types this class just wraps a PrimitiveType enum,
 * but for types like CHAR and DECIMAL, this class contain additional information.
 * <p>
 * Scalar types have a few ways they can be compared to other scalar types. They can be:
 * 1. completely identical,
 * 2. implicitly castable (convertible without loss of precision)
 * 3. subtype. For example, in the case of decimal, a type can be decimal(*, *)
 * indicating that any decimal type is a subtype of the decimal type.
 */
public class ScalarType extends Type implements Cloneable {

    // SQL allows the engine to pick the default precision. We pick the largest
    // precision that is supported by the smallest decimal type in the BE (4 bytes).
    public static final int DEFAULT_PRECISION = 9;
    public static final int DEFAULT_SCALE = 0; // SQL standard
    // Longest supported VARCHAR and CHAR, chosen to match Hive.
    public static final int DEFAULT_STRING_LENGTH = 65533;
    public static final int MAX_VARCHAR_LENGTH = 1048576;
    public static final int MAX_CHAR_LENGTH = 255;
    // HLL DEFAULT LENGTH  2^14(registers) + 1(type)
    public static final int MAX_HLL_LENGTH = 16385;
    @SerializedName(value = "type")
    private final PrimitiveType type;

    // Only used for type CHAR.
    @SerializedName(value = "len")
    private int len = -1;

    // Only used if type is DECIMAL. -1 (for both) is used to represent a
    // decimal with any precision and scale.
    // It is invalid to have one by -1 and not the other.
    // TODO: we could use that to store DECIMAL(8,*), indicating a decimal
    // with 8 digits of precision and any valid ([0-8]) scale.
    @SerializedName(value = "precision")
    private int precision;
    @SerializedName(value = "scale")
    private int scale;

    protected ScalarType(PrimitiveType type) {
        this.type = type;
    }

    public ScalarType() {
        this.type = PrimitiveType.INVALID_TYPE;
    }

    public static ScalarType createType(PrimitiveType type, int len, int precision, int scale) {
        switch (type) {
            case CHAR:
                return createCharType(len);
            case VARCHAR:
                return createVarcharType(len);
            case DECIMALV2:
                return createDecimalV2Type(precision, scale);
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return createDecimalV3Type(type, precision, scale);
            default:
                return createType(type);
        }
    }

    public static ScalarType createType(PrimitiveType type) {
        ScalarType res = PRIMITIVE_TYPE_SCALAR_TYPE_MAP.get(type);
        Preconditions.checkNotNull(res, "unknown type " + type);
        return res;
    }

    public static ScalarType createType(String type) {
        ScalarType res = STATIC_TYPE_MAP.get(type);
        Preconditions.checkNotNull(res, "unknown type " + type);
        return res;
    }

    public static ScalarType createCharType(int len) {
        ScalarType type = new ScalarType(PrimitiveType.CHAR);
        type.len = len;
        return type;
    }

    private static boolean isDecimalV3Enabled() {
        return Config.enable_decimal_v3;
    }

    public static void checkEnableDecimalV3() throws AnalysisException {
        if (!isDecimalV3Enabled()) {
            throw new AnalysisException("Config field enable_decimal_v3 is false now, " +
                    "turn it on before decimal32/64/128 are used, " +
                    "execute cmd 'admin set frontend config (\"enable_decimal_v3\" = \"true\")' " +
                    "on every FE server");
        }
    }

    private static AnalysisException decimalParseError(String errorMsg) {
        final String hint =
                "Legal format is 'DECIMAL(precision, scale)', " +
                        "constraints 0<precision<=38 and 0<=scale<=precision must be hold. ";
        return new AnalysisException(hint + "error='" + errorMsg + "'");
    }

    /**
     * Unified decimal is used for parser, which creating default decimal from name
     */
    public static ScalarType createUnifiedDecimalType() throws AnalysisException {
        throw decimalParseError("both precision and scale are absent");
    }

    public static ScalarType createUnifiedDecimalType(int precision) throws AnalysisException {
        throw decimalParseError("scale is absent");
    }

    public static ScalarType createUnifiedDecimalType(int precision, int scale) {
        if (isDecimalV3Enabled()) {
            // use decimal64 even if precision <= 9, because decimal32 is vulnerable to overflow
            // and casted to decimal64 before expression evaluations are performed on it in BE, so
            // decimal32 has a performance penalty.
            PrimitiveType pt = precision <= 18 ? PrimitiveType.DECIMAL64 : PrimitiveType.DECIMAL128;
            return createDecimalV3Type(pt, precision, scale);
        } else {
            return createDecimalV2Type(precision, scale);
        }
    }

    public static ScalarType createDecimalV2Type() {
        return DEFAULT_DECIMALV2;
    }

    public static ScalarType createDecimalV2Type(int precision) {
        return createDecimalV2Type(precision, DEFAULT_SCALE);
    }

    public static ScalarType createDecimalV2Type(int precision, int scale) {
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public static ScalarType createDecimalV3Type(PrimitiveType type, int precision, int scale) {
        Preconditions.checkArgument(0 <= precision && precision <= PrimitiveType.getMaxPrecisionOfDecimal(type),
                "DECIMAL's precision should range from 1 to 38");
        Preconditions.checkArgument(0 <= scale && scale <= precision,
                "DECIMAL(P[,S]) type P must be greater than or equal to the value of S");

        ScalarType scalarType = new ScalarType(type);
        scalarType.precision = Math.max(precision, 1);
        scalarType.scale = scale;
        return scalarType;
    }

    public static ScalarType createDecimalV3TypeForZero(int scale) {
        final ScalarType scalarType;
        if (scale <= PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32)) {
            scalarType = new ScalarType(PrimitiveType.DECIMAL32);
        } else if (scale <= PrimitiveType.getDefaultScaleOfDecimal(PrimitiveType.DECIMAL64)) {
            scalarType = new ScalarType(PrimitiveType.DECIMAL64);
        } else if (scale <= PrimitiveType.getDefaultScaleOfDecimal(PrimitiveType.DECIMAL128)) {
            scalarType = new ScalarType(PrimitiveType.DECIMAL128);
        } else {
            scalarType = new ScalarType(PrimitiveType.DECIMAL128);
            scale = PrimitiveType.getDefaultScaleOfDecimal(PrimitiveType.DECIMAL128);
        }
        scalarType.precision = scale;
        scalarType.scale = scale;
        return scalarType;
    }

    /**
     * Wildcard decimal is used for function matching
     */
    public static ScalarType createWildcardDecimalV3Type(PrimitiveType type) {
        Preconditions.checkArgument(type.isDecimalV3Type());
        ScalarType scalarType = new ScalarType(type);
        scalarType.precision = -1;
        scalarType.scale = -1;
        return scalarType;
    }

    public static ScalarType createDecimalV3Type(PrimitiveType type, int precision) {
        int defaultScale = PrimitiveType.getDefaultScaleOfDecimal(type);
        return createDecimalV3Type(type, precision, Math.min(precision, defaultScale));
    }

    public static ScalarType createDecimalV3Type(PrimitiveType type) {
        int maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(type);
        int defaultScale = PrimitiveType.getDefaultScaleOfDecimal(type);
        return createDecimalV3Type(type, maxPrecision, defaultScale);
    }

    public static ScalarType createDecimalV3NarrowestType(int precision, int scale) {
        if (precision == 0 && scale == 0) {
            return createDecimalV3TypeForZero(0);
        }
        final int decimal32MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32);
        final int decimal64MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL64);
        final int decimal128MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
        if (0 < precision && precision <= decimal32MaxPrecision) {
            return createDecimalV3Type(PrimitiveType.DECIMAL32, precision, scale);
        } else if (decimal32MaxPrecision < precision && precision <= decimal64MaxPrecision) {
            return createDecimalV3Type(PrimitiveType.DECIMAL64, precision, scale);
        } else if (decimal64MaxPrecision < precision && precision <= decimal128MaxPrecision) {
            return createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale);
        } else {
            Preconditions.checkState(false,
                    "Illegal decimal precision(1 to 38): precision=" + precision);
            return ScalarType.INVALID;
        }
    }

    public static ScalarType createDefaultString() {
        ScalarType stringType = ScalarType.createVarcharType(ScalarType.DEFAULT_STRING_LENGTH);
        return stringType;
    }

    // Use for Hive string now.
    public static ScalarType createDefaultExternalTableString() {
        ScalarType stringType = ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH);
        return stringType;
    }

    public static ScalarType createMaxVarcharType() {
        ScalarType stringType = ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH);
        return stringType;
    }

    public static ScalarType createMaxVarcharType() {
        ScalarType stringType = ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH);
        stringType.setAssignedStrLenInColDefinition();
        return stringType;
    }

    public static ScalarType createVarcharType(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.len = len;
        return type;
    }

    public static ScalarType createVarchar(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.len = len;
        return type;
    }

    public static ScalarType createVarcharType() {
        return Type.VARCHAR;
    }

    public static ScalarType createHllType() {
        ScalarType type = new ScalarType(PrimitiveType.HLL);
        type.len = MAX_HLL_LENGTH;
        return type;
    }

    public static ScalarType createUnknownType() {
        return new ScalarType(PrimitiveType.UNKNOWN_TYPE);
    }

    // A common type for two decimal v3 types means that if t2 = getCommonTypeForDecimalV3(t0, t1),
    // two invariants following is always holds:
    // 1. t2's integer part is sufficient to hold both t0 and t1's counterparts: i.e.
    //    t2.precision - t2.scale = max((t0.precision - t0.scale), (t1.precision - t1.scale))
    // 2. t2's fraction part is sufficient to hold both t0 and t1's counterparts: i.e.
    //    t2.scale = max(t0.scale, t1.scale)
    public static ScalarType getCommonTypeForDecimalV3(ScalarType lhs, ScalarType rhs) {
        Preconditions.checkState(lhs.isDecimalV3() && rhs.isDecimalV3());
        int lhsPrecision = lhs.getScalarPrecision();
        int lhsScale = lhs.getScalarScale();
        int rhsPrecision = rhs.getScalarPrecision();
        int rhsScale = rhs.getScalarScale();
        int lhsIntegerPartWidth = lhsPrecision - lhsScale;
        int rhsIntegerPartWidth = rhsPrecision - rhsScale;
        int integerPartWidth = Math.max(lhsIntegerPartWidth, rhsIntegerPartWidth);
        int scale = Math.max(lhsScale, rhsScale);
        int precision = integerPartWidth + scale;
        if (precision > 38) {
            return ScalarType.DOUBLE;
        } else {
            // the common type's PrimitiveType of two decimal types should wide enough, i.e
            // the common type of (DECIMAL32, DECIMAL64) should be DECIMAL64
            PrimitiveType primitiveType =
                    PrimitiveType.getWiderDecimalV3Type(lhs.getPrimitiveType(), rhs.getPrimitiveType());
            // the narrowestType for specified precision and scale is just wide properly to hold a decimal value, i.e
            // DECIMAL128(7,4), DECIMAL64(7,4) and DECIMAL32(7,4) can all be held in a DECIMAL32(7,4) type without
            // precision loss.
            Type narrowestType = ScalarType.createDecimalV3NarrowestType(precision, scale);
            primitiveType = PrimitiveType.getWiderDecimalV3Type(primitiveType, narrowestType.getPrimitiveType());
            // create a commonType with wider primitive type.
            return ScalarType.createDecimalV3Type(primitiveType, precision, scale);
        }
    }

    // getAssigmentCompatibleTypeOfDecimalV3 is used by FunctionCallExpr for finding the common Type of argument types.
    // i.e.
    // least(decimal64(15,2), decimal32(5,4)) match least instance: least(decimal64(18,4), decimal64(18,4));
    // least(double, decimal128(38,7)) match least instance: least(double, double).
    public static ScalarType getAssigmentCompatibleTypeOfDecimalV3(
            ScalarType decimalType, ScalarType otherType) {
        Preconditions.checkState(decimalType.isDecimalV3());
        // same triple (type, precision, scale)
        if (decimalType.equals(otherType)) {
            PrimitiveType type = PrimitiveType.getWiderDecimalV3Type(PrimitiveType.DECIMAL64, decimalType.type);
            return ScalarType.createDecimalV3Type(type, decimalType.precision, decimalType.scale);
        }

        switch (otherType.getPrimitiveType()) {
            case DECIMALV2:
                return getCommonTypeForDecimalV3(decimalType,
                        createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 9));
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return getCommonTypeForDecimalV3(decimalType, otherType);

            case BOOLEAN:
                return getCommonTypeForDecimalV3(decimalType,
                        createDecimalV3Type(PrimitiveType.DECIMAL32, 1, 0));
            case TINYINT:
                return getCommonTypeForDecimalV3(decimalType,
                        createDecimalV3Type(PrimitiveType.DECIMAL32, 3, 0));
            case SMALLINT:
                return getCommonTypeForDecimalV3(decimalType,
                        createDecimalV3Type(PrimitiveType.DECIMAL32, 5, 0));
            case INT:
                return getCommonTypeForDecimalV3(decimalType,
                        createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 0));
            case BIGINT:
                return getCommonTypeForDecimalV3(decimalType,
                        createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 0));
            case LARGEINT:
                return getCommonTypeForDecimalV3(decimalType,
                        createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case DATE:
            case DATETIME:
            case TIME:
            case JSON:
                return DOUBLE;
            default:
                return INVALID;
        }
    }

    /**
     * Return type t such that values from both t1 and t2 can be assigned to t.
     * If strict, only return types when there will be no loss of precision.
     * Returns INVALID_TYPE if there is no such type or if any of t1 and t2
     * is INVALID_TYPE.
     */
    public static ScalarType getAssignmentCompatibleType(ScalarType t1, ScalarType t2, boolean strict) {
        if (!t1.isValid() || !t2.isValid()) {
            return INVALID;
        }
        if (t1.isUnknown() || t2.isUnknown()) {
            return UNKNOWN_TYPE;
        }
        if (t1.equals(t2)) {
            return t1;
        }
        if (t1.isNull()) {
            return t2;
        }
        if (t2.isNull()) {
            return t1;
        }

        boolean t1IsHLL = t1.type == PrimitiveType.HLL;
        boolean t2IsHLL = t2.type == PrimitiveType.HLL;
        if (t1IsHLL || t2IsHLL) {
            if (t1IsHLL && t2IsHLL) {
                return createHllType();
            }
            return INVALID;
        }

        if (t1.isStringType() || t2.isStringType()) {
            return createVarcharType(Math.max(t1.len, t2.len));
        }

        if (t1.isDecimalV3()) {
            return getAssigmentCompatibleTypeOfDecimalV3(t1, t2);
        }

        if (t2.isDecimalV3()) {
            return getAssigmentCompatibleTypeOfDecimalV3(t2, t1);
        }

        if (t1.isDecimalOfAnyVersion() && t2.isDate() || t1.isDate() && t2.isDecimalOfAnyVersion()) {
            return INVALID;
        }

        if (t1.isDecimalV2() || t2.isDecimalV2()) {
            return DECIMALV2;
        }

        if (t1.isFunctionType() || t2.isFunctionType()) {
            return INVALID;
        }

        PrimitiveType smallerType =
                (t1.type.ordinal() < t2.type.ordinal() ? t1.type : t2.type);
        PrimitiveType largerType =
                (t1.type.ordinal() > t2.type.ordinal() ? t1.type : t2.type);
        PrimitiveType result = compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
        Preconditions.checkNotNull(result, String.format("No assignment from %s to %s", t1, t2));
        return createType(result);
    }

    /**
     * Returns true t1 can be implicitly cast to t2, false otherwise.
     * If strict is true, only consider casts that result in no loss of precision.
     */
    public static boolean isImplicitlyCastable(ScalarType t1, ScalarType t2, boolean strict) {
        return getAssignmentCompatibleType(t1, t2, strict).matchesType(t2);
    }

    // TODO(mofei) Why call implicit cast in the explicit cast context
    public static boolean canCastTo(ScalarType type, ScalarType targetType) {
        return PrimitiveType.isImplicitCast(type.getPrimitiveType(), targetType.getPrimitiveType());
    }

    @Override
    public String toString() {
        if (type == PrimitiveType.CHAR) {
            if (isWildcardChar()) {
                return "CHAR";
            }
            return "CHAR(" + len + ")";
        } else if (type.isDecimalV2Type()) {
            if (isWildcardDecimal()) {
                return "DECIMAL";
            }
            return "DECIMAL(" + precision + "," + scale + ")";
        } else if (type.isDecimalV3Type()) {
            if (isWildcardDecimal()) {
                return type.toString();
            }
            return type + "(" + precision + "," + scale + ")";
        } else if (type == PrimitiveType.VARCHAR) {
            if (isWildcardVarchar()) {
                return "VARCHAR";
            }
            return "VARCHAR(" + len + ")";
        }
        return type.toString();
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (type) {
            case CHAR:
                if (len == -1) {
                    stringBuilder.append("char");
                } else {
                    stringBuilder.append("char").append("(").append(len).append(")");
                }
                break;
            case VARCHAR:
                if (len == -1) {
                    stringBuilder.append("varchar");
                } else {
                    stringBuilder.append("varchar").append("(").append(len).append(")");
                }
                break;
            case DECIMALV2:
                stringBuilder.append("decimal").append("(").append(precision).append(", ").append(scale).append(")");
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                stringBuilder.append(type.toString().toLowerCase()).append("(").append(precision).append(", ")
                        .append(scale).append(")");
                break;
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "tinyint(4)";
            case SMALLINT:
                return "smallint(6)";
            case INT:
                return "int(11)";
            case BIGINT:
                return "bigint(20)";
            case LARGEINT:
                return "largeint(40)";
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATETIME:
            case HLL:
            case BITMAP:
            case BINARY:
            case PERCENTILE:
            case JSON:
            case FUNCTION:
                stringBuilder.append(type.toString().toLowerCase());
                break;
            default:
                stringBuilder.append("unknown type: ").append(type);
                break;
        }
        return stringBuilder.toString();
    }

    @Override
    protected String prettyPrint(int lpad) {
        return Strings.repeat(" ", lpad) + toSql();
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        switch (type) {
            case VARCHAR:
            case CHAR:
            case HLL: {
                node.setType(TTypeNodeType.SCALAR);
                TScalarType scalarType = new TScalarType();
                scalarType.setType(type.toThrift());
                scalarType.setLen(len);
                node.setScalar_type(scalarType);
                break;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                node.setType(TTypeNodeType.SCALAR);
                TScalarType scalarType = new TScalarType();
                scalarType.setType(type.toThrift());
                scalarType.setScale(scale);
                scalarType.setPrecision(precision);
                node.setScalar_type(scalarType);
                break;
            }
            default: {
                node.setType(TTypeNodeType.SCALAR);
                TScalarType scalarType = new TScalarType();
                scalarType.setType(type.toThrift());
                node.setScalar_type(scalarType);
                break;
            }
        }
    }

    public int decimalPrecision() {
        Preconditions.checkState(type.isDecimalV2Type() || type.isDecimalV3Type());
        return precision;
    }

    public int decimalScale() {
        Preconditions.checkState(type.isDecimalV2Type() || type.isDecimalV3Type());
        return scale;
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return type;
    }

    public int ordinal() {
        return type.ordinal();
    }

    public int getLength() {
        return len;
    }

    public void setLength(int len) {
        this.len = len;
    }

    // add scalar infix to override with getPrecision
    public int getScalarScale() {
        return scale;
    }

    public int getScalarPrecision() {
        return precision;
    }

    @Override
    public boolean isWildcardDecimal() {
        return (type.isDecimalV2Type() || type.isDecimalV3Type())
                && precision == -1 && scale == -1;
    }

    @Override
    public boolean isWildcardVarchar() {
        return (type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) && len == -1;
    }

    @Override
    public boolean isWildcardChar() {
        return type == PrimitiveType.CHAR && len == -1;
    }

    @Override
    public boolean isSupported() {
        // BINARY and UNKNOWN_TYPE is unsupported
        return (type != PrimitiveType.BINARY) &&
                (type != PrimitiveType.UNKNOWN_TYPE);
    }

    @Override
    public int getSlotSize() {
        return type.getSlotSize();
    }

    @Override
    public int getTypeSize() {
        return type.getTypeSize();
    }

    /**
     * Returns true if this object is of type t.
     * Handles wildcard types. That is, if t is the wildcard type variant
     * of 'this', returns true.
     */
    @Override
    public boolean matchesType(Type t) {
        if (t.isPseudoType()) {
            return t.matchesType(this);
        }
        if (this.getPrimitiveType() == t.getPrimitiveType()) {
            Preconditions.checkArgument(t.isScalarType());
            return !this.isDecimalV3()
                    || t.isWildcardDecimal()
                    || this.isWildcardDecimal()
                    || (getScalarScale() == ((ScalarType) t).getScalarScale());
        }
        if (this.isStringType() && t.isStringType()) {
            return true;
        }
        return isDecimalV2() && t.isDecimalV2();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ScalarType)) {
            return false;
        }
        ScalarType other = (ScalarType) o;
        if (type != other.type) {
            return false;
        }
        if (type == PrimitiveType.CHAR) {
            return len == other.len;
        }
        if (type == PrimitiveType.VARCHAR) {
            return len == other.len;
        }
        if (type.isDecimalV2Type() || type.isDecimalV3Type()) {
            return precision == other.precision && scale == other.scale;
        }
        return true;
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = type.toThrift();
        if (type == PrimitiveType.CHAR || type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) {
            thrift.setLen(len);
        }
        if (type.isDecimalV2Type() || type.isDecimalV3Type()) {
            thrift.setPrecision(precision);
            thrift.setScale(scale);
        }
        return thrift;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + Objects.hashCode(type);
        result = 31 * result + precision;
        result = 31 * result + scale;
        return result;
    }

    @Override
    public ScalarType clone() {
        return (ScalarType) super.clone();
    }

    @Override
    public String canonicalName() {
        if (isDecimalOfAnyVersion()) {
            Preconditions.checkArgument(!isWildcardDecimal());
            return String.format("DECIMAL(%d,%d)", precision, scale);
        } else {
            return toString();
        }
    }

    // This implementation is the same as BE schema_columns_scanner.cpp to_mysql_data_type_string
    public String toMysqlDataTypeString() {
        switch (type) {
            case BOOLEAN:
                return "tinyint";
            case LARGEINT:
                return "bigint unsigned";
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMALV2:
                return "decimal";
            default:
                return type.toString().toLowerCase();
        }
    }

    // This implementation is the same as BE schema_columns_scanner.cpp type_to_string
    public String toMysqlColumnTypeString() {
        switch (type) {
            case BOOLEAN:
                return "tinyint(1)";
            case LARGEINT:
                return "bigint(20) unsigned";
            default:
                return toSql();
        }
    }
}
