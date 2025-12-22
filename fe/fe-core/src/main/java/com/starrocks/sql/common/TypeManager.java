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

package com.starrocks.sql.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprCastFunction;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.FunctionType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.JsonType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeCompatibilityMatrix;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.UnknownType;
import com.starrocks.type.VarcharType;
import com.starrocks.type.VariantType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * TypeManager serves as the type manager for the new optimizer.
 * All types of operations will be unified into this class.
 * <p>
 * TODO(lhy):
 * We will converge the original type management to this class step by step,
 * rather than scattered all over the code
 */
public class TypeManager {
    public static Type getCommonSuperType(Type t1, Type t2) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return getCommonScalarType((ScalarType) t1, (ScalarType) t2);
        }
        if (t1.isArrayType() && t2.isArrayType()) {
            return getCommonArrayType((ArrayType) t1, (ArrayType) t2);
        }
        if (t1.isMapType() && t2.isMapType()) {
            return getCommonMapType((MapType) t1, (MapType) t2);
        }
        if (t1.isStructType() && t2.isStructType()) {
            return getCommonStructType((StructType) t1, (StructType) t2);
        }
        if (t1.isBoolean() && t2.isComplexType()) {
            return t2;
        }
        if (t1.isComplexType() && t2.isBoolean()) {
            return t1;
        }
        if (t1.isNull() || t2.isNull()) {
            return t1.isNull() ? t2 : t1;
        }

        return InvalidType.INVALID;
    }

    public static Type getCommonSuperType(List<Type> types) {
        Type commonType = types.get(0);
        for (int i = 1; i < types.size(); ++i) {
            Type nextType = getCommonSuperType(commonType, types.get(i));
            if (nextType.isInvalid()) {
                throw new SemanticException("types " + commonType + " and " + types.get(i) + " cannot be matched");
            }
            commonType = nextType;
        }
        return commonType;
    }

    private static Type getCommonScalarType(ScalarType t1, ScalarType t2) {
        return getAssignmentCompatibleType(t1, t2, false);
    }

    private static Type getCommonArrayType(ArrayType t1, ArrayType t2) {
        Type item1 = t1.getItemType();
        Type item2 = t2.getItemType();
        Type common = getCommonSuperType(item1, item2);
        return common.isValid() ? new ArrayType(common) : common;
    }

    private static Type getCommonMapType(MapType t1, MapType t2) {
        Type keyCommon = getCommonSuperType(t1.getKeyType(), t2.getKeyType());
        if (!keyCommon.isValid()) {
            return InvalidType.INVALID;
        }
        Type valueCommon = getCommonSuperType(t1.getValueType(), t2.getValueType());
        if (!valueCommon.isValid()) {
            return InvalidType.INVALID;
        }
        return new MapType(keyCommon, valueCommon);
    }

    private static Type getCommonStructType(StructType t1, StructType t2) {
        if (t1.getFields().size() != t2.getFields().size()) {
            return InvalidType.INVALID;
        }
        ArrayList<StructField> fields = Lists.newArrayList();
        for (int i = 0; i < t1.getFields().size(); ++i) {
            Type fieldCommon = getCommonSuperType(t1.getField(i).getType(), t2.getField(i).getType());
            if (!fieldCommon.isValid()) {
                return InvalidType.INVALID;
            }

            // default t1's field name
            fields.add(new StructField(t1.getField(i).getName(), fieldCommon));
        }
        return new StructType(fields);
    }

    public static Expr addCastExpr(Expr expr, Type targetType) {
        try {
            if (targetType.matchesType(expr.getType()) || targetType.isNull()) {
                return expr;
            }

            if (!canCastTo(expr.getType(), targetType)) {
                throw new SemanticException(
                        "Cannot cast '" + ExprToSql.toSql(expr) + "' from " + expr.getType() + " to " + targetType);
            }
            return ExprCastFunction.uncheckedCastTo(expr, targetType);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    public static Type getCompatibleTypeForBetweenAndIn(List<Type> types, boolean isBetween) {
        Preconditions.checkState(!types.isEmpty());
        Type compatibleType = types.get(0);

        for (int i = 1; i < types.size(); i++) {
            compatibleType = getCompatibleTypeForBetweenAndIn(compatibleType, types.get(i), isBetween);
        }

        if (VarcharType.VARCHAR.equals(compatibleType)) {
            if (types.get(0).isDateType()) {
                return types.get(0);
            }
        }

        return compatibleType;
    }

    private static Type getCompatibleTypeForBetweenAndIn(Type t1, Type t2, boolean isBetween) {
        // if predicate is 'IN' and one type is string ,another type is not float
        // we choose string or decimal as cmpType according to session variable cboEqBaseType
        if (!isBetween) {
            if (t1.isComplexType() || t2.isComplexType()) {
                return TypeManager.getCommonSuperType(t1, t2);
            }

            if (t1.isStringType() && t2.isExactNumericType() || t1.isExactNumericType() && t2.isStringType()) {
                return getEquivalenceBaseType(t1, t2);
            }
        }
        if (t1.getPrimitiveType() == PrimitiveType.NULL_TYPE) {
            return t2;
        }
        if (t2.getPrimitiveType() == PrimitiveType.NULL_TYPE) {
            return t1;
        }

        if (t1.isDecimalV3() || t2.isDecimalV3()) {
            Type decimalType = t1.isDecimalV3() ? t1 : t2;
            Type otherType = t1.isDecimalV3() ? t2 : t1;
            Preconditions.checkState(otherType.isScalarType());
            return getAssigmentCompatibleTypeOfDecimalV3((ScalarType) decimalType, (ScalarType) otherType);
        }

        if (t1.getPrimitiveType() != PrimitiveType.INVALID_TYPE &&
                t1.getPrimitiveType().equals(t2.getPrimitiveType())) {
            return t1;
        }

        if (t1.isJsonType() || t2.isJsonType()) {
            return JsonType.JSON;
        }


        PrimitiveType t1ResultType = getResultType(t1).getPrimitiveType();
        PrimitiveType t2ResultType = getResultType(t2).getPrimitiveType();
        // Following logical is compatible with MySQL.
        if ((t1ResultType == PrimitiveType.VARCHAR && t2ResultType == PrimitiveType.VARCHAR)) {
            return VarcharType.VARCHAR;
        }
        if (t1ResultType == PrimitiveType.BIGINT && t2ResultType == PrimitiveType.BIGINT) {
            return getAssignmentCompatibleType(t1, t2, false);
        }

        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.DECIMALV2)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.DECIMALV2)) {
            return DecimalType.DECIMALV2;
        }
        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.LARGEINT)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.LARGEINT)) {
            return IntegerType.LARGEINT;
        }
        return FloatType.DOUBLE;
    }

    private static Type getResultType(Type type) {
        return switch (type.getPrimitiveType()) {
            case BOOLEAN, TINYINT, SMALLINT, INT, BIGINT -> IntegerType.BIGINT;
            case LARGEINT -> IntegerType.LARGEINT;
            case FLOAT, DOUBLE -> FloatType.DOUBLE;
            case DATE, DATETIME, TIME, CHAR, VARCHAR, HLL, BITMAP, PERCENTILE, JSON -> VarcharType.VARCHAR;
            case DECIMALV2 -> DecimalType.DECIMALV2;
            case DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256 -> type;
            case FUNCTION -> FunctionType.FUNCTION;
            default -> InvalidType.INVALID;
        };
    }

    public static Type getCompatibleTypeForBinary(boolean isRangeCompare, Type type1, Type type2) {
        // 1. Many join on-clause use string = int predicate, follow mysql will cast to double, but
        //    starrocks cast to double will lose precision, the predicate result will error
        // 2. Why only support equivalence and unequivalence expression cast to string? Because string order is different
        //    with number order, like: '12' > '2' is false, but 12 > 2 is true
        if (!isRangeCompare) {
            if (type1.isComplexType() || type2.isComplexType()) {
                return TypeManager.getCommonSuperType(type1, type2);
            }

            Type baseType = getEquivalenceBaseType(type1, type2);
            if ((type1.isStringType() && type2.isExactNumericType()) ||
                    (type1.isExactNumericType() && type2.isStringType())) {
                return baseType;
            }
        }

        if (type1.isArrayType() || type2.isArrayType()) {
            return TypeManager.getCommonSuperType(type1, type2);
        }
        if (type1.isComplexType() || type2.isComplexType() || type1.isOnlyMetricType() || type2.isOnlyMetricType()) {
            // We don't support complex type (map/struct) for GT/LT predicate.
            return InvalidType.INVALID;
        }

        if (type1.equals(type2)) {
            return type1;
        }

        if (type1.isNull() || type2.isNull()) {
            return type1.isNull() ? type2 : type1;
        }

        if (type1.isJsonType() || type2.isJsonType()) {
            return JsonType.JSON;
        }

        if (type1.isDecimalV3() || type2.isDecimalV3()) {
            Type decimalType = type1.isDecimalV3() ? type1 : type2;
            Type otherType = type1.isDecimalV3() ? type2 : type1;
            Preconditions.checkState(otherType.isScalarType());
            return getAssigmentCompatibleTypeOfDecimalV3((ScalarType) decimalType, (ScalarType) otherType);
        }

        BiFunction<Type, Type, Boolean> isDataAndString =
                (a, b) -> a.isDateType() && (b.isStringType() || b.isDateType());
        if (isDataAndString.apply(type1, type2) || isDataAndString.apply(type2, type1)) {
            return DateType.DATETIME;
        }

        // number type
        if (type1.isFixedPointType() && type2.isFixedPointType()) {
            return type1.getTypeSize() > type2.getTypeSize() ? type1 : type2;
        } else if ((type1.isBoolean() && type2.isNumericType()) ||
                (type1.isFixedPointType() && type2.isFloatingPointType())) {
            return type2;
        } else if ((type2.isBoolean() && type1.isNumericType()) ||
                (type2.isFixedPointType() && type1.isFloatingPointType())) {
            return type1;
        } else if (type1.isBinaryType() && type2.isVarchar()) {
            return type1;
        }

        return FloatType.DOUBLE;
    }

    private static Type getEquivalenceBaseType(Type type1, Type type2) {
        Type baseType = StringType.STRING;
        if (ConnectContext.get() != null && SessionVariableConstants.DECIMAL.equalsIgnoreCase(ConnectContext.get()
                .getSessionVariable().getCboEqBaseType())) {
            baseType = DecimalType.DEFAULT_DECIMAL128;
            // TODO(stephen): support auto scale up decimal precision
            if (type1.isDecimal256() || type2.isDecimal256()) {
                baseType = DecimalType.DEFAULT_DECIMAL256;
            }
            if (type1.isDecimalOfAnyVersion() || type2.isDecimalOfAnyVersion()) {
                baseType = type1.isDecimalOfAnyVersion() ? type1 : type2;
            }
        }
        if (ConnectContext.get() != null && SessionVariableConstants.DOUBLE.equalsIgnoreCase(ConnectContext.get()
                .getSessionVariable().getCboEqBaseType())) {
            baseType = FloatType.DOUBLE;
        }
        return baseType;
    }

    public static Type getCompatibleTypeForCaseWhen(List<Type> types) {
        Type compatibleType = types.get(0);
        boolean isContainVarcharWithoutLength = false;
        for (int i = 1; i < types.size(); i++) {
            compatibleType = getCommonSuperType(compatibleType, types.get(i));
            if (!compatibleType.isValid()) {
                throw new SemanticException("Failed to get compatible type for CaseWhen with %s and %s",
                        types.get(i), types.get(i - 1));
            }
            if (types.get(i) instanceof ScalarType) {
                ScalarType scalarType = (ScalarType) types.get(i);
                // If the varchar type is without length, we should use default string type
                // to avoid the varchar type without length is not compatible with other types.
                if (scalarType.isVarchar() && scalarType.getLength() <= 0) {
                    isContainVarcharWithoutLength = true;
                }
            }
        }
        if (compatibleType.isStringType()) {
            ScalarType resultType = (ScalarType) compatibleType;
            // If result type is varchar with length, it may cause the case when result type is not compatible.
            if (isContainVarcharWithoutLength && resultType.getLength() > 0) {
                return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
            } else {
                return compatibleType;
            }
        } else {
            return compatibleType;
        }
    }

    public static boolean canCastTo(Type from, Type to) {
        if (from.isNull()) {
            return true;
        } else if (from.isStringType() && to.isBitmapType()) {
            return true;
        } else if (from.isScalarType() && to.isScalarType()) {
            return canCastTo((ScalarType) from, (ScalarType) to);
        } else if (from.isArrayType() && to.isArrayType()) {
            return canCastTo(((ArrayType) from).getItemType(), ((ArrayType) to).getItemType());
        } else if (from.isMapType() && to.isMapType()) {
            MapType fromMap = (MapType) from;
            MapType toMap = (MapType) to;
            return canCastTo(fromMap.getKeyType(), toMap.getKeyType()) &&
                    canCastTo(fromMap.getValueType(), toMap.getValueType());
        } else if (from.isStructType() && to.isStructType()) {
            StructType fromStruct = (StructType) from;
            StructType toStruct = (StructType) to;
            if (fromStruct.getFields().size() != toStruct.getFields().size()) {
                return false;
            }
            for (int i = 0; i < fromStruct.getFields().size(); ++i) {
                if (!canCastTo(fromStruct.getField(i).getType(), toStruct.getField(i).getType())) {
                    return false;
                }
            }
            return true;
        } else if (from.isStringType() && to.isArrayType()) {
            return true;
        } else if (from.isJsonType() && to.isArrayType()) {
            ArrayType array = (ArrayType) to;
            if (array.getItemType().isScalarType() || array.getItemType().isStructType()) {
                return true;
            }
            return false;
        } else if (from.isJsonType() && to.isStructType()) {
            return true;
        } else if (from.isJsonType() && to.isMapType()) {
            MapType map = (MapType) to;
            return canCastTo(VarcharType.VARCHAR, map.getKeyType()) && canCastTo(JsonType.JSON, map.getValueType());
        } else if (from.isVariantType() && variantCanCastToComplexType(to)) {
            return true;
        } else if (from.isBoolean() && to.isComplexType()) {
            // for mock nest type with NULL value, the cast must return NULL
            // like cast(map{1: NULL} as MAP<int, int>)
            return true;
        } else {
            return false;
        }
    }

    private static boolean variantCanCastToComplexType(Type to) {
        if (to.isArrayType()) {
            ArrayType arrayType = (ArrayType) to;
            Type itemType = arrayType.getItemType();
            return itemType.isScalarType() || itemType.isStructType() || itemType.isVariantType();
        } else if (to.isMapType()) {
            MapType mapType = (MapType) to;
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            return canCastTo(VarcharType.VARCHAR, keyType) && canCastTo(VariantType.VARIANT, valueType);
        } else {
            return to.isStructType();
        }
    }

    /**
     * Return type t such that values from both t1 and t2 can be assigned to t without an
     * explicit cast. If strict, does not consider conversions that would result in loss
     * of precision (e.g. converting decimal to float). Returns INVALID_TYPE if there is
     * no such type or if any of t1 and t2 is INVALID_TYPE.
     * TODO: Support non-scalar types.
     */
    public static Type getAssignmentCompatibleType(Type t1, Type t2, boolean strict) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return getAssignmentCompatibleType((ScalarType) t1, (ScalarType) t2, strict);
        }
        return InvalidType.INVALID;
    }

    public static Type getCommonType(Type[] argTypes, int fromIndex, int toIndex) {
        Preconditions.checkState(argTypes != null);
        Preconditions.checkState(0 <= fromIndex && fromIndex < toIndex && toIndex <= argTypes.length);
        Type commonType = argTypes[fromIndex];
        for (int i = fromIndex + 1; i < toIndex; ++i) {
            commonType = getCommonType(commonType, argTypes[i]);
        }
        return commonType;
    }

    /**
     * Given two types, return the common supertype of them.
     *
     * @return the common type, INVALID if no common type exists.
     */
    public static Type getCommonType(Type t1, Type t2) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return getAssignmentCompatibleType((ScalarType) t1, (ScalarType) t2, true);
        }
        if (t1.isArrayType() && t2.isArrayType()) {
            ArrayType arrayType1 = (ArrayType) t1;
            ArrayType arrayType2 = (ArrayType) t2;

            Type item1 = arrayType1.getItemType();
            Type item2 = arrayType2.getItemType();
            Type common = getCommonType(item1, item2);
            return common.isValid() ? new ArrayType(common) : common;
        }
        if (t1.isNull() || t2.isNull()) {
            return t1.isNull() ? t2 : t1;
        }
        return InvalidType.INVALID;
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
            PrimitiveType type = PrimitiveType.getWiderDecimalV3Type(PrimitiveType.DECIMAL64, decimalType.getType());
            return TypeFactory.createDecimalV3Type(type, decimalType.getScalarPrecision(), decimalType.getScalarScale());
        }

        switch (otherType.getPrimitiveType()) {
            case DECIMALV2:
                return getCommonTypeForDecimalV3(decimalType,
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 9));
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return getCommonTypeForDecimalV3(decimalType, otherType);

            case BOOLEAN:
                return getCommonTypeForDecimalV3(decimalType,
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 1, 0));
            case TINYINT:
                return getCommonTypeForDecimalV3(decimalType,
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 3, 0));
            case SMALLINT:
                return getCommonTypeForDecimalV3(decimalType,
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 5, 0));
            case INT:
                return getCommonTypeForDecimalV3(decimalType,
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 0));
            case BIGINT:
                return getCommonTypeForDecimalV3(decimalType,
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 0));
            case LARGEINT:
                return getCommonTypeForDecimalV3(decimalType,
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case DATE:
            case DATETIME:
            case TIME:
            case JSON:
                return FloatType.DOUBLE;
            default:
                return InvalidType.INVALID;
        }
    }

    // isAssignable means that assigning or casting rhs to lhs never overflows.
    // only both integer part width and fraction part width of lhs is not narrower than counterparts
    // of rhs, then rhs can be assigned to lhs. for integer types, integer part width is computed by
    // calling Type::getPrecision and its scale is 0.
    public static boolean isAssignable2Decimal(ScalarType lhs, ScalarType rhs) {
        int lhsIntPartWidth;
        int lhsScale;
        int rhsIntPartWidth;
        int rhsScale;
        if (lhs.isFixedPointType()) {
            lhsIntPartWidth = lhs.getPrecision();
            lhsScale = 0;
        } else {
            lhsIntPartWidth = lhs.getScalarPrecision() - lhs.getScalarScale();
            lhsScale = lhs.getScalarScale();
        }

        if (rhs.isFixedPointType()) {
            rhsIntPartWidth = rhs.getPrecision();
            rhsScale = 0;
        } else {
            rhsIntPartWidth = rhs.getScalarPrecision() - rhs.getScalarScale();
            rhsScale = rhs.getScalarScale();
        }

        // when lhs is integer, for instance, tinyint, lhsIntPartWidth is 3, it cannot holds
        // a DECIMAL(3, 0).
        if (lhs.isFixedPointType() && rhs.isDecimalOfAnyVersion()) {
            return lhsIntPartWidth > rhsIntPartWidth && lhsScale >= rhsScale;
        } else {
            return lhsIntPartWidth >= rhsIntPartWidth && lhsScale >= rhsScale;
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
            return InvalidType.INVALID;
        }
        if (t1.isUnknown() || t2.isUnknown()) {
            return UnknownType.UNKNOWN_TYPE;
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

        boolean t1IsHLL = t1.getType() == PrimitiveType.HLL;
        boolean t2IsHLL = t2.getType() == PrimitiveType.HLL;
        if (t1IsHLL || t2IsHLL) {
            if (t1IsHLL && t2IsHLL) {
                return HLLType.HLL;
            }
            return InvalidType.INVALID;
        }

        if (t1.isStringType() || t2.isStringType()) {
            return TypeFactory.createVarcharType(Math.max(t1.getLength(), t2.getLength()));
        }

        if (t1.isDecimalV3()) {
            return getAssigmentCompatibleTypeOfDecimalV3(t1, t2);
        }

        if (t2.isDecimalV3()) {
            return getAssigmentCompatibleTypeOfDecimalV3(t2, t1);
        }

        if (t1.isDecimalOfAnyVersion() && t2.isDate() || t1.isDate() && t2.isDecimalOfAnyVersion()) {
            return InvalidType.INVALID;
        }

        if (t1.isDecimalV2() || t2.isDecimalV2()) {
            return DecimalType.DECIMALV2;
        }

        if (t1.isFunctionType() || t2.isFunctionType()) {
            return InvalidType.INVALID;
        }

        PrimitiveType smallerType =
                (t1.getType().ordinal() < t2.getType().ordinal() ? t1.getType() : t2.getType());
        PrimitiveType largerType =
                (t1.getType().ordinal() > t2.getType().ordinal() ? t1.getType() : t2.getType());
        PrimitiveType result = TypeCompatibilityMatrix.getCompatibleType(smallerType, largerType);
        Preconditions.checkNotNull(result, String.format("No assignment from %s to %s", t1, t2));
        return TypeFactory.createType(result);
    }

    /**
     * Returns true if t1 can be implicitly cast to t2 according to Impala's casting rules.
     * Implicit casts are always allowed when no loss of precision would result (i.e. every
     * value of t1 can be represented exactly by a value of t2). Implicit casts are allowed
     * in certain other cases such as casting numeric types to floating point types and
     * converting strings to timestamps.
     * If strict is true, only consider casts that result in no loss of precision.
     * TODO: Support casting of non-scalar types.
     */
    public static boolean isImplicitlyCastable(Type t1, Type t2, boolean strict) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return isImplicitlyCastable((ScalarType) t1, (ScalarType) t2, strict);
        }
        if (t1.isArrayType() && t2.isArrayType()) {
            return isImplicitlyCastable(((ArrayType) t1).getItemType(), ((ArrayType) t2).getItemType(), strict);
        }
        return false;
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
        boolean hasDecimal256 = lhs.isDecimal256() || rhs.isDecimal256();
        // TODO(stephen): support auto scale up decimal precision
        if ((precision > 38 && !hasDecimal256) || (precision > 76)) {
            return FloatType.DOUBLE;
        } else {
            // the common type's PrimitiveType of two decimal types should wide enough, i.e
            // the common type of (DECIMAL32, DECIMAL64) should be DECIMAL64
            PrimitiveType primitiveType =
                    PrimitiveType.getWiderDecimalV3Type(lhs.getPrimitiveType(), rhs.getPrimitiveType());
            // the narrowestType for specified precision and scale is just wide properly to hold a decimal value, i.e
            // DECIMAL128(7,4), DECIMAL64(7,4) and DECIMAL32(7,4) can all be held in a DECIMAL32(7,4) type without
            // precision loss.
            Type narrowestType = TypeFactory.createDecimalV3NarrowestType(precision, scale);
            primitiveType = PrimitiveType.getWiderDecimalV3Type(primitiveType, narrowestType.getPrimitiveType());
            // create a commonType with wider primitive type.
            return TypeFactory.createDecimalV3Type(primitiveType, precision, scale);
        }
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
}
