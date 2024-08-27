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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.sql.analyzer.SemanticException;

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

        return Type.INVALID;
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
        return ScalarType.getAssignmentCompatibleType(t1, t2, false);
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
            return Type.INVALID;
        }
        Type valueCommon = getCommonSuperType(t1.getValueType(), t2.getValueType());
        if (!valueCommon.isValid()) {
            return Type.INVALID;
        }
        return new MapType(keyCommon, valueCommon);
    }

    private static Type getCommonStructType(StructType t1, StructType t2) {
        if (t1.getFields().size() != t2.getFields().size()) {
            return Type.INVALID;
        }
        ArrayList<StructField> fields = Lists.newArrayList();
        for (int i = 0; i < t1.getFields().size(); ++i) {
            Type fieldCommon = getCommonSuperType(t1.getField(i).getType(), t2.getField(i).getType());
            if (!fieldCommon.isValid()) {
                return Type.INVALID;
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

            if (!Type.canCastTo(expr.getType(), targetType)) {
                throw new SemanticException(
                        "Cannot cast '" + expr.toSql() + "' from " + expr.getType() + " to " + targetType);
            }
            return expr.uncheckedCastTo(targetType);
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

        if (Type.VARCHAR.equals(compatibleType)) {
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
            return ScalarType.getAssigmentCompatibleTypeOfDecimalV3((ScalarType) decimalType, (ScalarType) otherType);
        }

        if (t1.getPrimitiveType() != PrimitiveType.INVALID_TYPE &&
                t1.getPrimitiveType().equals(t2.getPrimitiveType())) {
            return t1;
        }

        if (t1.isJsonType() || t2.isJsonType()) {
            return Type.JSON;
        }


        PrimitiveType t1ResultType = t1.getResultType().getPrimitiveType();
        PrimitiveType t2ResultType = t2.getResultType().getPrimitiveType();
        // Following logical is compatible with MySQL.
        if ((t1ResultType == PrimitiveType.VARCHAR && t2ResultType == PrimitiveType.VARCHAR)) {
            return Type.VARCHAR;
        }
        if (t1ResultType == PrimitiveType.BIGINT && t2ResultType == PrimitiveType.BIGINT) {
            return Type.getAssignmentCompatibleType(t1, t2, false);
        }

        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.DECIMALV2)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.DECIMALV2)) {
            return Type.DECIMALV2;
        }
        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.LARGEINT)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.LARGEINT)) {
            return Type.LARGEINT;
        }
        return Type.DOUBLE;
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
            return Type.INVALID;
        }

        if (type1.equals(type2)) {
            return type1;
        }

        if (type1.isNull() || type2.isNull()) {
            return type1.isNull() ? type2 : type1;
        }

        if (type1.isJsonType() || type2.isJsonType()) {
            return Type.JSON;
        }

        if (type1.isDecimalV3() || type2.isDecimalV3()) {
            Type decimalType = type1.isDecimalV3() ? type1 : type2;
            Type otherType = type1.isDecimalV3() ? type2 : type1;
            Preconditions.checkState(otherType.isScalarType());
            return ScalarType.getAssigmentCompatibleTypeOfDecimalV3((ScalarType) decimalType, (ScalarType) otherType);
        }

        BiFunction<Type, Type, Boolean> isDataAndString =
                (a, b) -> a.isDateType() && (b.isStringType() || b.isDateType());
        if (isDataAndString.apply(type1, type2) || isDataAndString.apply(type2, type1)) {
            return Type.DATETIME;
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
        }

        return Type.DOUBLE;
    }

    private static Type getEquivalenceBaseType(Type type1, Type type2) {
        Type baseType = Type.STRING;
        if (ConnectContext.get() != null && SessionVariableConstants.DECIMAL.equalsIgnoreCase(ConnectContext.get()
                .getSessionVariable().getCboEqBaseType())) {
            baseType = Type.DEFAULT_DECIMAL128;
            if (type1.isDecimalOfAnyVersion() || type2.isDecimalOfAnyVersion()) {
                baseType = type1.isDecimalOfAnyVersion() ? type1 : type2;
            }
        }
        if (ConnectContext.get() != null && SessionVariableConstants.DOUBLE.equalsIgnoreCase(ConnectContext.get()
                .getSessionVariable().getCboEqBaseType())) {
            baseType = Type.DOUBLE;
        }
        return baseType;
    }

    public static Type getCompatibleTypeForCaseWhen(List<Type> types) {
        Type compatibleType = types.get(0);
        for (int i = 1; i < types.size(); i++) {
            compatibleType = getCommonSuperType(compatibleType, types.get(i));
            if (!compatibleType.isValid()) {
                throw new SemanticException("Failed to get compatible type for CaseWhen with %s and %s",
                        types.get(i), types.get(i - 1));
            }
        }

        return compatibleType;
    }
}
