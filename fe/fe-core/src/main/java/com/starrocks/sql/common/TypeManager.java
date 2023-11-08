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
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;

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
        if (t1.getFields().size() != t1.getFields().size()) {
            return Type.INVALID;
        }
        List<Type> fieldTypes = Lists.newArrayList();
        for (int i = 0; i < t1.getFields().size(); ++i) {
            Type fieldCommon = getCommonSuperType(t1.getField(i).getType(), t2.getField(i).getType());
            if (!fieldCommon.isValid()) {
                return Type.INVALID;
            }
            fieldTypes.add(fieldCommon);
        }
        // TODO(alvin): needed to assign field names for this struct type
        return new StructType(fieldTypes);
    }

    public static Expr addCastExpr(Expr expr, Type targetType) {
        try {
            if (targetType.matchesType(expr.getType()) || targetType.isNull()) {
                return expr;
            }

            if (expr.getType().isArrayType()) {
                Type originArrayItemType = ((ArrayType) expr.getType()).getItemType();

                if (!targetType.isArrayType()) {
                    throw new SemanticException(
                            "Cannot cast '" + expr.toSql() + "' from " + expr.getType() + " to " + targetType);
                }

                if (!Type.canCastTo(originArrayItemType, ((ArrayType) targetType).getItemType())) {
                    throw new SemanticException("Cannot cast '" + expr.toSql()
                            + "' from " + originArrayItemType + " to " + ((ArrayType) targetType).getItemType());
                }
            } else if (expr.getType().isMapType()) {
                if (!targetType.isMapType()) {
                    throw new SemanticException(
                            "Cannot cast '" + expr.toSql() + "' from " + expr.getType() + " to " + targetType);
                }

                if (!Type.canCastTo(expr.getType(), targetType)) {
                    throw new SemanticException("Cannot cast '" + expr.toSql()
                            + "' from " + expr.getType() + " to " + ((ArrayType) targetType).getItemType());
                }
            } else if (expr.getType().isStructType()) {
                if (!targetType.isStructType()) {
                    throw new SemanticException(
                            "Cannot cast '" + expr.toSql() + "' from " + expr.getType() + " to " + targetType);
                }

                if (!Type.canCastTo(expr.getType(), targetType)) {
                    throw new SemanticException("Cannot cast '" + expr.toSql()
                            + "' from " + expr.getType() + " to " + ((ArrayType) targetType).getItemType());
                }
            } else {
                if (!Type.canCastTo(expr.getType(), targetType)) {
                    throw new SemanticException("Cannot cast '" + expr.toSql()
                            + "' from " + expr.getType() + " to " + targetType);
                }
            }
            return expr.uncheckedCastTo(targetType);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    public static Type getCompatibleTypeForBetweenAndIn(List<Type> types) {
        Preconditions.checkState(types.size() > 0);
        Type compatibleType = types.get(0);

        for (int i = 1; i < types.size(); i++) {
            compatibleType = Type.getCmpType(compatibleType, types.get(i));
        }

        if (Type.VARCHAR.equals(compatibleType)) {
            if (types.get(0).isDateType()) {
                return types.get(0);
            }
        }

        return compatibleType;
    }

    public static Type getCompatibleTypeForBinary(boolean isNotRangeComparison, Type type1, Type type2) {
        // 1. Many join on-clause use string = int predicate, follow mysql will cast to double, but
        //    starrocks cast to double will lose precision, the predicate result will error
        // 2. Why only support equivalence and unequivalence expression cast to string? Because string order is different
        //    with number order, like: '12' > '2' is false, but 12 > 2 is true
        if (isNotRangeComparison) {
            Type baseType = Type.STRING;
            if (ConnectContext.get() != null && SessionVariableConstants.DECIMAL.equalsIgnoreCase(ConnectContext.get()
                    .getSessionVariable().getCboEqBaseType())) {
                baseType = Type.DEFAULT_DECIMAL128;
                if (type1.isDecimalOfAnyVersion() || type2.isDecimalOfAnyVersion()) {
                    baseType = type1.isDecimalOfAnyVersion() ? type1 : type2;
                }
            }

            if ((type1.isStringType() && type2.isExactNumericType()) ||
                    (type1.isExactNumericType() && type2.isStringType())) {
                return baseType;
            }
        }

        return BinaryPredicate.getCmpType(type1, type2);
    }

    public static Type getCompatibleTypeForCaseWhen(List<Type> types) {
        return getCompatibleType(types, "CaseWhen");
    }

    public static Type getCompatibleType(List<Type> types, String kind) {
        Type compatibleType = types.get(0);
        for (int i = 1; i < types.size(); i++) {
            compatibleType = Type.getAssignmentCompatibleType(compatibleType, types.get(i), false);
            if (!compatibleType.isValid()) {
                throw new SemanticException("Failed to get compatible type for %s with %s and %s",
                        kind, types.get(i), types.get(i - 1));
            }
        }

        return compatibleType;
    }
}
