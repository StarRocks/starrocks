// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
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
            if ((type1.isStringType() && type2.isExactNumericType()) ||
                    (type1.isExactNumericType() && type2.isStringType())) {
                return Type.STRING;
            }
        }

        return BinaryPredicate.getCmpType(type1, type2);
    }

    public static Type getCompatibleTypeForCaseWhen(List<Type> types) {
        Type compatibleType = types.get(0);
        for (int i = 1; i < types.size(); i++) {
            compatibleType = Type.getAssignmentCompatibleType(compatibleType, types.get(i), false);
            if (!compatibleType.isValid()) {
                throw new SemanticException("Failed to get Compatible Type ForCaseWhen with %s and %s",
                        types.get(i), types.get(i - 1));
            }
        }

        return compatibleType;
    }

    public static class TypeTriple {
        public ScalarType returnType;
        public ScalarType lhsTargetType;
        public ScalarType rhsTargetType;
    }

    public static TypeTriple getReturnTypeOfDecimal(ArithmeticExpr.Operator op, ScalarType lhsType,
                                                    ScalarType rhsType) {
        Preconditions.checkState(lhsType.isDecimalV3() && rhsType.isDecimalV3(),
                "Types of lhs and rhs must be DecimalV3");
        final PrimitiveType lhsPtype = lhsType.getPrimitiveType();
        final PrimitiveType rhsPtype = rhsType.getPrimitiveType();
        final int lhsScale = lhsType.getScalarScale();
        final int rhsScale = rhsType.getScalarScale();

        // get the wider decimal type
        PrimitiveType widerType = PrimitiveType.getWiderDecimalV3Type(lhsPtype, rhsPtype);
        // compute arithmetic expr use decimal64 for both decimal32 and decimal64
        widerType = PrimitiveType.getWiderDecimalV3Type(widerType, PrimitiveType.DECIMAL64);
        int maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(widerType);

        TypeTriple result = new TypeTriple();
        result.lhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, lhsScale);
        result.rhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, rhsScale);
        int returnScale = 0;
        switch (op) {
            case ADD:
            case SUBTRACT:
            case MOD:
                returnScale = Math.max(lhsScale, rhsScale);
                break;
            case MULTIPLY:
                returnScale = lhsScale + rhsScale;
                // promote type result type of multiplication if it is too narrow to hold all significant bits
                if (returnScale > maxPrecision) {
                    final int maxPrecisionOfDecimal128 =
                            PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
                    // decimal128 is already the widest decimal types, so throw an error if scale of result exceeds 38
                    Preconditions.checkState(widerType != PrimitiveType.DECIMAL128,
                            String.format("Return scale(%d) exceeds maximum value(%d)", returnScale,
                                    maxPrecisionOfDecimal128));
                    widerType = PrimitiveType.DECIMAL128;
                    maxPrecision = maxPrecisionOfDecimal128;
                    result.lhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, lhsScale);
                    result.rhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, rhsScale);
                }
                break;
            case INT_DIVIDE:
            case DIVIDE:
                if (lhsScale <= 6) {
                    returnScale = lhsScale + 6;
                } else if (lhsScale <= 12) {
                    returnScale = 12;
                } else {
                    returnScale = lhsScale;
                }
                widerType = PrimitiveType.DECIMAL128;
                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(widerType);
                result.lhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, lhsScale);
                result.rhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, rhsScale);
                int adjustedScale = returnScale + rhsScale;
                if (adjustedScale > maxPrecision) {
                    throw new SemanticException(
                            String.format(
                                    "Dividend fails to adjust scale to %d that exceeds maximum value(%d)",
                                    adjustedScale,
                                    maxPrecision));
                }
                break;
            default:
                Preconditions.checkState(false, "DecimalV3 only support operators: +-*/%");
        }
        result.returnType = op == ArithmeticExpr.Operator.INT_DIVIDE ? ScalarType.BIGINT :
                ScalarType.createDecimalV3Type(widerType, maxPrecision, returnScale);
        return result;
    }
}
