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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.MapType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public final class ExprCastFunction {

    private ExprCastFunction() {
    }

    public static Expr castTo(Expr expr, Type targetType) throws AnalysisException {
        Preconditions.checkNotNull(expr, "expression cannot be null");
        Preconditions.checkNotNull(targetType, "target type cannot be null");

        if (targetType.isNull()) {
            return expr;
        }

        if (targetType.isHllType() && expr.getType().isStringType()) {
            return expr;
        }

        if (!TypeManager.canCastTo(expr.getType(), targetType)) {
            throw new AnalysisException(
                    "Cannot cast '" + ExprToSql.toSql(expr) + "' from " + expr.getType() + " to " + targetType);
        }
        return uncheckedCastTo(expr, targetType);
    }

    public static Expr uncheckedCastTo(Expr expr, Type targetType) throws AnalysisException {
        Preconditions.checkNotNull(expr, "expression cannot be null");
        Preconditions.checkNotNull(targetType, "target type cannot be null");

        Expr specialized = specializedUncheckedCast(expr, targetType);
        if (specialized != null) {
            return specialized;
        }
        return new CastExpr(targetType, expr);
    }

    public static void castChild(Expr parent, Type targetType, int childIndex) throws AnalysisException {
        Preconditions.checkNotNull(parent, "parent expression cannot be null");
        Expr child = parent.getChild(childIndex);
        Expr newChild = castTo(child, targetType);
        parent.setChild(childIndex, newChild);
    }

    public static void uncheckedCastChild(Expr parent, Type targetType, int childIndex) throws AnalysisException {
        Preconditions.checkNotNull(parent, "parent expression cannot be null");
        Expr child = parent.getChild(childIndex);
        Expr newChild = uncheckedCastTo(child, targetType);
        parent.setChild(childIndex, newChild);
    }

    private static Expr specializedUncheckedCast(Expr expr, Type targetType) throws AnalysisException {
        if (expr instanceof FunctionCallExpr) {
            Expr result = castFunctionCall((FunctionCallExpr) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof StringLiteral) {
            Expr result = castStringLiteral((StringLiteral) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof LargeIntLiteral) {
            Expr result = castLargeIntLiteral((LargeIntLiteral) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof IntLiteral) {
            Expr result = castIntLiteral((IntLiteral) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof DateLiteral) {
            Expr result = castDateLiteral((DateLiteral) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof FloatLiteral) {
            Expr result = castFloatLiteral((FloatLiteral) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof NullLiteral) {
            Expr result = castNullLiteral((NullLiteral) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof DecimalLiteral) {
            Expr result = castDecimalLiteral((DecimalLiteral) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof UserVariableExpr) {
            Expr result = castUserVariableExpr((UserVariableExpr) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof MapExpr) {
            Expr result = castMapExpr((MapExpr) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        if (expr instanceof ArrayExpr) {
            Expr result = castArrayExpr((ArrayExpr) expr, targetType);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private static Expr castFunctionCall(FunctionCallExpr expr, Type targetType) {
        if (expr.getFn() == null || expr.getFn().getReturnType() == null) {
            return null;
        }
        if (expr.getFn().getReturnType().equals(targetType)) {
            return expr;
        }
        return null;
    }

    private static Expr castStringLiteral(StringLiteral literal, Type targetType) throws AnalysisException {
        String value = literal.getValue();
        if (targetType.isNumericType()) {
            switch (targetType.getPrimitiveType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    return new IntLiteral(value, targetType);
                case LARGEINT:
                    return new LargeIntLiteral(value);
                case FLOAT:
                case DOUBLE:
                    try {
                        return new FloatLiteral(Double.parseDouble(value), targetType);
                    } catch (NumberFormatException e) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_NUMBER, value);
                    }
                    return null;
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    DecimalLiteral decimalLiteral = new DecimalLiteral(value);
                    return castDecimalLiteral(decimalLiteral, targetType);
                default:
                    break;
            }
        } else if (targetType.isDateType()) {
            try {
                return convertStringLiteralToDate(value, targetType);
            } catch (AnalysisException e) {
                return null;
            }
        } else if (targetType.getPrimitiveType() == literal.getType().getPrimitiveType()) {
            return literal;
        } else if (targetType.isStringType()) {
            StringLiteral stringLiteral = new StringLiteral(literal);
            stringLiteral.setType(targetType);
            return stringLiteral;
        }
        return null;
    }

    private static Expr convertStringLiteralToDate(String value, Type targetType) throws AnalysisException {
        try {
            return new DateLiteral(value, targetType);
        } catch (AnalysisException e) {
            if (targetType.isDatetime()) {
                DateLiteral literal = new DateLiteral(value, DateType.DATE);
                literal.setType(DateType.DATETIME);
                return literal;
            }
            throw e;
        }
    }

    private static Expr castLargeIntLiteral(LargeIntLiteral literal, Type targetType) throws AnalysisException {
        BigInteger value = literal.getValue();
        if (targetType.isFloatingPointType()) {
            return new FloatLiteral(value.doubleValue(), targetType);
        } else if (targetType.isDecimalOfAnyVersion()) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(new BigDecimal(value));
            decimalLiteral.setType(targetType);
            return decimalLiteral;
        } else if (targetType.isNumericType()) {
            try {
                return new IntLiteral(value.longValueExact(), targetType);
            } catch (ArithmeticException e) {
                throw new AnalysisException("Number out of range[" + value + "]. type: " + targetType);
            }
        }
        return null;
    }

    private static Expr castIntLiteral(IntLiteral literal, Type targetType) throws AnalysisException {
        if (!targetType.isNumericType()) {
            return null;
        }
        if (targetType.isFixedPointType()) {
            if (!targetType.isLargeint()) {
                if (!literal.getType().equals(targetType)) {
                    if (literal.getType().getPrimitiveType().ordinal() > targetType.getPrimitiveType().ordinal()) {
                        return null;
                    } else {
                        IntLiteral intLiteral = new IntLiteral(literal);
                        intLiteral.setType(targetType);
                        return intLiteral;
                    }
                }
                return literal;
            } else {
                return new LargeIntLiteral(Long.toString(literal.getValue()));
            }
        } else if (targetType.isFloatingPointType()) {
            return new FloatLiteral((double) literal.getValue(), targetType);
        } else if (targetType.isDecimalOfAnyVersion()) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(new BigDecimal(literal.getValue()));
            decimalLiteral.setType(targetType);
            return decimalLiteral;
        }
        return literal;
    }

    private static Expr castDateLiteral(DateLiteral literal, Type targetType) throws AnalysisException {
        if (targetType.isDateType()) {
            if (literal.getType().equals(targetType)) {
                return literal;
            }
            if (targetType.isDate()) {
                return new DateLiteral((int) literal.getYear(), (int) literal.getMonth(), (int) literal.getDay());
            } else if (targetType.isDatetime()) {
                return new DateLiteral((int) literal.getYear(), (int) literal.getMonth(), (int) literal.getDay(),
                        (int) literal.getHour(), (int) literal.getMinute(), (int) literal.getSecond(),
                        (int) literal.getMicrosecond());
            } else {
                throw new AnalysisException("Error date literal type : " + literal.getType());
            }
        } else if (targetType.isStringType()) {
            return new StringLiteral(literal.getStringValue());
        } else if (TypeManager.isImplicitlyCastable(literal.getType(), targetType, true)) {
            return new CastExpr(targetType, literal);
        }
        Preconditions.checkState(false);
        return literal;
    }

    private static Expr castFloatLiteral(FloatLiteral literal, Type targetType) throws AnalysisException {
        if (!(targetType.isFloatingPointType() || targetType.isDecimalOfAnyVersion())) {
            return null;
        }
        if (targetType.isFloatingPointType()) {
            if (!literal.getType().equals(targetType)) {
                FloatLiteral floatLiteral = new FloatLiteral(literal);
                floatLiteral.setType(targetType);
                return floatLiteral;
            }
            return literal;
        } else {
            DecimalLiteral decimalLiteral = new DecimalLiteral(new BigDecimal(Double.toString(literal.getValue())));
            decimalLiteral.setType(targetType);
            return decimalLiteral;
        }
    }

    private static Expr castNullLiteral(NullLiteral literal, Type targetType) {
        Preconditions.checkState(targetType.isValid());
        if (!literal.getType().equals(targetType)) {
            NullLiteral nullLiteral = new NullLiteral(literal);
            nullLiteral.setType(targetType);
            return nullLiteral;
        }
        return literal;
    }

    private static Expr castDecimalLiteral(DecimalLiteral literal, Type targetType) throws AnalysisException {
        if (targetType.getPrimitiveType().isDecimalV3Type()) {
            literal.setType(targetType);
            DecimalLiteral.checkLiteralOverflowInBinaryStyle(literal.getValue(), (ScalarType) targetType);
            BigDecimal value = literal.getValue();
            int realScale = DecimalLiteral.getRealScale(value);
            int scale = ((ScalarType) targetType).getScalarScale();
            if (scale <= realScale) {
                literal.overwriteValue(value.setScale(scale, RoundingMode.HALF_UP));
            }
            return literal;
        } else if (targetType.getPrimitiveType().isDecimalV2Type()) {
            literal.setType(targetType);
            return literal;
        } else if (targetType.isFloatingPointType()) {
            return new FloatLiteral(literal.getValue().doubleValue(), targetType);
        } else if (targetType.isIntegerType()) {
            return new IntLiteral(literal.getValue().longValue(), targetType);
        } else if (targetType.isStringType()) {
            return new StringLiteral(literal.getValue().toString());
        }
        return null;
    }

    private static Expr castUserVariableExpr(UserVariableExpr expr, Type targetType) throws AnalysisException {
        Preconditions.checkState(expr.getValue() != null,
                "should analyze UserVariableExpr first then cast its value");
        UserVariableExpr userVariableExpr = new UserVariableExpr(expr);
        userVariableExpr.setValue(uncheckedCastTo(expr.getValue(), targetType));
        return userVariableExpr;
    }

    private static Expr castMapExpr(MapExpr expr, Type targetType) throws AnalysisException {
        if (!(targetType instanceof MapType)) {
            return null;
        }
        MapType mapType = (MapType) targetType;
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        List<Expr> newItems = new ArrayList<>();
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChild(i);
            Type desired = (i % 2 == 0) ? keyType : valueType;
            if (child.getType().matchesType(desired)) {
                newItems.add(child);
            } else {
                newItems.add(castTo(child, desired));
            }
        }
        MapExpr mapExpr = new MapExpr(targetType, newItems, expr.getPos());
        mapExpr.analysisDone();
        return mapExpr;
    }

    private static Expr castArrayExpr(ArrayExpr expr, Type targetType) throws AnalysisException {
        if (!(targetType instanceof ArrayType)) {
            return null;
        }
        ArrayType arrayType = (ArrayType) targetType;
        Type itemType = arrayType.getItemType();
        List<Expr> newItems = new ArrayList<>();
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChild(i);
            if (child.getType().matchesType(itemType)) {
                newItems.add(child);
            } else {
                newItems.add(castTo(child, itemType));
            }
        }
        ArrayExpr arrayExpr = new ArrayExpr(targetType, newItems, expr.getPos());
        arrayExpr.analysisDone();
        return arrayExpr;
    }
}
