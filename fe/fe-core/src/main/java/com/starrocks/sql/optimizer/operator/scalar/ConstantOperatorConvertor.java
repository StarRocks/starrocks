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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.ExprCastFunction;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.time.LocalDateTime;

/**
 * Converts a {@link ConstantOperator} to its corresponding {@link LiteralExpr}.
 *
 * <p>This utility class provides constant-to-literal conversion without the full
 * expression-building infrastructure of ScalarOperatorToExecExpr, intended for use in
 * partition-pruning and other paths that only need to convert constants to literals.
 */
public final class ConstantOperatorConvertor {

    private ConstantOperatorConvertor() {
    }

    /**
     * Converts a {@link ConstantOperator} to a {@link LiteralExpr}.
     *
     * @param literal the constant operator to convert; must not be {@code null}
     * @return the corresponding literal expression
     * @throws UnsupportedOperationException if the type is not supported or the conversion fails
     */
    public static LiteralExpr toLiteralExpr(ConstantOperator literal) {
        try {
            Type type = literal.getType();

            if (literal.isNull()) {
                NullLiteral nullLiteral = new NullLiteral();
                nullLiteral.setType(literal.getType());
                return nullLiteral;
            }

            if (type.isBoolean()) {
                return new BoolLiteral(literal.getBoolean());
            } else if (type.isTinyint()) {
                return new IntLiteral(literal.getTinyInt(), IntegerType.TINYINT);
            } else if (type.isSmallint()) {
                return new IntLiteral(literal.getSmallint(), IntegerType.SMALLINT);
            } else if (type.isInt()) {
                return new IntLiteral(literal.getInt(), IntegerType.INT);
            } else if (type.isBigint()) {
                return new IntLiteral(literal.getBigint(), IntegerType.BIGINT);
            } else if (type.isLargeint()) {
                return new LargeIntLiteral(literal.getLargeInt().toString());
            } else if (type.isFloat()) {
                return new FloatLiteral(literal.getDouble(), FloatType.FLOAT);
            } else if (type.isDouble()) {
                return new FloatLiteral(literal.getDouble(), FloatType.DOUBLE);
            } else if (type.isDate()) {
                LocalDateTime ldt = literal.getDate();
                return new DateLiteral(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth());
            } else if (type.isDatetime()) {
                LocalDateTime ldt = literal.getDatetime();
                return new DateLiteral(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                        ldt.getHour(), ldt.getMinute(), ldt.getSecond(), ldt.getNano() / 1000);
            } else if (type.isTime()) {
                return new FloatLiteral(literal.getTime(), DateType.TIME);
            } else if (type.isDecimalOfAnyVersion()) {
                DecimalLiteral d = new DecimalLiteral(literal.getDecimal());
                ExprCastFunction.uncheckedCastTo(d, type);
                return d;
            } else if (type.isVarchar() || type.isChar()) {
                return StringLiteral.create(literal.getVarchar());
            } else if (type.isBinaryType()) {
                return new VarBinaryLiteral(literal.getBinary());
            } else {
                throw new UnsupportedOperationException("Unsupported constant type: " + type.toSql());
            }
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new UnsupportedOperationException(e.getMessage(), e);
        }
    }
}
