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
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.type.Type;

import java.time.LocalDateTime;

/**
 * Centralized factory for building {@link LiteralExpr} instances. The logic used to live
 * in {@link LiteralExpr} directly but has been moved here to keep the base class focused
 * on shared literal behaviour.
 */
public final class LiteralExprFactory {
    private LiteralExprFactory() {
    }

    public static LiteralExpr create(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        LiteralExpr literalExpr;
        try {
            switch (type.getPrimitiveType()) {
                case NULL_TYPE:
                    literalExpr = new NullLiteral();
                    break;
                case BOOLEAN:
                    literalExpr = new BoolLiteral(value);
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    literalExpr = new IntLiteral(value, type);
                    break;
                case LARGEINT:
                    literalExpr = new LargeIntLiteral(value);
                    break;
                case FLOAT:
                case DOUBLE:
                    literalExpr = new FloatLiteral(value, type);
                    break;
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    literalExpr = new DecimalLiteral(value);
                    break;
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case HLL:
                    literalExpr = new StringLiteral(value);
                    break;
                case DATE:
                case DATETIME:
                    try {
                        LocalDateTime localDateTime = DateUtils.parseStrictDateTime(value);
                        literalExpr = new DateLiteral(localDateTime, type);
                        if (type.isDatetime() && value.contains(".") && localDateTime.getNano() == 0) {
                            int precision = value.length() - value.indexOf(".") - 1;
                            ((DateLiteral) literalExpr).setPrecision(precision);
                        }
                    } catch (Exception ex) {
                        throw new AnalysisException("date literal [" + value + "] is invalid");
                    }
                    break;
                default:
                    throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
            }
        } catch (ParsingException e) {
            throw new AnalysisException(e.getDetailMsg());
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

    public static LiteralExpr createInfinity(Type type, boolean isMax) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        if (isMax) {
            return MaxLiteral.MAX_VALUE;
        }
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return IntLiteral.createMinValue(type);
            case LARGEINT:
                return LargeIntLiteral.createMinValue();
            case DATE:
            case DATETIME:
                return DateLiteral.createMinValue(type);
            default:
                throw new AnalysisException("Invalid data type for creating infinity: " + type);
        }
    }

    public static LiteralExpr createDefault(Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        LiteralExpr literalExpr;
        switch (type.getPrimitiveType()) {
            case NULL_TYPE:
                literalExpr = new NullLiteral();
                break;
            case BOOLEAN:
                literalExpr = new BoolLiteral(false);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                literalExpr = new IntLiteral(0, type);
                break;
            case LARGEINT:
                literalExpr = new LargeIntLiteral("0");
                break;
            case FLOAT:
            case DOUBLE:
                literalExpr = new FloatLiteral(0.0, type);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                literalExpr = new DecimalLiteral("0");
                break;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case HLL:
                literalExpr = new StringLiteral("");
                break;
            case DATE:
            case DATETIME:
                literalExpr = new DateLiteral(DateUtils.parseStrictDateTime("1970-01-01 00:00:00"), type);
                break;
            default:
                throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
        }
        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }
}
