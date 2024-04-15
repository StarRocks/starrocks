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
package com.starrocks.connector.delta;

import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import io.delta.standalone.expressions.And;
import io.delta.standalone.expressions.Column;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.expressions.GreaterThan;
import io.delta.standalone.expressions.GreaterThanOrEqual;
import io.delta.standalone.expressions.In;
import io.delta.standalone.expressions.IsNotNull;
import io.delta.standalone.expressions.IsNull;
import io.delta.standalone.expressions.LessThan;
import io.delta.standalone.expressions.LessThanOrEqual;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.expressions.Not;
import io.delta.standalone.expressions.Or;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.catalog.PrimitiveType.DATE;

public class ScalarOperatorToDeltaExpression {
    private static final Logger LOG = LogManager.getLogger(ScalarOperatorToDeltaExpression.class);

    public Expression convert(ScalarOperator operator, DeltaContext context) {
        DeltaExprVisitor visitor = new DeltaExprVisitor();
        return operator.accept(visitor, context);
    }

    public static class DeltaContext {
        private final StructType schema;

        public DeltaContext(StructType schema) {
            this.schema = schema;
        }

        public StructType getSchema() {
            return schema;
        }
    }

    private static class DeltaExprVisitor extends ScalarOperatorVisitor<Expression, DeltaContext> {

        @Override
        public Expression visitCompoundPredicate(CompoundPredicateOperator operator, DeltaContext context) {
            CompoundPredicateOperator.CompoundType op = operator.getCompoundType();
            if (op == CompoundPredicateOperator.CompoundType.NOT) {
                if (operator.getChild(0) instanceof LikePredicateOperator) {
                    return null;
                }
                Expression expression = operator.getChild(0).accept(this, context);

                if (expression != null) {
                    return new Not(expression);
                }
            } else {
                Expression left = operator.getChild(0).accept(this, context);
                Expression right = operator.getChild(1).accept(this, context);
                if (left != null && right != null) {
                    return (op == CompoundPredicateOperator.CompoundType.OR) ?
                            new Or(left, right) : new And(left, right);
                }
            }
            return null;
        }

        @Override
        public Expression visitIsNullPredicate(IsNullPredicateOperator operator, DeltaContext context) {

            String columnName = getColumnName(operator.getChild(0));

            if (columnName == null) {
                return null;
            }
            Column column = context.getSchema().column(columnName);
            if (operator.isNotNull()) {
                return new IsNotNull(column);
            } else {
                return new IsNull(column);
            }
        }

        @Override
        public Expression visitBinaryPredicate(BinaryPredicateOperator operator, DeltaContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }
            Column column = context.getSchema().column(columnName);
            Literal literal = (Literal) getLiteralValue(operator.getChild(1), column);
            if (literal == null) {
                return null;
            }

            switch (operator.getBinaryType()) {
                case LT:
                    return new LessThan(column, literal);
                case LE:
                    return new LessThanOrEqual(column, literal);
                case GT:
                    return new GreaterThan(column, literal);
                case GE:
                    return new GreaterThanOrEqual(column, literal);
                case EQ:
                    return new EqualTo(column, literal);
                case NE:
                    return new Not(new EqualTo(column, literal));
                default:
                    return null;
            }
        }

        @Override
        public Expression visitInPredicate(InPredicateOperator operator, DeltaContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }

            Column column = context.getSchema().column(columnName);
            List<Expression> literalValues = operator.getListChildren().stream().map(childOperator ->
                    getLiteralValue(childOperator, column)
            ).collect(Collectors.toList());

            if (operator.isNotIn()) {
                return new Not(new In(column, literalValues));
            } else {
                return new In(column, literalValues);
            }
        }

        @Override
        public Expression visit(ScalarOperator scalarOperator, DeltaContext context) {
            return null;
        }


    }

    private static class ExtractColumnName extends ScalarOperatorVisitor<String, Void> {

        @Override
        public String visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        public String visitVariableReference(ColumnRefOperator operator, Void context) {
            return operator.getName();
        }

        public String visitCastOperator(CastOperator operator, Void context) {
            return operator.getChild(0).accept(this, context);
        }
    }

    private static String getColumnName(ScalarOperator operator) {
        if (operator == null) {
            return null;
        }
        String columnName = operator.accept(new ExtractColumnName(), null);
        if (columnName == null || columnName.isEmpty()) {
            return null;
        }
        return columnName;
    }

    private static Expression getLiteralValue(ScalarOperator operator, Column column) {
        if (operator == null) {
            return null;
        }

        return operator.accept(new ExtractLiteralValue(column), null);
    }

    private static class ExtractLiteralValue extends ScalarOperatorVisitor<Expression, Void> {

        private Column column;

        public ExtractLiteralValue(Column column) {
            this.column = column;
        }

        @Override
        public Expression visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public Expression visitConstant(ConstantOperator operator, Void context) {
            DataType dataType = column.dataType();
            if (dataType instanceof BooleanType) {
                try {
                    return Literal.of(new BoolLiteral(operator.getBoolean()).getValue());
                } catch (Exception e) {
                    LOG.error("Failed to convert {} to boolean type", operator);
                    throw new StarRocksConnectorException("Failed to convert %s to boolean type", operator);
                }
            } else if (dataType instanceof ByteType) {
                return Literal.of(operator.getInt());
            } else if (dataType instanceof IntegerType) {
                return Literal.of(operator.getInt());
            } else if (dataType instanceof LongType) {
                return Literal.of(operator.getBigint());
            } else if (dataType instanceof FloatType) {
                return Literal.of(operator.getFloat());
            } else if (dataType instanceof DoubleType) {
                return Literal.of(operator.getDouble());
            } else if (dataType instanceof StringType) {
                //When using an attempt, delta is of type string,
                // and the inner table is of type date.
                // An error is reported by converting the date type to string
                if (operator.getType().getPrimitiveType() == DATE) {
                    String tmpDate = null;
                    try {
                        tmpDate = new DateLiteral(operator.getDate(), Type.DATE).getStringValue();
                    } catch (AnalysisException e) {
                        throw new StarRocksConnectorException("Failed to convert %s to date type", operator);
                    }
                    return Literal.of(tmpDate);
                }
                return Literal.of(operator.getVarchar());
            } else if (dataType instanceof DateType) {
                return Literal.of(new Date(operator.getDate().toLocalDate().toEpochDay()));
            } else if (dataType instanceof TimestampType) {
                return Literal.of(Timestamp.valueOf(operator.getDatetime()));
            } else {
                return null;
            }
        }
    }
}
