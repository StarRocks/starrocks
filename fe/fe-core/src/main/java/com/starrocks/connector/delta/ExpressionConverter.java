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

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.AstVisitor;
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
import java.util.ArrayList;
import java.util.List;

public class ExpressionConverter extends AstVisitor<Expression, Void> {
    private static final Logger LOG = LogManager.getLogger(ExpressionConverter.class);

    StructType tableSchema;

    public ExpressionConverter(StructType tableSchema) {
        this.tableSchema = tableSchema;
    }

    public Expression convert(Expr expr) {
        if (expr == null) {
            return null;
        }
        return visit(expr);
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate node, Void context) {
        CompoundPredicate.Operator op = node.getOp();
        if (op == CompoundPredicate.Operator.NOT) {
            if (node.getChild(0) instanceof FunctionCallExpr ||
                    node.getChild(0) instanceof LikePredicate) {
                return null;
            }

            Expression expression = node.getChild(0).accept(this, null);
            if (expression != null) {
                return new Not(expression);
            }
        } else {
            Expression left = node.getChild(0).accept(this, null);
            Expression right = node.getChild(1).accept(this, null);
            if (left != null && right != null) {
                return (op == CompoundPredicate.Operator.OR) ? new Or(left, right) : new And(left, right);
            }
        }
        return null;
    }

    @Override
    public Expression visitIsNullPredicate(IsNullPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        Column column = tableSchema.column(columnName);
        if (node.isNotNull()) {
            return new IsNotNull(column);
        } else {
            return new IsNull(column);
        }
    }

    @Override
    public Expression visitBinaryPredicate(BinaryPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        Column column = tableSchema.column(columnName);
        Literal literal = getLiteral(node.getChild(1), column.dataType());
        if (literal == null) {
            return null;
        }
        switch (node.getOp()) {
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
    public Expression visitInPredicate(InPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        Column column = tableSchema.column(columnName);
        List<Expr> valuesExprList = node.getListChildren();
        List<Literal> literalValues = new ArrayList<>(valuesExprList.size());
        for (Expr valueExpr : valuesExprList) {
            Literal value = getLiteral(valueExpr, column.dataType());
            if (value == null) {
                return null;
            }
            literalValues.add(value);
        }
        if (node.isNotIn()) {
            return new Not(new In(column, literalValues));
        } else {
            return new In(column, literalValues);
        }
    }

    private static Literal getLiteral(Expr expr, DataType dataType) {
        if (!(expr instanceof LiteralExpr)) {
            return null;
        }

        LiteralExpr literalExpr = (LiteralExpr) expr;
        if (dataType instanceof BooleanType) {
            try {
                return Literal.of(new BoolLiteral(literalExpr.getStringValue()).getValue());
            } catch (Exception e) {
                LOG.error("Failed to convert {} to boolean type", literalExpr);
                throw new StarRocksConnectorException("Failed to convert %s to boolean type", literalExpr);
            }
        } else if (dataType instanceof ByteType) {
            return Literal.of((byte) ((IntLiteral) literalExpr).getValue());
        } else if (dataType instanceof IntegerType) {
            return Literal.of((int) ((IntLiteral) literalExpr).getValue());
        } else if (dataType instanceof LongType) {
            return Literal.of(((IntLiteral) literalExpr).getValue());
        } else if (dataType instanceof FloatType) {
            return Literal.of((float) ((FloatLiteral) literalExpr).getValue());
        } else if (dataType instanceof DoubleType) {
            return Literal.of(((FloatLiteral) literalExpr).getValue());
        } else if (dataType instanceof StringType) {
            return Literal.of(((StringLiteral) literalExpr).getUnescapedValue());
        } else if (dataType instanceof DateType) {
            return Literal.of(Date.valueOf(literalExpr.getStringValue()));
        } else if (dataType instanceof TimestampType) {
            return Literal.of(Timestamp.valueOf(((DateLiteral) literalExpr).toLocalDateTime()));
        } else {
            return null;
        }
    }

    private static String getColumnName(Expr expr) {
        if (expr == null) {
            return null;
        }
        String columnName = new ExpressionConverter.ExtractColumnName().visit(expr, null);
        if (columnName == null || columnName.isEmpty()) {
            return null;
        }
        return columnName;
    }

    private static class ExtractColumnName extends AstVisitor<String, Void> {
        @Override
        public String visitCastExpr(CastExpr node, Void context) {
            return node.getChild(0).accept(this, null);
        }

        @Override
        public String visitSlot(SlotRef node, Void context) {
            return node.getColumn().getName();
        }
    }
}
