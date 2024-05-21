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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.AstVisitor;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class ExpressionConverter implements AstVisitor<Predicate, Void> {
    private static final Logger LOG = LogManager.getLogger(ExpressionConverter.class);

    StructType tableSchema;

    public ExpressionConverter(StructType tableSchema) {
        this.tableSchema = tableSchema;
    }

    public Predicate convert(Expr expr) {
        if (expr == null) {
            return null;
        }
        return visit(expr);
    }

    @Override
    public Predicate visitCompoundPredicate(CompoundPredicate node, Void context) {
        CompoundPredicate.Operator op = node.getOp();
        if (op == CompoundPredicate.Operator.NOT) {
            if (node.getChild(0) instanceof FunctionCallExpr ||
                    node.getChild(0) instanceof LikePredicate) {
                return null;
            }

            Expression expression = node.getChild(0).accept(this, null);
            if (expression != null) {
                return new Predicate("NOT", expression);
            }
        } else {
            Predicate left = node.getChild(0).accept(this, null);
            Predicate right = node.getChild(1).accept(this, null);
            if (left != null && right != null) {
                return (op == CompoundPredicate.Operator.OR) ? new Or(left, right) : new And(left, right);
            }
        }
        return null;
    }

    @Override
    public Predicate visitIsNullPredicate(IsNullPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        int index = tableSchema.indexOf(columnName);
        Column column = tableSchema.column(index);
        if (node.isNotNull()) {
            return new Predicate("IS_NOT_NULL", column);
        } else {
            return new Predicate("IS_NULL", column);
        }
    }

    @Override
    public Predicate visitBinaryPredicate(BinaryPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }

        int index = tableSchema.indexOf(columnName);
        Column column = tableSchema.column(index);
        Literal literal = getLiteral(node.getChild(1), tableSchema.at(index).getDataType());
        if (literal == null) {
            return null;
        }
        switch (node.getOp()) {
            case LT:
                return new Predicate("<", column, literal);
            case LE:
                return new Predicate("<=", column, literal);
            case GT:
                return new Predicate(">", column, literal);
            case GE:
                return new Predicate(">=", column, literal);
            case EQ:
                return new Predicate("=", column, literal);
            case NE:
                return new Predicate("NOT", new Predicate("=", column, literal));
            default:
                return null;
        }
    }

    private static Literal getLiteral(Expr expr, DataType dataType) {
        if (!(expr instanceof LiteralExpr)) {
            return null;
        }

        LiteralExpr literalExpr = (LiteralExpr) expr;
        if (dataType instanceof BooleanType) {
            try {
                return Literal.ofBoolean(new BoolLiteral(literalExpr.getStringValue()).getValue());
            } catch (Exception e) {
                LOG.error("Failed to convert {} to boolean type", literalExpr);
                throw new StarRocksConnectorException("Failed to convert %s to boolean type", literalExpr);
            }
        } else if (dataType instanceof ByteType) {
            return Literal.ofByte((byte) ((IntLiteral) literalExpr).getValue());
        } else if (dataType instanceof ShortType) {
            return Literal.ofShort((short) ((IntLiteral) literalExpr).getValue());
        } else if (dataType instanceof IntegerType) {
            return Literal.ofInt((int) ((IntLiteral) literalExpr).getValue());
        } else if (dataType instanceof LongType) {
            return Literal.ofLong(((IntLiteral) literalExpr).getValue());
        } else if (dataType instanceof FloatType) {
            return Literal.ofFloat((float) ((FloatLiteral) literalExpr).getValue());
        } else if (dataType instanceof DoubleType) {
            return Literal.ofDouble(((FloatLiteral) literalExpr).getValue());
        } else if (dataType instanceof StringType) {
            return Literal.ofString(((StringLiteral) literalExpr).getUnescapedValue());
        } else if (dataType instanceof DateType) {
            return Literal.ofDate((int) (ChronoUnit.DAYS.between(LocalDate.EPOCH,
                    LocalDate.parse(literalExpr.getStringValue()))));
        } else if (dataType instanceof TimestampType) {
            return Literal.ofTimestamp(ChronoUnit.MICROS.between(LocalDate.EPOCH.atStartOfDay(),
                    LocalDateTime.parse(literalExpr.getStringValue(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
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

    private static class ExtractColumnName implements AstVisitor<String, Void> {
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
