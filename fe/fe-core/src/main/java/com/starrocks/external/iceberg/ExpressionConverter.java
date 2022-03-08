// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;

public class ExpressionConverter {

    public static UnboundPredicate toIcebergExpression(Expr expr) {
        if (!(expr instanceof BinaryPredicate)) {
            return null;
        }

        BinaryPredicate predicate = (BinaryPredicate) expr;
        Operation op = getIcebergOperator(predicate.getOp());
        if (op == null) {
            return null;
        }

        if (!(predicate.getChild(0) instanceof SlotRef)) {
            return null;
        }
        SlotRef ref = (SlotRef) predicate.getChild(0);

        if (!(predicate.getChild(1) instanceof LiteralExpr)) {
            return null;
        }
        LiteralExpr literal = (LiteralExpr) predicate.getChild(1);

        String colName = ref.getDesc().getColumn().getName();
        UnboundPredicate unboundPredicate = null;
        switch (literal.getType().getPrimitiveType()) {
            case BOOLEAN: {
                unboundPredicate = Expressions.predicate(op, colName,
                        ((BoolLiteral) literal).getValue());
                break;
            }
            case TINYINT:
            case SMALLINT:
            case INT: {
                unboundPredicate = Expressions.predicate(op, colName,
                        (int) ((IntLiteral) literal).getValue());
                break;
            }
            case BIGINT: {
                unboundPredicate = Expressions.predicate(op, colName,
                        ((IntLiteral) literal).getValue());
                break;
            }
            case FLOAT: {
                unboundPredicate = Expressions.predicate(op, colName,
                        (float) ((FloatLiteral) literal).getValue());
                break;
            }
            case DOUBLE: {
                unboundPredicate = Expressions.predicate(op, colName,
                        ((FloatLiteral) literal).getValue());
                break;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                unboundPredicate = Expressions.predicate(op, colName,
                        ((DecimalLiteral) literal).getValue());
                break;
            }
            case HLL:
            case VARCHAR:
            case CHAR: {
                unboundPredicate = Expressions.predicate(op, colName,
                        ((StringLiteral) literal).getUnescapedValue());
                break;
            }
            case DATE: {
                unboundPredicate = Expressions.predicate(op, colName,
                        Math.toIntExact(((DateLiteral) literal).toLocalDateTime()
                                .toLocalDate().toEpochDay()));
                break;
            }
            case DATETIME:
            default:
                break;
        }

        return unboundPredicate;
    }

    private static Operation getIcebergOperator(BinaryPredicate.Operator op) {
        switch (op) {
            case EQ: return Operation.EQ;
            case NE: return Operation.NOT_EQ;
            case LE: return Operation.LT_EQ;
            case GE: return Operation.GT_EQ;
            case LT: return Operation.LT;
            case GT: return Operation.GT;
            case EQ_FOR_NULL: return Operation.IS_NULL;
            default: return null;
        }
    }
}
