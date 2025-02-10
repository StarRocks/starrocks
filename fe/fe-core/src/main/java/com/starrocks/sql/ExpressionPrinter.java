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

package com.starrocks.sql;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExpressionPrinter<C> extends ScalarOperatorVisitor<String, C> {

    public String print(ScalarOperator scalarOperator, C context) {
        return scalarOperator.accept(this, context);
    }

    public String print(ScalarOperator scalarOperator) {
        return scalarOperator.accept(this, null);
    }

    @Override
    public String visit(ScalarOperator scalarOperator, C context) {
        return scalarOperator.toString();
    }

    @Override
    public String visitConstant(ConstantOperator literal, C context) {
        if (literal.getType().isDatetime()) {
            LocalDateTime time = (LocalDateTime) Optional.ofNullable(literal.getValue()).orElse(LocalDateTime.MIN);
            if (time.getNano() > 0) {
                return String.format("%04d-%02d-%02d %02d:%02d:%02d.%6d",
                        time.getYear(), time.getMonthValue(), time.getDayOfMonth(),
                        time.getHour(), time.getMinute(), time.getSecond(), time.getNano() / 1000);
            } else {
                return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        time.getYear(), time.getMonthValue(), time.getDayOfMonth(),
                        time.getHour(), time.getMinute(), time.getSecond());
            }
        } else if (literal.getType().isDate()) {
            LocalDateTime time = (LocalDateTime) Optional.ofNullable(literal.getValue()).orElse(LocalDateTime.MIN);
            return String.format("%04d-%02d-%02d", time.getYear(), time.getMonthValue(), time.getDayOfMonth());
        } else if (literal.getType().isStringType()) {
            return "'" + literal.getValue() + "'";
        }

        return String.valueOf(literal.getValue());
    }

    @Override
    public String visitVariableReference(ColumnRefOperator variable, C context) {
        return variable.getId() + ":" + variable.getName();
    }

    @Override
    public String visitArray(ArrayOperator array, C context) {
        return array.getChildren().stream().map(p -> print(p, context)).collect(Collectors.joining(", "));
    }

    @Override
    public String visitCollectionElement(CollectionElementOperator collectSubOp, C context) {
        return collectSubOp.getChildren().stream().map(p -> print(p, context)).collect(Collectors.joining(", "));
    }

    @Override
    public String visitCall(CallOperator call, C context) {
        String fnName = call.getFnName();

        switch (fnName) {
            case FunctionSet.ADD:
                return print(call.getChild(0), context) + " + " + print(call.getChild(1), context);
            case FunctionSet.SUBTRACT:
                return print(call.getChild(0), context) + " - " + print(call.getChild(1), context);
            case FunctionSet.MULTIPLY:
                return print(call.getChild(0), context) + " * " + print(call.getChild(1), context);
            case FunctionSet.DIVIDE:
                return print(call.getChild(0), context) + " / " + print(call.getChild(1), context);
        }

        return fnName + "(" +
                call.getChildren().stream().map(p -> print(p, context)).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicateOperator predicate, C context) {
        StringBuilder sb = new StringBuilder();
        sb.append(print(predicate.getChild(0), context)).append(" ");

        if (predicate.isNotBetween()) {
            sb.append("NOT ");
        }

        sb.append("BETWEEN ");
        sb.append(predicate.getChild(1)).append(" AND ").append(predicate.getChild(2));
        return sb.toString();
    }

    @Override
    public String visitCloneOperator(CloneOperator operator, C context) {
        return "CLONE(" + print(operator.getChild(0), context) + ")";
    }

    @Override
    public String visitBinaryPredicate(BinaryPredicateOperator predicate, C context) {
        return print(predicate.getChild(0), context) + " " + predicate.getBinaryType().toString() + " " +
                print(predicate.getChild(1), context);
    }

    @Override
    public String visitCompoundPredicate(CompoundPredicateOperator predicate, C context) {
        if (CompoundPredicateOperator.CompoundType.NOT.equals(predicate.getCompoundType())) {
            return "NOT " + print(predicate.getChild(0), context);
        } else if (CompoundPredicateOperator.CompoundType.AND.equals(predicate.getCompoundType())) {

            String leftPredicate;
            if (predicate.getChild(0) instanceof CompoundPredicateOperator
                    && ((CompoundPredicateOperator) predicate.getChild(0)).getCompoundType().equals(
                    CompoundPredicateOperator.CompoundType.OR)) {
                leftPredicate = "(" + print(predicate.getChild(0), context) + ")";
            } else {
                leftPredicate = print(predicate.getChild(0), context);
            }

            String rightPredicate;
            if (predicate.getChild(1) instanceof CompoundPredicateOperator
                    && ((CompoundPredicateOperator) predicate.getChild(1)).getCompoundType().equals(
                    CompoundPredicateOperator.CompoundType.OR)) {
                rightPredicate = "(" + print(predicate.getChild(1), context) + ")";
            } else {
                rightPredicate = print(predicate.getChild(1), context);
            }

            return leftPredicate + " " + predicate.getCompoundType().toString() + " " + rightPredicate;
        } else {
            return print(predicate.getChild(0), context) + " " + predicate.getCompoundType().toString() + " " +
                    print(predicate.getChild(1), context);
        }
    }

    @Override
    public String visitExistsPredicate(ExistsPredicateOperator predicate, C context) {
        StringBuilder strBuilder = new StringBuilder();
        if (predicate.isNotExists()) {
            strBuilder.append("NOT ");

        }
        strBuilder.append("EXISTS ");
        strBuilder.append(print(predicate.getChild(0), context));
        return strBuilder.toString();
    }

    @Override
    public String visitInPredicate(InPredicateOperator predicate, C context) {
        StringBuilder sb = new StringBuilder();
        sb.append(print(predicate.getChild(0), context)).append(" ");
        if (predicate.isNotIn()) {
            sb.append("NOT ");
        }

        sb.append("IN (");
        sb.append(
                predicate.getChildren().stream().skip(1).map(p -> print(p, context)).collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitIsNullPredicate(IsNullPredicateOperator predicate, C context) {
        if (!predicate.isNotNull()) {
            return this.print(predicate.getChild(0), context) + " IS NULL";
        } else {
            return print(predicate.getChild(0), context) + " IS NOT NULL";
        }
    }

    @Override
    public String visitLikePredicateOperator(LikePredicateOperator predicate, C context) {
        if (LikePredicateOperator.LikeType.LIKE.equals(predicate.getLikeType())) {
            return print(predicate.getChild(0), context) + " LIKE " + print(predicate.getChild(1), context);
        }

        return print(predicate.getChild(0), context) + " REGEXP " + print(predicate.getChild(1), context);
    }

    @Override
    public String visitMatchExprOperator(MatchExprOperator predicate, C context) {
        return print(predicate.getChild(0), context) + " MATCH " + print(predicate.getChild(1), context);
    }

    @Override
    public String visitCastOperator(CastOperator operator, C context) {
        return "cast(" + print(operator.getChild(0), context) + " as " + operator.getType().toSql() + ")";
    }

    @Override
    public String visitCaseWhenOperator(CaseWhenOperator operator, C context) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CASE ");
        if (operator.hasCase()) {
            stringBuilder.append(print(operator.getCaseClause(), context)).append(" ");
        }

        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            stringBuilder.append("WHEN ").append(print(operator.getWhenClause(i), context))
                    .append(" ");
            stringBuilder.append("THEN ").append(print(operator.getThenClause(i), context))
                    .append(" ");
        }

        if (operator.hasElse()) {
            stringBuilder.append("ELSE ").append(print(operator.getElseClause(), context))
                    .append(" ");
        }

        stringBuilder.append("END");
        return stringBuilder.toString();
    }

    @Override
    public String visitDictMappingOperator(DictMappingOperator operator, C context) {
        return operator.toString();
    }
}
