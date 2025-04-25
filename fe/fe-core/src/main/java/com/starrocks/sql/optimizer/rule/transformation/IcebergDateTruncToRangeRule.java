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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Transforms date_trunc equality predicates into range expressions specifically for Iceberg tables.
 *
 * For example:
 *   date_trunc('day', column) = '2023-02-26'
 * becomes:
 *   column >= '2023-02-26 00:00:00' AND column < '2023-02-27 00:00:00'
 *
 * This enables Iceberg to use its native partition pruning capabilities with StarRocks' date_trunc function.
 */
public class IcebergDateTruncToRangeRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(IcebergDateTruncToRangeRule.class);

    public static final IcebergDateTruncToRangeRule INSTANCE = new IcebergDateTruncToRangeRule(
            RuleType.TF_ICEBERG_DATE_TRUNC_TO_RANGE,
            Pattern.create(OperatorType.LOGICAL_FILTER)
                    .addChildren(Pattern.create(OperatorType.LOGICAL_SCAN)));

    public IcebergDateTruncToRangeRule() {
        this(RuleType.TF_ICEBERG_DATE_TRUNC_TO_RANGE,
             Pattern.create(OperatorType.LOGICAL_FILTER)
                     .addChildren(Pattern.create(OperatorType.LOGICAL_SCAN)));
    }

    public IcebergDateTruncToRangeRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // First check if it's a filter applied to a scan
        if (!(input.getOp() instanceof LogicalFilterOperator) ||
                !(input.getInputs().get(0).getOp() instanceof LogicalScanOperator)) {
            return false;
        }

        // Check if it's an Iceberg table
        LogicalScanOperator scanOp = (LogicalScanOperator) input.getInputs().get(0).getOp();
        Table table = scanOp.getTable();
        if (table == null || !table.isIcebergTable()) {
            return false;
        }

        // Check if predicate contains any date_trunc calls
        LogicalFilterOperator filterOperator = (LogicalFilterOperator) input.getOp();
        ScalarOperator predicate = filterOperator.getPredicate();
        return containsDateTrunc(predicate);
    }

    private boolean containsDateTrunc(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binOp = (BinaryPredicateOperator) predicate;
            if (binOp.getBinaryType() == BinaryType.EQ &&
                    binOp.getChild(0) instanceof CallOperator &&
                    ((CallOperator) binOp.getChild(0)).getFnName().equals(FunctionSet.DATE_TRUNC)) {
                return true;
            }
        } else if (predicate.getChildren().size() > 0) {
            for (ScalarOperator child : predicate.getChildren()) {
                if (containsDateTrunc(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOperator = (LogicalFilterOperator) input.getOp();
        ScalarOperator predicate = filterOperator.getPredicate();

        // Apply transformation to the predicate
        ScalarOperator newPredicate = predicate.accept(new DateTruncTransformer(), null);

        if (newPredicate != predicate) {
            LogicalFilterOperator newFilter = new LogicalFilterOperator(newPredicate);
            OptExpression result = OptExpression.create(newFilter, input.getInputs());
            LOG.debug("Transformed date_trunc predicate for Iceberg table: {} -> {}",
                     predicate, newPredicate);
            return Lists.newArrayList(result);
        }

        return Lists.newArrayList(input);
    }

    /**
     * Visitor that transforms date_trunc equality predicates to range expressions
     */
    private static class DateTruncTransformer extends ScalarOperatorVisitor<ScalarOperator, Void> {
        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator operator, Void context) {
            if (operator.getBinaryType() == BinaryType.EQ &&
                    operator.getChild(0) instanceof CallOperator &&
                    ((CallOperator) operator.getChild(0)).getFnName().equals(FunctionSet.DATE_TRUNC) &&
                    operator.getChild(1) instanceof ConstantOperator) {

                CallOperator dateTruncCall = (CallOperator) operator.getChild(0);
                ConstantOperator dateValue = (ConstantOperator) operator.getChild(1);

                ScalarOperator transformed = transformDateTruncPredicate(dateTruncCall, dateValue);
                if (transformed != null) {
                    return transformed;
                }
            }

            return operator;
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator operator, Void context) {
            List<ScalarOperator> newChildren = Lists.newArrayList();
            boolean childrenChanged = false;

            for (ScalarOperator child : operator.getChildren()) {
                ScalarOperator newChild = child.accept(this, context);
                newChildren.add(newChild);
                childrenChanged |= (newChild != child);
            }

            if (childrenChanged) {
                return new CompoundPredicateOperator(operator.getCompoundType(), newChildren);
            }
            return operator;
        }

        @Override
        public ScalarOperator visit(ScalarOperator operator, Void context) {
            if (operator.getChildren().isEmpty()) {
                return operator;
            }

            List<ScalarOperator> newChildren = Lists.newArrayList();
            boolean childrenChanged = false;

            for (ScalarOperator child : operator.getChildren()) {
                ScalarOperator newChild = child.accept(this, context);
                newChildren.add(newChild);
                childrenChanged |= (newChild != child);
            }

            if (childrenChanged) {
                return operator.withChildren(newChildren);
            }
            return operator;
        }
    }

    /**
     * Transforms a date_trunc equality predicate into a range expression.
     */
    private static ScalarOperator transformDateTruncPredicate(CallOperator dateTruncCall, ConstantOperator dateValue) {
        if (!(dateTruncCall.getChild(0) instanceof ConstantOperator) ||
                !(dateTruncCall.getChild(1) instanceof ColumnRefOperator)) {
            return null;
        }

        ConstantOperator dateUnit = (ConstantOperator) dateTruncCall.getChild(0);
        ColumnRefOperator columnRef = (ColumnRefOperator) dateTruncCall.getChild(1);

        if (!dateUnit.isConstantNull() && !dateValue.isConstantNull()) {
            String unit = dateUnit.getVarchar().toLowerCase();
            LocalDateTime truncatedDate = dateValue.getDatetime();
            LocalDateTime nextDate;

            // Determine end of range based on date unit
            switch (unit) {
                case "day":
                    nextDate = truncatedDate.plusDays(1);
                    break;
                case "month":
                    nextDate = truncatedDate.plusMonths(1);
                    break;
                case "year":
                    nextDate = truncatedDate.plusYears(1);
                    break;
                case "quarter":
                    // Quarter is 3 months
                    nextDate = truncatedDate.plusMonths(3);
                    break;
                case "week":
                    nextDate = truncatedDate.plusWeeks(1);
                    break;
                case "hour":
                    nextDate = truncatedDate.plusHours(1);
                    break;
                case "minute":
                    nextDate = truncatedDate.plusMinutes(1);
                    break;
                case "second":
                    nextDate = truncatedDate.plusSeconds(1);
                    break;
                default:
                    return null; // Unsupported date truncation unit
            }

            boolean isDate = columnRef.getType().isDate();

            ConstantOperator startConstant = isDate ?
                    ConstantOperator.createDate(truncatedDate) :
                    ConstantOperator.createDatetime(truncatedDate);

            ConstantOperator endConstant = isDate ?
                    ConstantOperator.createDate(nextDate) :
                    ConstantOperator.createDatetime(nextDate);

            BinaryPredicateOperator leftPredicate =
                    new BinaryPredicateOperator(BinaryType.GE, columnRef, startConstant);

            BinaryPredicateOperator rightPredicate =
                    new BinaryPredicateOperator(BinaryType.LT, columnRef, endConstant);

            return Utils.compoundAnd(leftPredicate, rightPredicate);
        }

        return null;
    }
}
