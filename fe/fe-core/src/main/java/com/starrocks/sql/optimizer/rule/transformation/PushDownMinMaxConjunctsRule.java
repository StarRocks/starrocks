// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class PushDownMinMaxConjunctsRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(PushDownMinMaxConjunctsRule.class);

    public static final PushDownMinMaxConjunctsRule HIVE_SCAN =
            new PushDownMinMaxConjunctsRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final PushDownMinMaxConjunctsRule HUDI_SCAN =
            new PushDownMinMaxConjunctsRule(OperatorType.LOGICAL_HUDI_SCAN);
    public static final PushDownMinMaxConjunctsRule ICEBERG_SCAN =
            new PushDownMinMaxConjunctsRule(OperatorType.LOGICAL_ICEBERG_SCAN);

    public PushDownMinMaxConjunctsRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_SCAN, Pattern.create(logicalOperatorType));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator operator = (LogicalScanOperator) input.getOp();

        try {
            computeMinMaxConjuncts(operator, context);
        } catch (Exception e) {
            LOG.warn("Remote scan min max conjuncts exception : " + e);
            throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
        }
        return Collections.emptyList();
    }

    private void computeMinMaxConjuncts(LogicalScanOperator operator, OptimizerContext context)
            throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        for (ScalarOperator scalarOperator : scanOperatorPredicates.getNonPartitionConjuncts()) {
            if (isSupportedMinMaxConjuncts(scalarOperator)) {
                addMinMaxConjuncts(scalarOperator, operator, context);
            }
        }
    }

    /**
     * Only conjuncts of the form <column> <op> <constant> and <column> in <constant> are supported,
     * and <op> must be one of LT, LE, GE, GT, or EQ.
     */
    private boolean isSupportedMinMaxConjuncts(ScalarOperator operator) {
        if (operator instanceof BinaryPredicateOperator) {
            ScalarOperator leftChild = operator.getChild(0);
            ScalarOperator rightChild = operator.getChild(1);
            if (!(leftChild.isColumnRef()) || !(rightChild.isConstantRef())) {
                return false;
            }
            return !((ConstantOperator) rightChild).isNull();
        } else if (operator instanceof InPredicateOperator) {
            if (!(operator.getChild(0).isColumnRef())) {
                return false;
            }
            if (((InPredicateOperator) operator).isNotIn()) {
                return false;
            }
            return ((InPredicateOperator) operator).allValuesMatch(ScalarOperator::isConstantRef)
                    && !((InPredicateOperator) operator).hasAnyNullValues();
        } else {
            return false;
        }
    }

    private void addMinMaxConjuncts(ScalarOperator scalarOperator, LogicalScanOperator operator,
                                    OptimizerContext context) throws AnalysisException {
        List<ScalarOperator> minMaxConjuncts = operator.getScanOperatorPredicates().getMinMaxConjuncts();
        if (scalarOperator instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) scalarOperator;
            ScalarOperator leftChild = binaryPredicateOperator.getChild(0);
            ScalarOperator rightChild = binaryPredicateOperator.getChild(1);
            if (binaryPredicateOperator.getBinaryType().isEqual()) {
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.LE,
                        leftChild, rightChild, operator, context));
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.GE,
                        leftChild, rightChild, operator, context));
            } else if (binaryPredicateOperator.getBinaryType().isRange()) {
                minMaxConjuncts.add(buildMinMaxConjunct(binaryPredicateOperator.getBinaryType(),
                        leftChild, rightChild, operator, context));
            }
        } else if (scalarOperator instanceof InPredicateOperator) {
            InPredicateOperator inPredicateOperator = (InPredicateOperator) scalarOperator;
            ConstantOperator max = null;
            ConstantOperator min = null;
            for (int i = 1; i < inPredicateOperator.getChildren().size(); ++i) {
                ConstantOperator child = (ConstantOperator) inPredicateOperator.getChild(i);
                if (min == null || child.compareTo(min) < 0) {
                    min = child;
                }
                if (max == null || child.compareTo(max) > 0) {
                    max = child;
                }
            }
            Preconditions.checkState(min != null);

            BinaryPredicateOperator minBound = buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.GE,
                    inPredicateOperator.getChild(0), min, operator, context);
            BinaryPredicateOperator maxBound = buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.LE,
                    inPredicateOperator.getChild(0), max, operator, context);
            minMaxConjuncts.add(minBound);
            minMaxConjuncts.add(maxBound);
        }
    }

    private BinaryPredicateOperator buildMinMaxConjunct(BinaryPredicateOperator.BinaryType type,
                                                        ScalarOperator left, ScalarOperator right,
                                                        LogicalScanOperator operator,
                                                        OptimizerContext context) throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ColumnRefOperator newColumnRef = context.getColumnRefFactory().create(left, left.getType(), left.isNullable());
        scanOperatorPredicates.getMinMaxColumnRefMap().put(newColumnRef, operator.getColRefToColumnMetaMap().get(left));
        return new BinaryPredicateOperator(type, newColumnRef, right);
    }
}
