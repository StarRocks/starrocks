// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ListPartitionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class HiveScanPartitionPruneRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(HiveScanPartitionPruneRule.class);

    public HiveScanPartitionPruneRule() {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(OperatorType.LOGICAL_HIVE_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalHiveScanOperator operator = (LogicalHiveScanOperator) input.getOp();
        if (operator.getTableType() == Table.TableType.HIVE) {
            // partitionColumnName -> (LiteralExpr -> partition ids)
            // no null partitions in this map, used by ListPartitionPruner
            Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
            // Store partitions with null partition values separately, used by ListPartitionPruner
            // partitionColumnName -> null partitionIds
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();

            try {
                initPartitionInfo(operator, columnToPartitionValuesMap, columnToNullPartitions, context);
                classifyConjuncts(operator, columnToPartitionValuesMap);
                computePartitionInfo(operator, columnToPartitionValuesMap, columnToNullPartitions, context);
            } catch (Exception e) {
                LOG.warn("Hive table partition prune failed : " + e);
                throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
            }
        }
        return Collections.emptyList();
    }

    private void initPartitionInfo(LogicalHiveScanOperator operator,
                                   Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                   Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
                                   OptimizerContext context) throws DdlException {
        HiveTable hiveTable = (HiveTable) operator.getTable();
        List<Column> partitionColumns = hiveTable.getPartitionColumns();
        List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
        for (Column column : partitionColumns) {
            ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
            columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
            columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
            partitionColumnRefOperators.add(partitionColumnRefOperator);
        }

        // no partition column table:
        // 1. partitionColumns is empty
        // 2. partitionKeys size = 1
        // 3. key.getKeys() is empty
        Map<PartitionKey, Long> partitionKeys = hiveTable.getPartitionKeys();
        for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
            PartitionKey key = entry.getKey();
            long partitionId = entry.getValue();
            List<LiteralExpr> literals = key.getKeys();
            for (int i = 0; i < literals.size(); i++) {
                ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(i);
                LiteralExpr literal = literals.get(i);
                if (Expr.IS_NULL_LITERAL.apply(literal)) {
                    columnToNullPartitions.get(columnRefOperator).add(partitionId);
                    continue;
                }

                Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                        .computeIfAbsent(literal, k -> Sets.newHashSet());
                partitions.add(partitionId);
            }
            operator.getIdToPartitionKey().put(partitionId, key);
        }
        LOG.debug("table: {}, partition values map: {}, null partition map: {}",
                hiveTable.getName(), columnToPartitionValuesMap, columnToNullPartitions);
    }

    private void classifyConjuncts(LogicalHiveScanOperator operator,
                                   Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap) {
        for (ScalarOperator scalarOperator : Utils.extractConjuncts(operator.getPredicate())) {
            List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(scalarOperator);
            if (!columnRefOperatorList.retainAll(columnToPartitionValuesMap.keySet())) {
                operator.getPartitionConjuncts().add(scalarOperator);
            } else {
                operator.getNonPartitionConjuncts().add(scalarOperator);
            }
        }
    }

    private void computePartitionInfo(LogicalHiveScanOperator operator,
                                      Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                      Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
                                      OptimizerContext context) throws AnalysisException {
        ListPartitionPruner partitionPruner =
                new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                        operator.getPartitionConjuncts());
        Collection<Long> selectedPartitionIds = partitionPruner.prune();
        if (selectedPartitionIds == null) {
            selectedPartitionIds = operator.getIdToPartitionKey().keySet();
        }
        operator.setSelectedPartitionIds(selectedPartitionIds);
        operator.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());

        computeMinMaxConjuncts(operator, context);
    }

    private void computeMinMaxConjuncts(LogicalHiveScanOperator operator, OptimizerContext context) {
        for (ScalarOperator scalarOperator : operator.getNonPartitionConjuncts()) {
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

    private void addMinMaxConjuncts(ScalarOperator scalarOperator, LogicalHiveScanOperator operator,
                                    OptimizerContext context) {
        List<ScalarOperator> minMaxConjuncts = operator.getMinMaxConjuncts();
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
                                                        LogicalHiveScanOperator operator,
                                                        OptimizerContext context) {
        ColumnRefOperator newColumnRef = context.getColumnRefFactory().create(left, left.getType(), left.isNullable());
        operator.getMinMaxColumnRefMap().put(newColumnRef, operator.getColRefToColumnMetaMap().get(left));
        return new BinaryPredicateOperator(type, newColumnRef, right);
    }
}
