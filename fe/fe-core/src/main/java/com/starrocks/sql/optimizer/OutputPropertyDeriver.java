// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// The output property of the node is calculated according to the attributes of the child node and itself.
// Currently join node enforces a valid property for the child node that cannot meet the requirements.
public class OutputPropertyDeriver extends PropertyDeriverBase<PhysicalPropertySet, ExpressionContext> {
    private PhysicalPropertySet requirements;
    // children output property
    private List<PhysicalPropertySet> childrenOutputProperties;

    public PhysicalPropertySet getOutputProperty(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression,
            List<PhysicalPropertySet> childrenOutputProperties) {
        this.requirements = requirements;
        // children best group expression
        this.childrenOutputProperties = childrenOutputProperties;

        return groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
    }

    public Pair<PhysicalPropertySet, Double> getOutputPropertyWithCost(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression,
            List<PhysicalPropertySet> childrenOutputProperties,
            double curTotalCost) {
        PhysicalPropertySet outputProperty = getOutputProperty(requirements, groupExpression, childrenOutputProperties);
        return Pair.create(outputProperty, curTotalCost);
    }

    @Override
    public PhysicalPropertySet visitOperator(Operator node, ExpressionContext context) {
        return PhysicalPropertySet.EMPTY;
    }

    private PhysicalPropertySet computeColocateJoinOutputProperty(HashDistributionSpec leftScanDistributionSpec,
                                                                  HashDistributionSpec rightScanDistributionSpec) {
        DistributionSpec.PropertyInfo leftInfo = leftScanDistributionSpec.getPropertyInfo();
        DistributionSpec.PropertyInfo rightInfo = rightScanDistributionSpec.getPropertyInfo();
        List<Integer> leftShuffleColumns = leftScanDistributionSpec.getShuffleColumns();

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
        long leftTableId = leftInfo.tableId;
        long rightTableId = rightInfo.tableId;

        if (leftTableId == rightTableId && !colocateIndex.isSameGroup(leftTableId, rightTableId)) {
            return createPropertySetByDistribution(leftScanDistributionSpec);
        } else {
            Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleDesc();
            if (!requiredShuffleDesc.isPresent()) {
                return createPropertySetByDistribution(leftScanDistributionSpec);
            }

            return createPropertySetByDistribution(
                    new HashDistributionSpec(
                            new HashDistributionDesc(leftShuffleColumns, HashDistributionDesc.SourceType.LOCAL),
                            leftScanDistributionSpec.getPropertyInfo()));
        }
    }

    // compute the distribution property info, just compute the nullable columns now
    private PhysicalPropertySet computeHashJoinDistributionPropertyInfo(PhysicalJoinOperator node,
                                                                        PhysicalPropertySet physicalPropertySet,
                                                                        List<Integer> leftOnPredicateColumns,
                                                                        List<Integer> rightOnPredicateColumns,
                                                                        ExpressionContext context) {
        DistributionSpec.PropertyInfo propertyInfo =
                physicalPropertySet.getDistributionProperty().getSpec().getPropertyInfo();

        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);
        if (node.getJoinType().isLeftOuterJoin()) {
            propertyInfo.nullableColumns.union(rightChildColumns);
        } else if (node.getJoinType().isRightOuterJoin()) {
            propertyInfo.nullableColumns.union(leftChildColumns);
        } else if (node.getJoinType().isFullOuterJoin()) {
            propertyInfo.nullableColumns.union(leftChildColumns);
            propertyInfo.nullableColumns.union(rightChildColumns);
        }

        if (node.getJoinType().isInnerJoin()) {
            for (int i = 0; i < leftOnPredicateColumns.size(); i++) {
                int leftColumn = leftOnPredicateColumns.get(i);
                int rightColumn = rightOnPredicateColumns.get(i);
                propertyInfo.addJoinEquivalentPair(leftColumn, rightColumn);
            }
        }

        return physicalPropertySet;
    }

    @Override
    public PhysicalPropertySet visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        return visitPhysicalJoin(node, context);
    }

    @Override
    public PhysicalPropertySet visitPhysicalMergeJoin(PhysicalMergeJoinOperator node, ExpressionContext context) {
        return visitPhysicalJoin(node, context);
    }

    @Override
    public PhysicalPropertySet visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator node, ExpressionContext context) {
        return childrenOutputProperties.get(0);
    }

    public PhysicalPropertySet visitPhysicalJoin(PhysicalJoinOperator node, ExpressionContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalPropertySet leftChildOutputProperty = childrenOutputProperties.get(0);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);

        // 1. Distribution is broadcast
        if (rightChildOutputProperty.getDistributionProperty().isBroadcast()) {
            return computeHashJoinDistributionPropertyInfo(node, leftChildOutputProperty, Collections.emptyList(),
                    Collections.emptyList(), context);
        }
        // 2. Distribution is shuffle
        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);
        JoinHelper joinHelper = JoinHelper.of(node, leftChildColumns, rightChildColumns);

        List<Integer> leftOnPredicateColumns = joinHelper.getLeftOnColumns();
        List<Integer> rightOnPredicateColumns = joinHelper.getRightOnColumns();
        Preconditions.checkState(leftOnPredicateColumns.size() == rightOnPredicateColumns.size());
        // Get required properties for children.
        List<PhysicalPropertySet> requiredProperties =
                computeShuffleJoinRequiredProperties(requirements, leftOnPredicateColumns, rightOnPredicateColumns);
        Preconditions.checkState(requiredProperties.size() == 2);
        List<Integer> leftShuffleColumns =
                ((HashDistributionSpec) requiredProperties.get(0).getDistributionProperty().getSpec())
                        .getShuffleColumns();

        DistributionProperty leftChildDistributionProperty = leftChildOutputProperty.getDistributionProperty();
        DistributionProperty rightChildDistributionProperty = rightChildOutputProperty.getDistributionProperty();
        if (leftChildDistributionProperty.isShuffle() && rightChildDistributionProperty.isShuffle()) {
            HashDistributionSpec leftDistributionSpec =
                    (HashDistributionSpec) leftChildOutputProperty.getDistributionProperty().getSpec();
            HashDistributionSpec rightDistributionSpec =
                    (HashDistributionSpec) rightChildOutputProperty.getDistributionProperty().getSpec();

            HashDistributionDesc leftDistributionDesc = leftDistributionSpec.getHashDistributionDesc();
            HashDistributionDesc rightDistributionDesc = rightDistributionSpec.getHashDistributionDesc();

            if (leftDistributionDesc.isLocal() && rightDistributionDesc.isLocal()) {
                // colocate join
                return computeHashJoinDistributionPropertyInfo(node,
                        computeColocateJoinOutputProperty(leftDistributionSpec, rightDistributionSpec),
                        leftOnPredicateColumns,
                        rightOnPredicateColumns, context);
            } else if (leftDistributionDesc.isLocal() && rightDistributionDesc.isBucketJoin()) {
                // bucket join
                return computeHashJoinDistributionPropertyInfo(node, leftChildOutputProperty, leftOnPredicateColumns,
                        rightOnPredicateColumns, context);
            } else if ((leftDistributionDesc.isShuffle() || leftDistributionDesc.isShuffleEnforce()) &&
                    (rightDistributionDesc.isShuffle()) || rightDistributionDesc.isShuffleEnforce()) {
                // shuffle join
                return computeHashJoinDistributionPropertyInfo(node,
                        computeShuffleJoinOutputProperty(leftDistributionDesc.getColumns()),
                        leftOnPredicateColumns,
                        rightOnPredicateColumns, context);
            } else if (leftDistributionDesc.isShuffle() && rightDistributionDesc.isLocal()) {
                // coordinator can not bucket shuffle data from left to right
                Preconditions.checkState(false, "Children output property distribution error");
                return PhysicalPropertySet.EMPTY;
            } else {
                Preconditions.checkState(false, "Children output property distribution error");
                return PhysicalPropertySet.EMPTY;
            }
        } else {
            Preconditions.checkState(false, "Children output property distribution error");
            return PhysicalPropertySet.EMPTY;
        }
    }

    private PhysicalPropertySet computeShuffleJoinOutputProperty(List<Integer> leftShuffleColumns) {
        Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleDesc();
        if (!requiredShuffleDesc.isPresent()) {
            return PhysicalPropertySet.EMPTY;
        }
        HashDistributionSpec leftShuffleDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(leftShuffleColumns, HashDistributionDesc.SourceType.SHUFFLE_JOIN));

        return createPropertySetByDistribution(leftShuffleDistribution);
    }

    private Optional<HashDistributionDesc> getRequiredShuffleDesc() {
        if (!requirements.getDistributionProperty().isShuffle()) {
            return Optional.empty();
        }

        HashDistributionDesc requireDistributionDesc =
                ((HashDistributionSpec) requirements.getDistributionProperty().getSpec()).getHashDistributionDesc();

        if (HashDistributionDesc.SourceType.SHUFFLE_JOIN.equals(requireDistributionDesc.getSourceType()) ||
                HashDistributionDesc.SourceType.SHUFFLE_AGG.equals(requireDistributionDesc.getSourceType())) {
            return Optional.of(requireDistributionDesc);
        }

        return Optional.empty();
    }

    @Override
    public PhysicalPropertySet visitPhysicalProject(PhysicalProjectOperator node, ExpressionContext context) {
        Preconditions.checkState(this.childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalHashAggregate(PhysicalHashAggregateOperator node,
                                                          ExpressionContext context) {
        Preconditions.checkState(this.childrenOutputProperties.size() == 1);
        // The child node meets the distribution attribute requirements for hash Aggregate nodes.
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalRepeat(PhysicalRepeatOperator node, ExpressionContext context) {
        Preconditions.checkState(this.childrenOutputProperties.size() == 1);

        List<ColumnRefOperator> subRefs = Lists.newArrayList(node.getRepeatColumnRef().get(0));
        node.getRepeatColumnRef().forEach(subRefs::retainAll);
        Set<ColumnRefOperator> allGroupingRefs = Sets.newHashSet();

        node.getRepeatColumnRef().forEach(allGroupingRefs::addAll);
        subRefs.forEach(allGroupingRefs::remove);

        DistributionSpec.PropertyInfo propertyInfo =
                childrenOutputProperties.get(0).getDistributionProperty().getSpec().getPropertyInfo();
        propertyInfo.nullableColumns.union(allGroupingRefs);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
        HashDistributionSpec olapDistributionSpec = node.getDistributionSpec();

        DistributionSpec.PropertyInfo physicalPropertyInfo = new DistributionSpec.PropertyInfo();

        physicalPropertyInfo.tableId = node.getTable().getId();
        physicalPropertyInfo.partitionIds = node.getSelectedPartitionId();

        return createPropertySetByDistribution(new HashDistributionSpec(
                new HashDistributionDesc(olapDistributionSpec.getShuffleColumns(),
                        HashDistributionDesc.SourceType.LOCAL), physicalPropertyInfo));
    }

    @Override
    public PhysicalPropertySet visitPhysicalTopN(PhysicalTopNOperator topN, ExpressionContext context) {
        PhysicalPropertySet outputProperty;
        if (topN.getSortPhase().isFinal()) {
            if (topN.isSplit()) {
                DistributionSpec distributionSpec = DistributionSpec.createGatherDistributionSpec();
                DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
                SortProperty sortProperty = new SortProperty(topN.getOrderSpec());
                outputProperty = new PhysicalPropertySet(distributionProperty, sortProperty);
            } else {
                outputProperty = new PhysicalPropertySet(new SortProperty(topN.getOrderSpec()));
            }
        } else {
            outputProperty = PhysicalPropertySet.EMPTY;
        }
        return outputProperty;
    }

    @Override
    public PhysicalPropertySet visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        List<Integer> partitionColumnRefSet = new ArrayList<>();

        node.getPartitionExpressions().forEach(e -> partitionColumnRefSet.addAll(
                Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList())));

        SortProperty sortProperty = new SortProperty(new OrderSpec(node.getEnforceOrderBy()));

        DistributionProperty distributionProperty;
        if (partitionColumnRefSet.isEmpty()) {
            distributionProperty = new DistributionProperty(DistributionSpec.createGatherDistributionSpec());
        } else {
            // Use child distribution
            distributionProperty = childrenOutputProperties.get(0).getDistributionProperty();
        }
        return new PhysicalPropertySet(distributionProperty, sortProperty);
    }

    @Override
    public PhysicalPropertySet visitPhysicalFilter(PhysicalFilterOperator node, ExpressionContext context) {
        Preconditions.checkState(this.childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalTableFunction(PhysicalTableFunctionOperator node,
                                                          ExpressionContext context) {
        Preconditions.checkState(this.childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        DistributionSpec gather = DistributionSpec.createGatherDistributionSpec();
        return createPropertySetByDistribution(gather);
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        return childrenOutputProperties.get(1);
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, ExpressionContext context) {
        return PhysicalPropertySet.EMPTY;
    }

    @Override
    public PhysicalPropertySet visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        return PhysicalPropertySet.EMPTY;
    }

    @Override
    public PhysicalPropertySet visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, ExpressionContext context) {
        return createGatherPropertySet();
    }

    @Override
    public PhysicalPropertySet visitPhysicalMysqlScan(PhysicalMysqlScanOperator node, ExpressionContext context) {
        return createGatherPropertySet();
    }

    @Override
    public PhysicalPropertySet visitPhysicalJDBCScan(PhysicalJDBCScanOperator node, ExpressionContext context) {
        return createGatherPropertySet();
    }

    @Override
    public PhysicalPropertySet visitPhysicalValues(PhysicalValuesOperator node, ExpressionContext context) {
        return createGatherPropertySet();
    }
}
