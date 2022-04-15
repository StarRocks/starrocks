// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.getEqConj;

// The output property of the node is calculated according to the attributes of the child node and itself.
// Currently join node enforces a valid property for the child node that cannot meet the requirements.
public class OutputPropertyDeriver extends OperatorVisitor<PhysicalPropertySet, ExpressionContext> {
    private PhysicalPropertySet requirements;
    // children output property
    private List<PhysicalPropertySet> childrenOutputProperties;

    public PhysicalPropertySet getOutputProperty(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression,
            List<GroupExpression> childrenBestExprList,
            List<PhysicalPropertySet> childrenOutputProperties) {
        this.requirements = requirements;
        // children best group expression
        this.childrenOutputProperties = childrenOutputProperties;

        return groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
    }

    public Pair<PhysicalPropertySet, Double> getOutputPropertyWithCost(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression,
            List<GroupExpression> childrenBestExprList,
            List<PhysicalPropertySet> childrenOutputProperties,
            double curTotalCost) {
        PhysicalPropertySet outputProperty =
                getOutputProperty(requirements, groupExpression, childrenBestExprList, childrenOutputProperties);
        return Pair.create(outputProperty, curTotalCost);
    }

    @Override
    public PhysicalPropertySet visitOperator(Operator node, ExpressionContext context) {
        return PhysicalPropertySet.EMPTY;
    }

    public PhysicalPropertySet computeColocateJoinOutputProperty(HashDistributionSpec leftScanDistributionSpec,
                                                                 HashDistributionSpec rightScanDistributionSpec) {
        DistributionSpec.PropertyInfo leftInfo = leftScanDistributionSpec.getPropertyInfo();
        DistributionSpec.PropertyInfo rightInfo = rightScanDistributionSpec.getPropertyInfo();
        List<Integer> leftShuffleColumns = leftScanDistributionSpec.getShuffleColumns();
        List<Integer> rightShuffleColumns = rightScanDistributionSpec.getShuffleColumns();

        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        long leftTableId = leftInfo.tableId;
        long rightTableId = rightInfo.tableId;

        if (leftTableId == rightTableId && !colocateIndex.isSameGroup(leftTableId, rightTableId)) {
            return createPropertySetByDistribution(leftScanDistributionSpec);
        } else {
            // TODO(ywb): check columns satisfy here instead of enforce phase because there need to compute equivalence
            //  columns，we will do this later
            Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleJoinDesc();
            if (!requiredShuffleDesc.isPresent()) {
                return createPropertySetByDistribution(leftScanDistributionSpec);
            }

            DistributionSpec.PropertyInfo newPhysicalPropertyInfo;
            HashDistributionDesc outputDesc;
            if (requiredShuffleDesc.get().getColumns().containsAll(rightShuffleColumns)) {
                outputDesc =
                        new HashDistributionDesc(rightShuffleColumns, HashDistributionDesc.SourceType.LOCAL);
                newPhysicalPropertyInfo = rightScanDistributionSpec.getPropertyInfo();
            } else {
                outputDesc =
                        new HashDistributionDesc(leftShuffleColumns, HashDistributionDesc.SourceType.LOCAL);
                newPhysicalPropertyInfo = leftScanDistributionSpec.getPropertyInfo();
            }
            return createPropertySetByDistribution(new HashDistributionSpec(outputDesc, newPhysicalPropertyInfo));
        }
    }

    // compute the distribution property info, just compute the nullable columns now
    public PhysicalPropertySet computeHashJoinDistributionPropertyInfo(PhysicalHashJoinOperator node,
                                                                       PhysicalPropertySet physicalPropertySet,
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
        return physicalPropertySet;
    }

    @Override
    public PhysicalPropertySet visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalPropertySet leftChildOutputProperty = childrenOutputProperties.get(0);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);

        // 1. Distribution is broadcast
        if (rightChildOutputProperty.getDistributionProperty().isBroadcast()) {
            return computeHashJoinDistributionPropertyInfo(node, leftChildOutputProperty, context);
        }
        // 2. Distribution is shuffle
        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);
        List<BinaryPredicateOperator> equalOnPredicate =
                getEqConj(leftChildColumns, rightChildColumns, Utils.extractConjuncts(node.getOnPredicate()));

        List<Integer> leftOnPredicateColumns = new ArrayList<>();
        List<Integer> rightOnPredicateColumns = new ArrayList<>();
        JoinPredicateUtils.getJoinOnPredicatesColumns(equalOnPredicate, leftChildColumns, rightChildColumns,
                leftOnPredicateColumns, rightOnPredicateColumns);
        Preconditions.checkState(leftOnPredicateColumns.size() == rightOnPredicateColumns.size());
        // Get required properties for children.
        List<PhysicalPropertySet> requiredProperties =
                Utils.computeShuffleJoinRequiredProperties(requirements, leftOnPredicateColumns,
                        rightOnPredicateColumns);
        Preconditions.checkState(requiredProperties.size() == 2);
        List<Integer> leftShuffleColumns =
                ((HashDistributionSpec) requiredProperties.get(0).getDistributionProperty().getSpec())
                        .getShuffleColumns();
        List<Integer> rightShuffleColumns =
                ((HashDistributionSpec) requiredProperties.get(1).getDistributionProperty().getSpec())
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

            if (leftDistributionDesc.isLocalShuffle() && rightDistributionDesc.isLocalShuffle()) {
                // colocate join
                return computeHashJoinDistributionPropertyInfo(node,
                        computeColocateJoinOutputProperty(leftDistributionSpec, rightDistributionSpec), context);
            } else if (leftDistributionDesc.isLocalShuffle() && rightDistributionDesc.isBucketJoin()) {
                // bucket join
                return computeHashJoinDistributionPropertyInfo(node, leftChildOutputProperty, context);
            } else if ((leftDistributionDesc.isJoinShuffle() || leftDistributionDesc.isShuffleEnforce()) &&
                    (rightDistributionDesc.isJoinShuffle()) || rightDistributionDesc.isShuffleEnforce()) {
                // shuffle join
                return computeHashJoinDistributionPropertyInfo(node,
                        computeShuffleJoinOutputProperty(leftShuffleColumns, rightShuffleColumns), context);
            } else if (leftDistributionDesc.isJoinShuffle() && rightDistributionDesc.isLocalShuffle()) {
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

    private PhysicalPropertySet computeShuffleJoinOutputProperty(List<Integer> leftShuffleColumns,
                                                                 List<Integer> rightShuffleColumns) {
        Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleJoinDesc();
        if (!requiredShuffleDesc.isPresent()) {
            return PhysicalPropertySet.EMPTY;
        }
        HashDistributionSpec leftShuffleDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(leftShuffleColumns, HashDistributionDesc.SourceType.SHUFFLE_JOIN));
        HashDistributionSpec rightShuffleDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(rightShuffleColumns, HashDistributionDesc.SourceType.SHUFFLE_JOIN));

        // TODO(ywb): check columns satisfy here instead of enforce phase because there need to compute equivalence columns，
        // we will do this later
        Preconditions.checkState(leftShuffleColumns.size() == rightShuffleColumns.size());
        // Hash shuffle columns must keep same
        List<Integer> requiredColumns = requiredShuffleDesc.get().getColumns();
        boolean checkLeft = leftShuffleColumns.containsAll(requiredColumns) &&
                leftShuffleColumns.size() == requiredColumns.size();
        boolean checkRight = rightShuffleColumns.containsAll(requiredColumns) &&
                rightShuffleColumns.size() == requiredColumns.size();

        if (checkLeft || checkRight) {
            for (int i = 0; i < requiredColumns.size(); i++) {
                checkLeft &= requiredColumns.get(i).equals(leftShuffleColumns.get(i));
                checkRight &= requiredColumns.get(i).equals(rightShuffleColumns.get(i));
            }
        }

        if (checkLeft) {
            return createPropertySetByDistribution(leftShuffleDistribution);
        } else if (checkRight) {
            return createPropertySetByDistribution(rightShuffleDistribution);
        } else {
            return PhysicalPropertySet.EMPTY;
        }
    }

    private Optional<HashDistributionDesc> getRequiredShuffleJoinDesc() {
        if (!requirements.getDistributionProperty().isShuffle()) {
            return Optional.empty();
        }

        HashDistributionDesc requireDistributionDesc =
                ((HashDistributionSpec) requirements.getDistributionProperty().getSpec()).getHashDistributionDesc();
        if (!HashDistributionDesc.SourceType.SHUFFLE_JOIN.equals(requireDistributionDesc.getSourceType())) {
            return Optional.empty();
        }

        return Optional.of(requireDistributionDesc);
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
        allGroupingRefs.removeAll(subRefs);

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

        if (node.canDoReplicatedJoin()) {
            physicalPropertyInfo.isReplicate = true;
        }

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

        node.getPartitionExpressions().forEach(e -> {
            partitionColumnRefSet
                    .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList()));
        });

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
        return createLimitGatherProperty(node.getLimit());
    }

    @Override
    public PhysicalPropertySet visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        DistributionSpec gather = DistributionSpec.createGatherDistributionSpec();
        return createPropertySetByDistribution(gather);
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        return requirements;
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
        return requirements;
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, ExpressionContext context) {
        return PhysicalPropertySet.EMPTY;
    }

    @Override
    public PhysicalPropertySet visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        return PhysicalPropertySet.EMPTY;
    }

    private PhysicalPropertySet createLimitGatherProperty(long limit) {
        DistributionSpec distributionSpec = DistributionSpec.createGatherDistributionSpec(limit);
        DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
        return new PhysicalPropertySet(distributionProperty, SortProperty.EMPTY);
    }

    private PhysicalPropertySet createPropertySetByDistribution(DistributionSpec distributionSpec) {
        DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
        return new PhysicalPropertySet(distributionProperty);
    }
}
