// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.getEqConj;

public class OutputPropertyDeriver extends OperatorVisitor<PhysicalPropertySet, ExpressionContext> {
    private GroupExpression groupExpression;
    private PhysicalPropertySet requirements;
    // children best group expression
    private List<GroupExpression> childrenBestExprList;
    // required properties for children.
    private List<PhysicalPropertySet> requiredChildrenProperties;
    // children output property
    private List<PhysicalPropertySet> childrenOutputProperties;
    private double curTotalCost;
    private final OptimizerContext context;

    public OutputPropertyDeriver(TaskContext taskContext) {
        this.context = taskContext.getOptimizerContext();
    }

    public PhysicalPropertySet getOutputProperty(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression,
            List<GroupExpression> childrenBestExprList,
            List<PhysicalPropertySet> requiredChildrenProperties,
            List<PhysicalPropertySet> childrenOutputProperties) {
        this.requirements = requirements;
        this.groupExpression = groupExpression;
        this.childrenBestExprList = childrenBestExprList;
        this.requiredChildrenProperties = requiredChildrenProperties;
        this.childrenOutputProperties = childrenOutputProperties;

        return groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
    }

    public Pair<PhysicalPropertySet, Double> getOutputPropertyWithCost(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression,
            List<GroupExpression> childrenBestExprList,
            List<PhysicalPropertySet> requiredChildrenProperties,
            List<PhysicalPropertySet> childrenOutputProperties,
            double curTotalCost) {
        this.curTotalCost = curTotalCost;
        PhysicalPropertySet outputProperty =
                getOutputProperty(requirements, groupExpression, childrenBestExprList, requiredChildrenProperties,
                        childrenOutputProperties);
        return Pair.create(outputProperty, this.curTotalCost);
    }

    @Override
    public PhysicalPropertySet visitOperator(Operator node, ExpressionContext context) {
        return PhysicalPropertySet.EMPTY;
    }

    public PhysicalPropertySet computeColocateJoinOutputProperty(HashDistributionSpec leftScanDistributionSpec,
                                                                 HashDistributionSpec rightScanDistributionSpec,
                                                                 List<Integer> leftShuffleColumns,
                                                                 List<Integer> rightShuffleColumns) {
        DistributionSpec.PropertyInfo leftInfo = leftScanDistributionSpec.getPropertyInfo();
        DistributionSpec.PropertyInfo rightInfo = rightScanDistributionSpec.getPropertyInfo();

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

            DistributionSpec.PropertyInfo newPhysicalPropertyInfo = new DistributionSpec.PropertyInfo();

            newPhysicalPropertyInfo.tableId = leftTableId;
            HashDistributionDesc outputDesc;
            if (requiredShuffleDesc.get().getColumns().contains(rightShuffleColumns)) {
                outputDesc =
                        new HashDistributionDesc(rightShuffleColumns, HashDistributionDesc.SourceType.SHUFFLE_LOCAL);
            } else {
                outputDesc =
                        new HashDistributionDesc(leftShuffleColumns, HashDistributionDesc.SourceType.SHUFFLE_LOCAL);
            }
            return createPropertySetByDistribution(new HashDistributionSpec(outputDesc, newPhysicalPropertyInfo));
        }
    }

    public boolean canColocateJoin(HashDistributionSpec leftLocalDistributionSpec,
                                   HashDistributionSpec rightLocalDistributionSpec,
                                   List<Integer> leftShuffleColumns, List<Integer> rightShuffleColumns) {
        HashDistributionDesc leftLocalDistributionDesc = leftLocalDistributionSpec.getHashDistributionDesc();
        HashDistributionDesc rightLocalDistributionDesc = rightLocalDistributionSpec.getHashDistributionDesc();

        if (ConnectContext.get().getSessionVariable().isDisableColocateJoin()) {
            return false;
        }

        DistributionSpec.PropertyInfo leftInfo = leftLocalDistributionSpec.getPropertyInfo();
        DistributionSpec.PropertyInfo rightInfo = rightLocalDistributionSpec.getPropertyInfo();

        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        long leftTableId = leftInfo.tableId;
        long rightTableId = rightInfo.tableId;

        // join self
        if (leftTableId == rightTableId && !colocateIndex.isColocateTable(leftTableId)) {
            if (!leftInfo.isSinglePartition() || !rightInfo.isSinglePartition() ||
                    !leftInfo.partitionIds.equals(rightInfo.partitionIds)) {
                return false;
            }
        } else {
            // colocate group
            if (!colocateIndex.isSameGroup(leftTableId, rightTableId)) {
                return false;
            }

            ColocateTableIndex.GroupId groupId = colocateIndex.getGroup(leftTableId);
            if (colocateIndex.isGroupUnstable(groupId)) {
                return false;
            }

            Preconditions.checkState(leftLocalDistributionDesc.getColumns().size() ==
                    rightLocalDistributionDesc.getColumns().size());

            // check orders of predicate columns is right
            // check predicate columns is satisfy bucket hash columns
            for (int i = 0; i < leftLocalDistributionDesc.getColumns().size(); ++i) {
                int leftScanColumnId = leftLocalDistributionDesc.getColumns().get(i);
                int leftIndex = leftShuffleColumns.indexOf(leftScanColumnId);

                int rightScanColumnId = rightLocalDistributionDesc.getColumns().get(i);
                int rightIndex = rightShuffleColumns.indexOf(rightScanColumnId);

                if (leftIndex != rightIndex) {
                    return false;
                }
            }
        }
        return true;
    }

    private PhysicalPropertySet addChildEnforcer(PhysicalPropertySet oldOutputProperty,
                                                 DistributionProperty newDistributionProperty,
                                                 double childCost, Group childGroup) {
        PhysicalPropertySet newOutputProperty = oldOutputProperty.copy();
        newOutputProperty.setDistributionProperty(newDistributionProperty);
        GroupExpression enforcer = newDistributionProperty.appendEnforcers(childGroup);

        enforcer.setOutputPropertySatisfyRequiredProperty(newOutputProperty, newOutputProperty);
        updateChildCostWithEnforcer(enforcer, oldOutputProperty, newOutputProperty, childCost, childGroup);
        return newOutputProperty;
    }

    private void updateChildCostWithEnforcer(GroupExpression enforcer,
                                             PhysicalPropertySet oldOutputProperty,
                                             PhysicalPropertySet newOutputProperty,
                                             double childCost, Group childGroup) {
        context.getMemo().insertEnforceExpression(enforcer, childGroup);
        // update current total cost
        curTotalCost -= childCost;
        // add enforcer cost
        childCost += CostModel.calculateCost(enforcer);
        curTotalCost += childCost;

        enforcer.setPropertyWithCost(newOutputProperty, Lists.newArrayList(oldOutputProperty), childCost);
        childGroup.setBestExpression(enforcer, childCost, newOutputProperty);

        if (ConnectContext.get().getSessionVariable().isSetUseNthExecPlan()) {
            enforcer.addValidOutputInputProperties(newOutputProperty, Lists.newArrayList(PhysicalPropertySet.EMPTY));
            enforcer.getGroup().addSatisfyRequiredPropertyGroupExpression(newOutputProperty, enforcer);
        }
    }

    private void transToBucketShuffleJoin(HashDistributionDesc leftLocalDistributionDesc,
                                          List<Integer> leftShuffleColumns, List<Integer> rightShuffleColumns) {
        List<Integer> bucketShuffleColumns = Lists.newArrayList();
        for (int leftScanColumn : leftLocalDistributionDesc.getColumns()) {
            int index = leftShuffleColumns.indexOf(leftScanColumn);
            bucketShuffleColumns.add(rightShuffleColumns.get(index));
        }

        DistributionSpec rightDistributionSpec =
                DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(bucketShuffleColumns,
                        HashDistributionDesc.SourceType.BUCKET_JOIN));

        GroupExpression rightChild = childrenBestExprList.get(1);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);
        // enforce right child BUCKET_JOIN type distribution
        enforceChildDistribution(rightDistributionSpec, rightChild, rightChildOutputProperty);

        // update group expression require property
        PhysicalPropertySet newRightChildInputProperty = createPropertySetByDistribution(rightDistributionSpec);
        requiredChildrenProperties.set(1, newRightChildInputProperty);
    }

    // enforce child SHUFFLE type distribution
    private void enforceChildShuffleDistribution(List<Integer> shuffleColumns, GroupExpression child,
                                                 PhysicalPropertySet childOutputProperty, int childIndex) {
        DistributionSpec enforceDistributionSpec =
                DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(shuffleColumns,
                        HashDistributionDesc.SourceType.SHUFFLE_ENFORCE));

        enforceChildDistribution(enforceDistributionSpec, child, childOutputProperty);

        PhysicalPropertySet newChildInputProperty = createPropertySetByDistribution(enforceDistributionSpec);
        requiredChildrenProperties.set(childIndex, newChildInputProperty);
    }

    private void enforceChildDistribution(DistributionSpec distributionSpec, GroupExpression child,
                                          PhysicalPropertySet childOutputProperty) {
        double childCosts = child.getCost(childOutputProperty);
        if (child.getOp() instanceof PhysicalDistributionOperator) {
            // update child distribution directly
            PhysicalDistributionOperator rightChildOperator = (PhysicalDistributionOperator) child.getOp();
            rightChildOperator.setDistributionSpec(distributionSpec);
            // update child group expression low cost table
            PhysicalPropertySet newChildOutputProperty = createPropertySetByDistribution(distributionSpec);

            child.setPropertyWithCost(newChildOutputProperty,
                    child.getInputProperties(childOutputProperty), childCosts);
            child.getGroup()
                    .setBestExpression(child, childCosts, newChildOutputProperty);
            if (ConnectContext.get().getSessionVariable().isSetUseNthExecPlan()) {
                // record the output/input properties when child group could satisfy this group expression required property
                child.addValidOutputInputProperties(newChildOutputProperty,
                        child.getInputProperties(childOutputProperty));
                child.getGroup().addSatisfyRequiredPropertyGroupExpression(newChildOutputProperty, child);
            }
        } else {
            // add physical distribution operator
            addChildEnforcer(childOutputProperty, new DistributionProperty(distributionSpec),
                    childCosts, child.getGroup());
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

    // compute the hash join node output property, StarRocks support the colocate shuffle join, this join type
    // not only do child nodes need to satisfy a particular data distribution, but some additional checks are
    // required(see canColocateJoin). If can not satisfy, It needs to add enforcer to make sure it's an legitimate
    // plan. Hint is similar, with an extra enforcer
    // TODO(ywb): Guaranteed to be a legitimate plan before computing the output property
    @Override
    public PhysicalPropertySet visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);

        String hint = node.getJoinHint();
        GroupExpression leftChild = childrenBestExprList.get(0);
        GroupExpression rightChild = childrenBestExprList.get(1);

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

        DistributionProperty leftChildDistributionProperty = leftChildOutputProperty.getDistributionProperty();
        DistributionProperty rightChildDistributionProperty = rightChildOutputProperty.getDistributionProperty();
        if (leftChildDistributionProperty.isShuffle() && rightChildDistributionProperty.isShuffle()) {
            HashDistributionSpec leftDistributionSpec =
                    (HashDistributionSpec) leftChildOutputProperty.getDistributionProperty().getSpec();
            HashDistributionSpec rightDistributionSpec =
                    (HashDistributionSpec) rightChildOutputProperty.getDistributionProperty().getSpec();

            HashDistributionDesc leftDistributionDesc = leftDistributionSpec.getHashDistributionDesc();
            HashDistributionDesc rightDistributionDesc = rightDistributionSpec.getHashDistributionDesc();

            // 2.1 respect the hint
            if ("SHUFFLE".equalsIgnoreCase(hint)) {
                if (leftDistributionDesc.isLocalShuffle()) {
                    enforceChildShuffleDistribution(leftOnPredicateColumns, leftChild, leftChildOutputProperty, 0);
                }
                if (rightDistributionDesc.isLocalShuffle()) {
                    enforceChildShuffleDistribution(rightOnPredicateColumns, rightChild, rightChildOutputProperty, 1);
                }
                return computeHashJoinDistributionPropertyInfo(node,
                        computeShuffleJoinOutputProperty(leftOnPredicateColumns, rightOnPredicateColumns), context);
            }

            if (leftDistributionDesc.isLocalShuffle() && rightDistributionDesc.isLocalShuffle()) {
                // colocate join
                if (!"BUCKET".equalsIgnoreCase(hint) &&
                        canColocateJoin(leftDistributionSpec, rightDistributionSpec, leftOnPredicateColumns,
                                rightOnPredicateColumns)) {
                    return computeHashJoinDistributionPropertyInfo(node,
                            computeColocateJoinOutputProperty(leftDistributionSpec, rightDistributionSpec,
                                    leftOnPredicateColumns, rightOnPredicateColumns), context);
                } else {
                    transToBucketShuffleJoin(leftDistributionDesc, leftOnPredicateColumns, rightOnPredicateColumns);
                    return computeHashJoinDistributionPropertyInfo(node, leftChildOutputProperty, context);
                }
            } else if (leftDistributionDesc.isLocalShuffle() && rightDistributionDesc.isJoinShuffle()) {
                // bucket join
                transToBucketShuffleJoin(leftDistributionDesc, leftOnPredicateColumns, rightOnPredicateColumns);
                return computeHashJoinDistributionPropertyInfo(node, leftChildOutputProperty, context);
            } else if (leftDistributionDesc.isJoinShuffle() && rightDistributionDesc.isLocalShuffle()) {
                // coordinator can not bucket shuffle data from left to right, so we need to adjust to shuffle join
                enforceChildShuffleDistribution(rightOnPredicateColumns, rightChild, rightChildOutputProperty, 1);
                return computeHashJoinDistributionPropertyInfo(node,
                        computeShuffleJoinOutputProperty(leftOnPredicateColumns, rightOnPredicateColumns), context);
            } else if (leftDistributionDesc.isJoinShuffle() && rightDistributionDesc.isJoinShuffle()) {
                // shuffle join
                return computeHashJoinDistributionPropertyInfo(node,
                        computeShuffleJoinOutputProperty(leftOnPredicateColumns, rightOnPredicateColumns), context);
            } else {
                Preconditions.checkState(false, "Children output property distribution error");
                return PhysicalPropertySet.EMPTY;
            }
        } else {
            Preconditions.checkState(false, "Children output property distribution error");
            return PhysicalPropertySet.EMPTY;
        }
    }

    private PhysicalPropertySet computeShuffleJoinOutputProperty(List<Integer> leftOnPredicateColumns,
                                                                 List<Integer> rightOnPredicateColumns) {
        Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleJoinDesc();
        if (!requiredShuffleDesc.isPresent()) {
            return PhysicalPropertySet.EMPTY;
        }
        HashDistributionSpec leftShuffleDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(leftOnPredicateColumns, HashDistributionDesc.SourceType.SHUFFLE_JOIN));
        HashDistributionSpec rightShuffleDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(rightOnPredicateColumns, HashDistributionDesc.SourceType.SHUFFLE_JOIN));

        // TODO(ywb): check columns satisfy here instead of enforce phase because there need to compute equivalence columns，
        // we will do this later
        Preconditions.checkState(leftOnPredicateColumns.size() == rightOnPredicateColumns.size());
        // Hash shuffle columns must keep same
        List<Integer> requiredColumns = requiredShuffleDesc.get().getColumns();
        boolean checkLeft = leftOnPredicateColumns.containsAll(requiredColumns) &&
                leftOnPredicateColumns.size() == requiredColumns.size();
        boolean checkRight = rightOnPredicateColumns.containsAll(requiredColumns) &&
                rightOnPredicateColumns.size() == requiredColumns.size();

        // @Todo: Modify PlanFragmentBuilder to support complex query
        // Different joins maybe different on-clause predicate order, so the order of shuffle key is different,
        // and unfortunately PlanFragmentBuilder doesn't support adjust the order of join shuffle key,
        // so we must check the shuffle order strict
        if (checkLeft || checkRight) {
            for (int i = 0; i < requiredColumns.size(); i++) {
                checkLeft &= requiredColumns.get(i).equals(leftOnPredicateColumns.get(i));
                checkRight &= requiredColumns.get(i).equals(rightOnPredicateColumns.get(i));
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
                        HashDistributionDesc.SourceType.SHUFFLE_LOCAL), physicalPropertyInfo));
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
