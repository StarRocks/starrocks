// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.getEqConj;

public class ChildOutputPropertyGuarantor extends OperatorVisitor<Void, ExpressionContext>  {
    private PhysicalPropertySet requirements;
    // children best group expression
    private List<GroupExpression> childrenBestExprList;
    // required properties for children.
    private List<PhysicalPropertySet> requiredChildrenProperties;
    // children output property
    private List<PhysicalPropertySet> childrenOutputProperties;
    private double curTotalCost;
    private final OptimizerContext context;

    public ChildOutputPropertyGuarantor(TaskContext taskContext) {
        this.context = taskContext.getOptimizerContext();
    }

    public double enforceLegalChildOutputProperty(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression,
            List<GroupExpression> childrenBestExprList,
            List<PhysicalPropertySet> requiredChildrenProperties,
            List<PhysicalPropertySet> childrenOutputProperties,
            double curTotalCost) {
        this.requirements = requirements;
        this.childrenBestExprList = childrenBestExprList;
        this.requiredChildrenProperties = requiredChildrenProperties;
        this.childrenOutputProperties = childrenOutputProperties;
        this.curTotalCost = curTotalCost;

        groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        return this.curTotalCost;
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        return null;
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
        }
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

        return true;
    }

    private PhysicalPropertySet createPropertySetByDistribution(DistributionSpec distributionSpec) {
        DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
        return new PhysicalPropertySet(distributionProperty);
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
        childrenOutputProperties.set(childIndex, newChildInputProperty);
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
        childrenOutputProperties.set(1, newRightChildInputProperty);
    }

    private void enforceChildDistribution(DistributionSpec distributionSpec, GroupExpression child,
                                          PhysicalPropertySet childOutputProperty) {
        double childCosts = child.getCost(childOutputProperty);
        Group childGroup = child.getGroup();
        if (child.getOp() instanceof PhysicalDistributionOperator) {
            DistributionProperty newDistributionProperty = new DistributionProperty(distributionSpec);
            PhysicalPropertySet newOutputProperty = new PhysicalPropertySet(newDistributionProperty);
            GroupExpression enforcer = newDistributionProperty.appendEnforcers(childGroup);
            enforcer.setOutputPropertySatisfyRequiredProperty(newOutputProperty, newOutputProperty);
            context.getMemo().insertEnforceExpression(enforcer, childGroup);

            enforcer.updatePropertyWithCost(newOutputProperty, child.getInputProperties(childOutputProperty), childCosts);
            childGroup.setBestExpression(enforcer, childCosts, newOutputProperty);

            if (ConnectContext.get().getSessionVariable().isSetUseNthExecPlan()) {
                enforcer.addValidOutputInputProperties(newOutputProperty, Lists.newArrayList(PhysicalPropertySet.EMPTY));
                enforcer.getGroup().addSatisfyRequiredPropertyGroupExpression(newOutputProperty, enforcer);
            }
        } else {
            // add physical distribution operator
            addChildEnforcer(childOutputProperty, new DistributionProperty(distributionSpec),
                    childCosts, child.getGroup());
        }
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

        enforcer.updatePropertyWithCost(newOutputProperty, Lists.newArrayList(oldOutputProperty), childCost);
        childGroup.setBestExpression(enforcer, childCost, newOutputProperty);

        if (ConnectContext.get().getSessionVariable().isSetUseNthExecPlan()) {
            enforcer.addValidOutputInputProperties(newOutputProperty, Lists.newArrayList(PhysicalPropertySet.EMPTY));
            enforcer.getGroup().addSatisfyRequiredPropertyGroupExpression(newOutputProperty, enforcer);
        }
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);

        String hint = node.getJoinHint();
        GroupExpression leftChild = childrenBestExprList.get(0);
        GroupExpression rightChild = childrenBestExprList.get(1);

        PhysicalPropertySet leftChildOutputProperty = childrenOutputProperties.get(0);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);

        // 1. Distribution is broadcast
        if (rightChildOutputProperty.getDistributionProperty().isBroadcast()) {
            return visitOperator(node, context);
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

            // 2.1 respect the hint
            if ("SHUFFLE".equalsIgnoreCase(hint)) {
                if (leftDistributionDesc.isLocalShuffle()) {
                    enforceChildShuffleDistribution(leftShuffleColumns, leftChild, leftChildOutputProperty, 0);
                }
                if (rightDistributionDesc.isLocalShuffle()) {
                    enforceChildShuffleDistribution(rightShuffleColumns, rightChild, rightChildOutputProperty, 1);
                }
                return visitOperator(node, context);
            }

            if (leftDistributionDesc.isLocalShuffle() && rightDistributionDesc.isLocalShuffle()) {
                // colocate join
                if (!"BUCKET".equalsIgnoreCase(hint) &&
                        canColocateJoin(leftDistributionSpec, rightDistributionSpec, leftShuffleColumns,
                                rightShuffleColumns)) {
                    return visitOperator(node, context);
                } else {
                    transToBucketShuffleJoin(leftDistributionDesc, leftShuffleColumns, rightShuffleColumns);
                    return visitOperator(node, context);
                }
            } else if (leftDistributionDesc.isLocalShuffle() && rightDistributionDesc.isJoinShuffle()) {
                // bucket join
                transToBucketShuffleJoin(leftDistributionDesc, leftShuffleColumns, rightShuffleColumns);
                return visitOperator(node, context);
            } else if (leftDistributionDesc.isJoinShuffle() && rightDistributionDesc.isLocalShuffle()) {
                // coordinator can not bucket shuffle data from left to right, so we need to adjust to shuffle join
                enforceChildShuffleDistribution(rightShuffleColumns, rightChild, rightChildOutputProperty, 1);
                return visitOperator(node, context);
            } else if (leftDistributionDesc.isJoinShuffle() && rightDistributionDesc.isJoinShuffle()) {
                // shuffle join
                return visitOperator(node, context);
            } else {
                Preconditions.checkState(false, "Children output property distribution error");
            }
        } else {
            Preconditions.checkState(false, "Children output property distribution error");
        }

        return visitOperator(node, context);
    }

}
