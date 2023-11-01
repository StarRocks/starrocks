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


package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class ChildOutputPropertyGuarantor extends PropertyDeriverBase<Void, ExpressionContext> {
    private final OptimizerContext context;
    private final GroupExpression groupExpression;

    private final PhysicalPropertySet requirements;
    // children best group expression
    private final List<GroupExpression> childrenBestExprList;
    // required properties for children.
    private final List<PhysicalPropertySet> requiredChildrenProperties;
    // children output property
    private final List<PhysicalPropertySet> childrenOutputProperties;

    private double curTotalCost;

    public ChildOutputPropertyGuarantor(TaskContext taskContext,
                                        GroupExpression groupExpression,
                                        PhysicalPropertySet requirements,
                                        List<GroupExpression> childrenBestExprList,
                                        List<PhysicalPropertySet> requiredChildrenProperties,
                                        List<PhysicalPropertySet> childrenOutputProperties,
                                        double curTotalCost) {
        this.context = taskContext.getOptimizerContext();
        this.groupExpression = groupExpression;
        this.requirements = requirements;
        this.childrenBestExprList = childrenBestExprList;
        this.requiredChildrenProperties = requiredChildrenProperties;
        this.childrenOutputProperties = childrenOutputProperties;
        this.curTotalCost = curTotalCost;
    }

    public double enforceLegalChildOutputProperty() {
        groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        return this.curTotalCost;
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        return null;
    }

    public boolean canColocateJoinForIcebergTable(HashDistributionSpec leftLocalDistributionSpec,
                                                  HashDistributionSpec rightLocalDistributionSpec,
                                                  List<DistributionCol> leftShuffleColumns,
                                                  List<DistributionCol> rightShuffleColumns) {
        if (ConnectContext.get().getSessionVariable().isDisableColocateJoin()) {
            return false;
        }

        IcebergDistributionDesc leftLocalDistributionDesc = (IcebergDistributionDesc) leftLocalDistributionSpec.
                getHashDistributionDesc();
        IcebergDistributionDesc rightLocalDistributionDesc = (IcebergDistributionDesc) rightLocalDistributionSpec.
                getHashDistributionDesc();

        List<IcebergTable.BucketProperty> leftBucketProperties = leftLocalDistributionDesc.getBucketProperties();
        List<IcebergTable.BucketProperty> rightBucketProperties = rightLocalDistributionDesc.getBucketProperties();

        if (leftBucketProperties.size() != rightBucketProperties.size()) {
            return false;
        }
        for (int i = 0; i < leftBucketProperties.size(); i++) {
            IcebergTable.BucketProperty leftBucketProperty = leftBucketProperties.get(i);
            int leftColumnId = leftLocalDistributionDesc.getDistributionCols().get(i).getColId();
            int idx = 0;
            for (; idx < leftShuffleColumns.size(); ++ idx) {
                if (leftShuffleColumns.get(idx).getColId() == leftColumnId) {
                    break;
                }
            }
            if (idx == leftShuffleColumns.size()) {
                return false;
            }
            IcebergTable.BucketProperty rightBucketProperty = rightBucketProperties.get(idx);
            if (leftBucketProperty.getBucketNum() != rightBucketProperty.getBucketNum()) {
                return false;
            }
        }
        return true;
    }

    public boolean canColocateJoin(HashDistributionSpec leftLocalDistributionSpec,
                                   HashDistributionSpec rightLocalDistributionSpec,
                                   List<DistributionCol> leftShuffleColumns,
                                   List<DistributionCol> rightShuffleColumns) {
        HashDistributionDesc leftLocalDistributionDesc = leftLocalDistributionSpec.getHashDistributionDesc();
        HashDistributionDesc rightLocalDistributionDesc = rightLocalDistributionSpec.getHashDistributionDesc();

        if (ConnectContext.get().getSessionVariable().isDisableColocateJoin()) {
            return false;
        }

        EquivalentDescriptor leftDesc = leftLocalDistributionSpec.getEquivDesc();
        EquivalentDescriptor rightDesc = rightLocalDistributionSpec.getEquivDesc();

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
        long leftTableId = leftDesc.getTableId();
        long rightTableId = rightDesc.getTableId();

        // join self
        if (leftTableId == rightTableId && !colocateIndex.isColocateTable(leftTableId)) {
            if (!leftDesc.isSinglePartition() || !rightDesc.isSinglePartition() ||
                    !leftDesc.getPartitionIds().equals(rightDesc.getPartitionIds())) {
                return false;
            }
        } else {
            // colocate group
            if (!colocateIndex.isSameGroup(leftTableId, rightTableId)) {
                return false;
            }

            ColocateTableIndex.GroupId leftGroupId = colocateIndex.getGroup(leftTableId);
            ColocateTableIndex.GroupId rightGroupId = colocateIndex.getGroup(rightTableId);
            if (colocateIndex.isGroupUnstable(leftGroupId) || colocateIndex.isGroupUnstable(rightGroupId)) {
                return false;
            }
            checkState(leftLocalDistributionDesc.getDistributionCols().size()
                            == rightLocalDistributionDesc.getDistributionCols().size(),
                    "Failed to enforce the output property of children in the join operator. " +
                            "left child distribution info %s, right child distribution info %s",
                    leftLocalDistributionSpec, rightLocalDistributionSpec);
        }

        for (int i = 0; i < leftLocalDistributionDesc.getDistributionCols().size(); ++i) {
            DistributionCol leftCol = leftLocalDistributionDesc.getDistributionCols().get(i);
            DistributionCol rightCol = rightLocalDistributionDesc.getDistributionCols().get(i);
            int idx = 0;
            for (; idx < leftShuffleColumns.size(); idx++) {
                DistributionCol leftRequiredCol = leftShuffleColumns.get(idx);
                DistributionCol rightRequiredCol = rightShuffleColumns.get(idx);
                if (leftLocalDistributionSpec.getEquivDesc().isConnected(leftRequiredCol, leftCol)
                        && rightLocalDistributionSpec.getEquivDesc().isConnected(rightRequiredCol, rightCol)) {
                    break;
                }
            }
            if (idx == leftShuffleColumns.size()) {
                return false;
            }
        }

        return true;
    }

    // enforce child SHUFFLE type distribution
    private GroupExpression enforceChildShuffleDistribution(List<DistributionCol> shuffleColumns, GroupExpression child,
                                                            PhysicalPropertySet childOutputProperty, int childIndex) {
        DistributionSpec enforceDistributionSpec =
                DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(enforceNullStrict(shuffleColumns),
                        HashDistributionDesc.SourceType.SHUFFLE_ENFORCE));

        Pair<GroupExpression, PhysicalPropertySet> pair =
                enforceChildDistribution(enforceDistributionSpec, child, childOutputProperty);
        PhysicalPropertySet newChildInputProperty = pair.second;

        requiredChildrenProperties.set(childIndex, newChildInputProperty);
        childrenOutputProperties.set(childIndex, newChildInputProperty);
        return pair.first;
    }

    private void enforceChildSatisfyShuffleJoin(HashDistributionSpec leftDistributionSpec,
                                                List<DistributionCol> leftShuffleColumns,
                                                List<DistributionCol> rightShuffleColumns,
                                                GroupExpression child, PhysicalPropertySet childOutputProperty) {
        List<DistributionCol> newRightShuffleColumns = Lists.newArrayList();
        HashDistributionDesc leftDistributionDesc = leftDistributionSpec.getHashDistributionDesc();
        EquivalentDescriptor leftEquivDesc = leftDistributionSpec.getEquivDesc();

        for (DistributionCol distributionCol : leftDistributionDesc.getDistributionCols()) {
            int idx = 0;
            for (; idx < leftShuffleColumns.size(); idx++) {
                DistributionCol leftShuffleCol = leftShuffleColumns.get(idx);
                if (leftEquivDesc.isConnected(leftShuffleCol, distributionCol)) {
                    break;
                }
            }
            checkState(idx != leftShuffleColumns.size(),
                    "distribution: %s not satisfied with the requirement: %s.",
                    leftDistributionDesc, leftShuffleColumns);
            newRightShuffleColumns.add(rightShuffleColumns.get(idx));
        }

        enforceChildShuffleDistribution(newRightShuffleColumns, child, childOutputProperty, 1);
    }

    private void transToBucketShuffleJoin(HashDistributionSpec leftLocalDistributionSpec,
                                          List<DistributionCol> leftShuffleColumns,
                                          List<DistributionCol> rightShuffleColumns) {
        List<DistributionCol> bucketShuffleColumns = Lists.newArrayList();
        HashDistributionDesc leftLocalDistributionDesc = leftLocalDistributionSpec.getHashDistributionDesc();
        EquivalentDescriptor leftEquivDesc = leftLocalDistributionSpec.getEquivDesc();
        for (DistributionCol distributionCol : leftLocalDistributionDesc.getDistributionCols()) {
            int idx = 0;
            for (; idx < leftShuffleColumns.size(); idx++) {
                DistributionCol leftShuffleCol = leftShuffleColumns.get(idx);
                if (leftEquivDesc.isConnected(leftShuffleCol, distributionCol)) {
                    break;
                }
            }
            checkState(idx != leftShuffleColumns.size(),
                    "distribution: %s not satisfied with the requirement: %s.",
                    leftLocalDistributionDesc.getDistributionCols(), leftShuffleColumns);
            bucketShuffleColumns.add(rightShuffleColumns.get(idx));
        }

        DistributionSpec rightDistributionSpec =
                DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(bucketShuffleColumns,
                        HashDistributionDesc.SourceType.BUCKET));

        GroupExpression rightChild = childrenBestExprList.get(1);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);
        // enforce right child BUCKET_JOIN type distribution
        // update group expression require property
        PhysicalPropertySet newRightChildInputProperty =
                enforceChildDistribution(rightDistributionSpec, rightChild, rightChildOutputProperty).second;

        requiredChildrenProperties.set(1, newRightChildInputProperty);
        childrenOutputProperties.set(1, newRightChildInputProperty);
    }

    private Pair<GroupExpression, PhysicalPropertySet> enforceChildDistribution(DistributionSpec distributionSpec,
                                                                                GroupExpression child,
                                                                                PhysicalPropertySet childOutputProperty) {
        double childCosts = child.getCost(childOutputProperty);
        Group childGroup = child.getGroup();

        DistributionProperty newDistributionProperty = DistributionProperty.createProperty(distributionSpec);
        PhysicalPropertySet newOutputProperty = childOutputProperty.copy();
        newOutputProperty.setDistributionProperty(newDistributionProperty);

        if (child.getOp() instanceof PhysicalDistributionOperator) {
            GroupExpression enforcer = newDistributionProperty.appendEnforcers(childGroup);
            enforcer.setOutputPropertySatisfyRequiredProperty(newOutputProperty, newOutputProperty);
            context.getMemo().insertEnforceExpression(enforcer, childGroup);

            enforcer.updatePropertyWithCost(newOutputProperty, child.getInputProperties(childOutputProperty),
                    childCosts);
            childGroup.setBestExpression(enforcer, childCosts, newOutputProperty);

            if (ConnectContext.get().getSessionVariable().isSetUseNthExecPlan()) {
                enforcer.addValidOutputPropertyGroup(newOutputProperty, Lists.newArrayList(childOutputProperty));
                enforcer.getGroup().addSatisfyOutputPropertyGroupExpression(newOutputProperty, enforcer);
            }
            return new Pair<>(enforcer, newOutputProperty);
        } else {
            // add physical distribution operator
            GroupExpression enforcer = newDistributionProperty.appendEnforcers(childGroup);
            enforcer.setOutputPropertySatisfyRequiredProperty(newOutputProperty, newOutputProperty);
            updateChildCostWithEnforcer(enforcer, childOutputProperty, newOutputProperty, childCosts, childGroup);
            return new Pair<>(enforcer, newOutputProperty);
        }
    }

    private GroupExpression addChildEnforcer(PhysicalPropertySet oldOutputProperty,
                                             DistributionProperty newDistributionProperty,
                                             double childCost, Group childGroup) {
        PhysicalPropertySet newOutputProperty = new PhysicalPropertySet(newDistributionProperty);
        GroupExpression enforcer = newDistributionProperty.appendEnforcers(childGroup);

        enforcer.setOutputPropertySatisfyRequiredProperty(newOutputProperty, newOutputProperty);
        updateChildCostWithEnforcer(enforcer, oldOutputProperty, newOutputProperty, childCost, childGroup);
        return enforcer;
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
            enforcer.addValidOutputPropertyGroup(newOutputProperty, Lists.newArrayList(oldOutputProperty));
            enforcer.getGroup().addSatisfyOutputPropertyGroupExpression(newOutputProperty, enforcer);
        }
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        return visitPhysicalJoin(node, context);
    }

    @Override
    public Void visitPhysicalMergeJoin(PhysicalMergeJoinOperator node, ExpressionContext context) {
        return visitPhysicalJoin(node, context);
    }

    @Override
    public Void visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator node, ExpressionContext context) {
        return visitPhysicalJoin(node, context);
    }

    public Void visitPhysicalJoin(PhysicalJoinOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 2);

        String hint = node.getJoinHint();
        GroupExpression leftChild = childrenBestExprList.get(0);
        GroupExpression rightChild = childrenBestExprList.get(1);

        PhysicalPropertySet leftChildOutputProperty = childrenOutputProperties.get(0);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);

        // 1. Distribution is broadcast
        DistributionProperty rightDistribute = rightChildOutputProperty.getDistributionProperty();
        if (rightDistribute.isBroadcast() || rightDistribute.isGather()) {
            return visitOperator(node, context);
        }
        // 2. Distribution is shuffle
        JoinHelper joinHelper = JoinHelper.of(node, context.getChildOutputColumns(0), context.getChildOutputColumns(1));
        List<DistributionCol> leftOnPredicateColumns = joinHelper.getLeftCols();
        List<DistributionCol> rightOnPredicateColumns = joinHelper.getRightCols();
        // Get required properties for children.
        List<PhysicalPropertySet> requiredProperties =
                computeShuffleJoinRequiredProperties(requirements, leftOnPredicateColumns,
                        rightOnPredicateColumns);
        checkState(requiredProperties.size() == 2);
        List<DistributionCol> leftShuffleColumns =
                ((HashDistributionSpec) requiredProperties.get(0).getDistributionProperty().getSpec())
                        .getShuffleColumns();
        List<DistributionCol> rightShuffleColumns =
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
            if (JoinOperator.HINT_SHUFFLE.equals(hint)) {
                if (leftDistributionDesc.isLocal()) {
                    enforceChildShuffleDistribution(leftShuffleColumns, leftChild, leftChildOutputProperty, 0);
                }
                if (rightDistributionDesc.isLocal()) {
                    rightChild =
                            enforceChildShuffleDistribution(rightShuffleColumns, rightChild, rightChildOutputProperty,
                                    1);
                }
                leftDistributionSpec = (HashDistributionSpec) childrenOutputProperties.get(0).getDistributionProperty().
                        getSpec();
                rightDistributionSpec =
                        (HashDistributionSpec) childrenOutputProperties.get(1).getDistributionProperty().
                                getSpec();
                if (!checkChildDistributionSatisfyShuffle(leftDistributionSpec, rightDistributionSpec,
                        leftShuffleColumns,
                        rightShuffleColumns)) {
                    enforceChildSatisfyShuffleJoin(leftDistributionSpec, leftShuffleColumns, rightShuffleColumns,
                            rightChild, childrenOutputProperties.get(1));
                }
                return visitOperator(node, context);
            }

            if (leftDistributionDesc.isLocal() && rightDistributionDesc.isLocal()) {
                // colocate join
                if (JoinOperator.HINT_BUCKET.equals(hint) ||
                        !canColocateJoin(leftDistributionSpec, rightDistributionSpec, leftShuffleColumns,
                                rightShuffleColumns)) {
                    transToBucketShuffleJoin(leftDistributionSpec, leftShuffleColumns, rightShuffleColumns);
                }
                return visitOperator(node, context);
            } else if (leftDistributionDesc.isLocal() && rightDistributionDesc.isShuffle()) {
                // bucket join
                transToBucketShuffleJoin(leftDistributionSpec, leftShuffleColumns, rightShuffleColumns);
                return visitOperator(node, context);
            } else if (leftDistributionDesc.isShuffle() && rightDistributionDesc.isLocal()) {
                // coordinator can not bucket shuffle data from left to right, so we need to adjust to shuffle join
                enforceChildSatisfyShuffleJoin(leftDistributionSpec, leftShuffleColumns, rightShuffleColumns,
                        rightChild, rightChildOutputProperty);
                return visitOperator(node, context);
            } else if (leftDistributionDesc.isShuffle() && rightDistributionDesc.isShuffle()) {
                // shuffle join
                if (!checkChildDistributionSatisfyShuffle(leftDistributionSpec, rightDistributionSpec,
                        leftShuffleColumns,
                        rightShuffleColumns)) {
                    enforceChildSatisfyShuffleJoin(leftDistributionSpec, leftShuffleColumns, rightShuffleColumns,
                            rightChild, rightChildOutputProperty);
                }
                return visitOperator(node, context);
            } else {
                //noinspection ConstantConditions
                checkState(false, "Children output property distribution error");
            }
        } else if (leftDistributionDesc.isIcebergLocal() && rightDistributionDesc.isIcebergLocal()) {
            // iceberg colocate join
            if (!canColocateJoinForIcebergTable(leftDistributionSpec, rightDistributionSpec, leftShuffleColumns,
                    rightShuffleColumns)) {
                enforceChildShuffleDistribution(leftShuffleColumns, leftChild, leftChildOutputProperty, 0);
                enforceChildShuffleDistribution(rightShuffleColumns, rightChild, rightChildOutputProperty, 1);
            }
            return visitOperator(node, context);
        } else if (leftDistributionDesc.isIcebergLocal() && rightDistributionDesc.isShuffle()) {
            // iceberg bucket join
            transToBucketShuffleJoin(leftDistributionSpec, leftShuffleColumns, rightShuffleColumns);
            return visitOperator(node, context);
        } else {
            checkState(false, "Children output property distribution error");
        }

        return visitOperator(node, context);
    }

    // Check that the children hash as the same column size, and in the same order which is decided by the on predicate
    //                      join (t1.v1=t2.v7 and t1.v2=t2.v8)
    //                      /                     \
    //                   t1 join(v1=v3)         t2 join(v7=v10)
    // left child hash shuffled by t1.v1 and right child hash shuffled by t2.v7 is legal
    //
    //                      join (t1.v1=t2.v7 and t1.v2=t2.v8)
    //                      /                     \
    //                   t1 join(v1=v3)         t2 join(v8=v10)
    // left child hash shuffled by t1.v1 and right child hash shuffled by t2.v8 is NOT legal
    private boolean checkChildDistributionSatisfyShuffle(HashDistributionSpec leftDistributionSpec,
                                                         HashDistributionSpec rightDistributionSpec,
                                                         List<DistributionCol> leftShuffleColumns,
                                                         List<DistributionCol> rightShuffleColumns) {
        List<DistributionCol> leftDistributionColumns = leftDistributionSpec.getShuffleColumns();
        List<DistributionCol> rightDistributionColumns = rightDistributionSpec.getShuffleColumns();
        EquivalentDescriptor leftEquivDesc = leftDistributionSpec.getEquivDesc();
        EquivalentDescriptor rightEquivDesc = rightDistributionSpec.getEquivDesc();

        if (leftDistributionColumns.size() != rightDistributionColumns.size()) {
            return false;
        }

        for (int i = 0; i < leftDistributionColumns.size(); i++) {
            DistributionCol leftCol = leftDistributionColumns.get(i);
            DistributionCol rightCol = rightDistributionColumns.get(i);
            int idx = 0;
            for (; idx < leftShuffleColumns.size(); idx++) {
                DistributionCol leftShuffleCol = leftShuffleColumns.get(idx);
                DistributionCol rightShuffleCol = rightShuffleColumns.get(idx);
                if (leftEquivDesc.isConnected(leftShuffleCol, leftCol) &&
                        rightEquivDesc.isConnected(rightShuffleCol, rightCol)) {
                    break;
                }
            }
            if (idx == leftShuffleColumns.size()) {
                return false;
            }
        }
        return true;
    }
}
