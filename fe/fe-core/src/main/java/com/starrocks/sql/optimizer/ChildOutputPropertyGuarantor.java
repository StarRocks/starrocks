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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

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

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
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

        // The order of equivalence predicates(shuffle columns are derived from them) is
        // meaningless, hence it is correct to use a set to save these shuffle pairs. According
        // to the distribution column information of the left and right children, we can build
        // distribution pairs. We can use colocate join is judged by whether all the distribution
        // pairs are exist in the equivalent predicates set.
        Set<Pair<Integer, Integer>> shufflePairs = Sets.newHashSet();
        for (int i = 0; i < leftShuffleColumns.size(); i++) {
            shufflePairs.add(Pair.create(leftShuffleColumns.get(i), rightShuffleColumns.get(i)));
        }

        for (int i = 0; i < leftLocalDistributionDesc.getColumns().size(); ++i) {
            int leftScanColumnId = leftLocalDistributionDesc.getColumns().get(i);
            ColumnRefSet leftEquivalentCols = leftLocalDistributionSpec.getPropertyInfo()
                    .getEquivalentColumns(leftScanColumnId);

            int rightScanColumnId = rightLocalDistributionDesc.getColumns().get(i);
            ColumnRefSet rightEquivalentCols = rightLocalDistributionSpec.getPropertyInfo()
                    .getEquivalentColumns(rightScanColumnId);

            if (!isDistributionPairExist(shufflePairs, leftEquivalentCols, rightEquivalentCols)) {
                return false;
            }
        }

        return true;
    }

    // enforce child SHUFFLE type distribution
    private GroupExpression enforceChildShuffleDistribution(List<Integer> shuffleColumns, GroupExpression child,
                                                            PhysicalPropertySet childOutputProperty, int childIndex) {
        DistributionSpec enforceDistributionSpec =
                DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(shuffleColumns,
                        HashDistributionDesc.SourceType.SHUFFLE_ENFORCE));

        Pair<GroupExpression, PhysicalPropertySet> pair =
                enforceChildDistribution(enforceDistributionSpec, child, childOutputProperty);
        PhysicalPropertySet newChildInputProperty = pair.second;

        requiredChildrenProperties.set(childIndex, newChildInputProperty);
        childrenOutputProperties.set(childIndex, newChildInputProperty);
        return pair.first;
    }

    private void enforceChildSatisfyShuffleJoin(HashDistributionSpec leftDistributionSpec,
                                                List<Integer> leftShuffleColumns, List<Integer> rightShuffleColumns,
                                                GroupExpression child, PhysicalPropertySet childOutputProperty) {
        List<Integer> newRightShuffleColumns = Lists.newArrayList();
        HashDistributionDesc leftDistributionDesc = leftDistributionSpec.getHashDistributionDesc();
        DistributionSpec.PropertyInfo leftDistributionPropertyInfo = leftDistributionSpec.getPropertyInfo();

        for (int cid : leftDistributionDesc.getColumns()) {
            if (leftShuffleColumns.contains(cid)) {
                int index = leftShuffleColumns.indexOf(cid);
                newRightShuffleColumns.add(rightShuffleColumns.get(index));
            } else {
                // find equivalent columns for the hash distribution columns
                int equivalentColumn =
                        Arrays.stream(leftDistributionPropertyInfo.getEquivalentColumns(cid).getColumnIds()).
                                filter(leftShuffleColumns::contains).findAny().orElse(cid);
                Preconditions.checkState(leftShuffleColumns.contains(equivalentColumn));
                int index = leftShuffleColumns.indexOf(equivalentColumn);
                newRightShuffleColumns.add(rightShuffleColumns.get(index));
            }
        }

        enforceChildShuffleDistribution(newRightShuffleColumns, child, childOutputProperty, 1);
    }

    private void transToBucketShuffleJoin(HashDistributionSpec leftLocalDistributionSpec,
                                          List<Integer> leftShuffleColumns, List<Integer> rightShuffleColumns) {
        List<Integer> bucketShuffleColumns = Lists.newArrayList();
        HashDistributionDesc leftLocalDistributionDesc = leftLocalDistributionSpec.getHashDistributionDesc();
        for (int leftScanColumn : leftLocalDistributionDesc.getColumns()) {
            int index = leftShuffleColumns.indexOf(leftScanColumn);
            if (index == -1) {
                /*
                 * Given the following example：
                 *      SELECT * FROM A JOIN B ON A.a = B.b
                 *      JOIN C ON B.b = C.c
                 *      JOIN D ON C.c = D.d
                 *      JOIN E ON D.d = E.e
                 * We focus on the third join `.. join D ON C.c = D.d`
                 * leftShuffleColumns: [C.d]
                 * rightShuffleColumns: [D.d]
                 * leftScanColumn: A.a
                 * joinEquivalentColumns: [A.a, B.b, C.c, D.d]
                 *
                 * So we can get A.a's equivalent column C.c from joinEquivalentColumns
                 */
                DistributionSpec.PropertyInfo propertyInfo = leftLocalDistributionSpec.getPropertyInfo();
                int[] joinEquivalentColumnsColumns = propertyInfo.getEquivalentJoinOnColumns(leftScanColumn);
                // TODO(hcf) Is the lookup strategy right?
                for (int alternativeLeftScanColumn : joinEquivalentColumnsColumns) {
                    index = leftShuffleColumns.indexOf(alternativeLeftScanColumn);
                    if (index != -1) {
                        break;
                    }
                }
                Preconditions.checkState(index != -1, "Cannot find join equivalent column");
            }
            bucketShuffleColumns.add(rightShuffleColumns.get(index));
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

        DistributionProperty newDistributionProperty = new DistributionProperty(distributionSpec);
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
        Preconditions.checkState(childrenOutputProperties.size() == 2);

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
        List<Integer> leftOnPredicateColumns = joinHelper.getLeftOnColumns();
        List<Integer> rightOnPredicateColumns = joinHelper.getRightOnColumns();
        // Get required properties for children.
        List<PhysicalPropertySet> requiredProperties =
                computeShuffleJoinRequiredProperties(requirements, leftOnPredicateColumns,
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
                Preconditions.checkState(false, "Children output property distribution error");
            }
        } else {
            Preconditions.checkState(false, "Children output property distribution error");
        }

        return visitOperator(node, context);
    }

    // Check that the children hash as the same column size, and in the same order which is decided by the on predicate
    //                      join (v1=v7 and v2=v8)
    //                      /                     \
    //                   join(v1=v3)             join(v7=v10)
    // bottom join hash shuffle by v1, v7 is legal
    //
    //                      join (v1=v7 and v2=v8)
    //                      /                     \
    //                   join(v1=v3)             join(v8=v10)
    // bottom join hash shuffle by v1, v8 is NOT legal
    private boolean checkChildDistributionSatisfyShuffle(HashDistributionSpec leftDistributionSpec,
                                                         HashDistributionSpec rightDistributionSpec,
                                                         List<Integer> leftShuffleColumns,
                                                         List<Integer> rightShuffleColumns) {
        List<Integer> leftDistributionColumns = leftDistributionSpec.getHashDistributionDesc().getColumns();
        List<Integer> rightDistributionColumns = rightDistributionSpec.getHashDistributionDesc().getColumns();
        DistributionSpec.PropertyInfo leftDistributionPropertyInfo = leftDistributionSpec.getPropertyInfo();
        DistributionSpec.PropertyInfo rightDistributionPropertyInfo = rightDistributionSpec.getPropertyInfo();

        List<Integer> leftIndexList = Lists.newArrayList();
        List<Integer> rightIndexList = Lists.newArrayList();
        for (Integer cid : leftDistributionColumns) {
            if (leftShuffleColumns.contains(cid)) {
                leftIndexList.add(leftShuffleColumns.indexOf(cid));
            } else {
                // find equivalent columns for the hash distribution columns
                int equivalentColumn =
                        Arrays.stream(leftDistributionPropertyInfo.getEquivalentColumns(cid).getColumnIds()).
                                filter(leftShuffleColumns::contains).findAny().orElse(cid);
                Preconditions.checkState(leftShuffleColumns.contains(equivalentColumn));
                leftIndexList.add(leftShuffleColumns.indexOf(equivalentColumn));
            }
        }

        for (Integer cid : rightDistributionColumns) {
            if (rightShuffleColumns.contains(cid)) {
                rightIndexList.add(rightShuffleColumns.indexOf(cid));
            } else {
                // find equivalent columns for the hash distribution columns
                int equivalentColumn =
                        Arrays.stream(rightDistributionPropertyInfo.getEquivalentColumns(cid).getColumnIds()).
                                filter(rightShuffleColumns::contains).findAny().orElse(cid);
                Preconditions.checkState(rightShuffleColumns.contains(equivalentColumn));
                rightIndexList.add(rightShuffleColumns.indexOf(equivalentColumn));
            }
        }
        return leftIndexList.equals(rightIndexList);
    }

    private boolean isDistributionPairExist(Set<Pair<Integer, Integer>> shufflePairs,
                                            ColumnRefSet leftEquivalentCols,
                                            ColumnRefSet rightEquivalentCols) {
        for (int leftCol : leftEquivalentCols.getColumnIds()) {
            for (int rightCol : rightEquivalentCols.getColumnIds()) {
                Pair<Integer, Integer> distributionPair = Pair.create(leftCol, rightCol);
                if (shufflePairs.contains(distributionPair)) {
                    return true;
                }
            }
        }
        return false;
    }
}
