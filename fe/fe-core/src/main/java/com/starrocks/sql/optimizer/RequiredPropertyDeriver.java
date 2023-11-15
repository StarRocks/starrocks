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
import com.google.common.collect.Sets;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.CTEProperty;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.EmptyDistributionProperty;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RequiredPropertyDeriver extends PropertyDeriverBase<Void, ExpressionContext> {
    private final ColumnRefFactory columnRefFactory;
    private final PhysicalPropertySet requirementsFromParent;
    private List<List<PhysicalPropertySet>> requiredProperties;

    public RequiredPropertyDeriver(TaskContext context) {
        this.requirementsFromParent = context.getRequiredProperty();
        this.columnRefFactory = context.getOptimizerContext().getColumnRefFactory();
    }

    public List<List<PhysicalPropertySet>> getRequiredProps(GroupExpression groupExpression) {
        requiredProperties = Lists.newArrayList();
        groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        deriveChildCTEProperty(groupExpression);
        return requiredProperties;
    }

    private void deriveChildCTEProperty(GroupExpression groupExpression) {

        OperatorType operatorType = groupExpression.getOp().getOpType();
        if (operatorType == OperatorType.PHYSICAL_CTE_ANCHOR) {
            PhysicalCTEAnchorOperator operator = (PhysicalCTEAnchorOperator) groupExpression.getOp();
            int idx = 0;
            for (PhysicalPropertySet propertySet : requiredProperties.get(0)) {
                PhysicalPropertySet newProperty = propertySet.copy();
                DistributionProperty oldDistribution = newProperty.getDistributionProperty();
                newProperty.setDistributionProperty(DistributionProperty.createProperty(
                        oldDistribution.getSpec(), true));
                Set<Integer> cteIds = Sets.newHashSet(requirementsFromParent.getCteProperty().getCteIds());
                if (idx == 0) {
                    cteIds.retainAll(groupExpression.inputAt(0).getLogicalProperty().getUsedCTEs().getCteIds());
                } else {
                    cteIds.retainAll(groupExpression.inputAt(1).getLogicalProperty().getUsedCTEs().getCteIds());
                    cteIds.add(operator.getCteId());
                }
                newProperty.setCteProperty(CTEProperty.createProperty(cteIds));
                requiredProperties.get(0).set(idx++, newProperty);
            }
        } else if (operatorType == OperatorType.PHYSICAL_NO_CTE) {
            Set<Integer> cteIds = Sets.newHashSet(requirementsFromParent.getCteProperty().getCteIds());
            CTEProperty cteProperty = CTEProperty.createProperty(cteIds);
            PhysicalPropertySet newProperty = requiredProperties.get(0).get(0).copy();
            DistributionProperty oldDistribution = newProperty.getDistributionProperty();
            newProperty.setDistributionProperty(DistributionProperty.createProperty(oldDistribution.getSpec(), true));
            newProperty.setCteProperty(cteProperty);

            requiredProperties.get(0).set(0, newProperty);
        } else {
            if (requirementsFromParent.getCteProperty().isEmpty()) {
                return;
            }
            // Pass CTE property to children
            for (List<PhysicalPropertySet> requiredProperty : requiredProperties) {
                for (int i = 0; i < requiredProperty.size(); i++) {
                    PhysicalPropertySet property = requiredProperty.get(i).copy();
                    Set<Integer> remainCteIds = Sets.newHashSet(requirementsFromParent.getCteProperty().getCteIds());
                    remainCteIds.retainAll(groupExpression.inputAt(i).getLogicalProperty().getUsedCTEs().getCteIds());
                    CTEProperty cteProperty = CTEProperty.createProperty(remainCteIds);
                    property.setCteProperty(cteProperty);
                    requiredProperty.set(i, property);
                }
            }
        }
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        List<PhysicalPropertySet> requiredProps = new ArrayList<>();
        for (int childIndex = 0; childIndex < context.arity(); ++childIndex) {
            requiredProps.add(PhysicalPropertySet.EMPTY);
        }
        requiredProperties.add(requiredProps);
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        // 1 For broadcast join
        PhysicalPropertySet rightBroadcastProperty =
                new PhysicalPropertySet(DistributionProperty.createProperty(DistributionSpec.createReplicatedDistributionSpec()));
        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty));

        JoinHelper joinHelper = JoinHelper.of(node, context.getChildOutputColumns(0), context.getChildOutputColumns(1));

        if (joinHelper.onlyBroadcast()) {
            return null;
        }

        if (joinHelper.onlyShuffle()) {
            requiredProperties.clear();
        }

        // 2 For shuffle join
        List<DistributionCol> leftCols = joinHelper.getLeftCols();
        List<DistributionCol> rightCols = joinHelper.getRightCols();
        if (leftCols.isEmpty() || rightCols.isEmpty()) {
            return null;
        }

        requiredProperties.add(computeShuffleJoinRequiredProperties(requirementsFromParent, leftCols, rightCols));

        return null;
    }

    @Override
    public Void visitPhysicalMergeJoin(PhysicalMergeJoinOperator node, ExpressionContext context) {
        //prepare sort property
        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);

        JoinHelper joinHelper = JoinHelper.of(node, leftChildColumns, rightChildColumns);

        List<DistributionCol> leftOnPredicateColumns = joinHelper.getLeftCols();
        List<DistributionCol> rightOnPredicateColumns = joinHelper.getRightCols();
        List<Ordering> leftOrderings = leftOnPredicateColumns.stream()
                .map(l -> new Ordering(columnRefFactory.getColumnRef(l.getColId()), true, true))
                .collect(Collectors.toList());

        List<Ordering> rightOrderings = rightOnPredicateColumns.stream()
                .map(l -> new Ordering(columnRefFactory.getColumnRef(l.getColId()), true, true))
                .collect(Collectors.toList());

        SortProperty leftSortProperty = SortProperty.createProperty(leftOrderings);
        SortProperty rightSortProperty = SortProperty.createProperty(rightOrderings);

        // 1 For broadcast join
        PhysicalPropertySet leftBroadcastProperty = new PhysicalPropertySet(leftSortProperty);
        PhysicalPropertySet rightBroadcastProperty =
                new PhysicalPropertySet(DistributionProperty.createProperty(DistributionSpec.createReplicatedDistributionSpec()),
                        rightSortProperty);
        requiredProperties.add(Lists.newArrayList(leftBroadcastProperty, rightBroadcastProperty));

        if (joinHelper.onlyBroadcast()) {
            return null;
        }

        if (joinHelper.onlyShuffle()) {
            requiredProperties.clear();
        }

        // 2 For shuffle join
        List<PhysicalPropertySet> physicalPropertySets =
                computeShuffleJoinRequiredProperties(requirementsFromParent, leftOnPredicateColumns,
                        rightOnPredicateColumns);
        physicalPropertySets.get(0).setSortProperty(leftSortProperty);
        physicalPropertySets.get(1).setSortProperty(rightSortProperty);
        requiredProperties.add(physicalPropertySets);

        return null;
    }

    @Override
    public Void visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator node, ExpressionContext context) {
        if (node.getJoinType().isRightJoin() || node.getJoinType().isFullOuterJoin()) {
            // Right join needs to maintain build_match_flag for right table, which could not be maintained on multiple nodes,
            // instead it should be gathered to one node
            PhysicalPropertySet gather =
                    new PhysicalPropertySet(DistributionProperty.createProperty(DistributionSpec.createGatherDistributionSpec()));
            requiredProperties.add(Lists.newArrayList(gather, gather));
        } else {
            PhysicalPropertySet rightBroadcastProperty =
                    new PhysicalPropertySet(
                            DistributionProperty.createProperty(DistributionSpec.createReplicatedDistributionSpec()));
            requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty));
        }
        return null;
    }

    @Override
    public Void visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
        // If scan tablet sum less than 1, do one phase local aggregate is enough
        int aggStage = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
        if (aggStage <= 1 && context.getRootProperty().oneTabletProperty().supportOneTabletOpt
                && node.isOnePhaseAgg()) {
            requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
            return null;
        }

        if (!node.getType().isLocal()) {
            List<DistributionCol> columns = node.getPartitionByColumns().stream()
                    .map(e -> new DistributionCol(e.getId(), true))
                    .collect(Collectors.toList());

            // None grouping columns
            if (columns.isEmpty()) {
                requiredProperties.add(Lists.newArrayList(createGatherPropertySet()));
                return null;
            }

            // shuffle aggregation
            requiredProperties.add(
                    Lists.newArrayList(computeAggRequiredShuffleProperties(columns)));
            return null;
        }

        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        return null;
    }

    @Override
    public Void visitPhysicalTopN(PhysicalTopNOperator topN, ExpressionContext context) {
        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        return null;
    }

    @Override
    public Void visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        requiredProperties.add(Lists.newArrayList(createGatherPropertySet()));
        return null;
    }

    @Override
    public Void visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        List<Integer> partitionColumnRefSet = new ArrayList<>();

        node.getPartitionExpressions().forEach(e -> partitionColumnRefSet
                .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList())));
        SortProperty sortProperty = SortProperty.createProperty(node.getEnforceOrderBy());

        DistributionProperty distributionProperty;
        if (partitionColumnRefSet.isEmpty()) {
            distributionProperty = DistributionProperty.createProperty(DistributionSpec.createGatherDistributionSpec());
        } else {
            // If scan tablet sum less than 1, no distribution property is required
            if (context.getRootProperty().oneTabletProperty().supportOneTabletOpt) {
                distributionProperty = EmptyDistributionProperty.INSTANCE;
            } else {
                List<DistributionCol> distributionCols = partitionColumnRefSet.stream()
                        .map(e -> new DistributionCol(e, true)).collect(
                        Collectors.toList());
                distributionProperty = createShuffleAggProperty(distributionCols);
            }
        }
        requiredProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty, sortProperty)));

        return null;
    }

    @Override
    public Void visitPhysicalUnion(PhysicalUnionOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return null;
    }

    @Override
    public Void visitPhysicalExcept(PhysicalExceptOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return null;
    }

    @Override
    public Void visitPhysicalIntersect(PhysicalIntersectOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return null;
    }

    private void processSetOperationChildProperty(ExpressionContext context) {
        List<PhysicalPropertySet> childProperty = new ArrayList<>();
        for (int i = 0; i < context.arity(); ++i) {
            childProperty.add(PhysicalPropertySet.EMPTY);
        }

        // Use Any to forbidden enforce some property, will add shuffle in FragmentBuilder
        requiredProperties.add(childProperty);
    }

    @Override
    public Void visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        // @todo: check the condition is right?
        //
        // If scan tablet sum less than 1, don't need required gather, because work machine always less than 1?
        // if (context.getRootProperty().isExecuteInOneTablet()) {
        //     requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        //     return null;
        // }

        // limit node in Memo means that the limit cannot be merged into other nodes.
        requiredProperties.add(Lists.newArrayList(createGatherPropertySet()));
        return null;
    }

    @Override
    public Void visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY, requirementsFromParent));
        return null;
    }

    @Override
    public Void visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        requiredProperties.add(Lists.newArrayList(requirementsFromParent));
        return null;
    }


    private List<PhysicalPropertySet> computeAggRequiredShuffleProperties(List<DistributionCol> groupByCols) {
        Optional<HashDistributionDesc> requiredShuffleDescOptional =
                getShuffleJoinHashDistributionDesc(requirementsFromParent);
        if (!requiredShuffleDescOptional.isPresent()) {
            // required property is not SHUFFLE_JOIN
            return Lists.newArrayList(createShuffleAggPropertySet(groupByCols));
        }

        List<DistributionCol> parentsRequiredCols = requiredShuffleDescOptional.get().getDistributionCols();
        List<DistributionCol> requiredCols;
        if (shouldAdjustGroupByOrder(parentsRequiredCols, groupByCols)) {
            // keep order with parent
            groupByCols = parentsRequiredCols.stream().map(e -> new DistributionCol(e.getColId(), true))
                    .collect(Collectors.toList());

        }
        if (canRelaxGroupByCols(parentsRequiredCols, groupByCols)) {
            requiredCols = groupByCols.stream().map(col -> col.getNullRelaxCol()).collect(
                    Collectors.toList());
        } else {
            requiredCols = groupByCols;
        }
        return Lists.newArrayList(createShuffleAggPropertySet(requiredCols));
    }

    private boolean canRelaxGroupByCols(List<DistributionCol> requiredCols, List<DistributionCol> groupByCols) {
        for (DistributionCol col : groupByCols) {
            for (DistributionCol requiredCol : requiredCols) {
                if (requiredCol.getColId() == col.getColId() && !requiredCol.isAggStrict()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean shouldAdjustGroupByOrder(List<DistributionCol> requiredCols, List<DistributionCol> groupByCols) {
        List<DistributionCol> nullStrictRequiredCols = requiredCols.stream().map(e -> e.getNullStrictCol())
                .collect(Collectors.toList());
        return CollectionUtils.isEqualCollection(nullStrictRequiredCols, groupByCols);
    }
}
