// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.getEqConj;

public class RequiredPropertyDeriver extends OperatorVisitor<Void, ExpressionContext> {
    private final PhysicalPropertySet requirementsFromParent;
    private List<List<PhysicalPropertySet>> requiredProperties;

    public RequiredPropertyDeriver(PhysicalPropertySet requirementsFromParent) {
        this.requirementsFromParent = requirementsFromParent;
    }

    public List<List<PhysicalPropertySet>> getRequiredProps(GroupExpression groupExpression) {
        requiredProperties = Lists.newArrayList();
        groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        return requiredProperties;
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        List<PhysicalPropertySet> requiredProps = new ArrayList<>();
        for (int childIndex = 0; childIndex < context.arity(); ++childIndex) {
            // @todo: resolve required gather property by check child limit
            if (!node.hasLimit() && context.getChildOperator(childIndex).hasLimit()) {
                requiredProps.add(createLimitGatherProperty(context.getChildOperator(childIndex).getLimit()));
            } else {
                requiredProps.add(PhysicalPropertySet.EMPTY);
            }
        }
        requiredProperties.add(requiredProps);
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        String hint = node.getJoinHint();

        // 1 For broadcast join
        PhysicalPropertySet rightBroadcastProperty =
                new PhysicalPropertySet(new DistributionProperty(DistributionSpec.createReplicatedDistributionSpec()));
        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty));

        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);
        List<BinaryPredicateOperator> equalOnPredicate =
                getEqConj(leftChildColumns, rightChildColumns, Utils.extractConjuncts(node.getOnPredicate()));

        if (Utils.canOnlyDoBroadcast(node, equalOnPredicate, hint)) {
            return null;
        }

        if (node.getJoinType().isRightJoin() || node.getJoinType().isFullOuterJoin()
                || "SHUFFLE".equalsIgnoreCase(hint) || "BUCKET".equalsIgnoreCase(hint)) {
            requiredProperties.clear();
        }

        // 2 For shuffle join
        List<Integer> leftOnPredicateColumns = new ArrayList<>();
        List<Integer> rightOnPredicateColumns = new ArrayList<>();
        JoinPredicateUtils.getJoinOnPredicatesColumns(equalOnPredicate, leftChildColumns, rightChildColumns,
                leftOnPredicateColumns, rightOnPredicateColumns);
        Preconditions.checkState(leftOnPredicateColumns.size() == rightOnPredicateColumns.size());
        requiredProperties
                .add(Utils.computeShuffleJoinRequiredProperties(requirementsFromParent, leftOnPredicateColumns,
                        rightOnPredicateColumns));

        return null;
    }

    @Override
    public Void visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
        // If scan tablet sum leas than 1, do one phase local aggregate is enough
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == 0
                && context.getRootProperty().isExecuteInOneTablet()
                && node.getType().isGlobal() && !node.isSplit()) {
            requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
            return null;
        }

        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (node.getType().isGlobal() && !node.isSplit())) {
            requiredProperties.add(Lists.newArrayList(createLimitGatherProperty(child.getLimit())));
            return null;
        }

        if (!node.getType().isLocal()) {
            List<Integer> columns = node.getPartitionByColumns().stream().map(ColumnRefOperator::getId).collect(
                    Collectors.toList());

            // None grouping columns
            if (columns.isEmpty()) {
                DistributionProperty distributionProperty =
                        new DistributionProperty(DistributionSpec.createGatherDistributionSpec());
                requiredProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty)));
                return null;
            }

            // shuffle aggregation
            DistributionSpec distributionSpec = DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(columns, HashDistributionDesc.SourceType.SHUFFLE_AGG));
            DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
            requiredProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty)));
            return null;
        }

        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        return null;
    }

    @Override
    public Void visitPhysicalTopN(PhysicalTopNOperator topN, ExpressionContext context) {
        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (topN.getSortPhase().isFinal() && !topN.isSplit())) {
            PhysicalPropertySet requiredProperty = createLimitGatherProperty(child.getLimit());
            requiredProperties.add(Lists.newArrayList(requiredProperty));
        } else {
            requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        }

        return null;
    }

    @Override
    public Void visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        DistributionSpec gather = DistributionSpec.createGatherDistributionSpec();
        DistributionProperty requiredProperty = new DistributionProperty(gather);

        requiredProperties.add(Lists.newArrayList(new PhysicalPropertySet(requiredProperty)));
        return null;
    }

    @Override
    public Void visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        List<Integer> partitionColumnRefSet = new ArrayList<>();

        node.getPartitionExpressions().forEach(e -> partitionColumnRefSet
                .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList())));

        SortProperty sortProperty = new SortProperty(new OrderSpec(node.getEnforceOrderBy()));

        DistributionProperty distributionProperty;
        if (partitionColumnRefSet.isEmpty()) {
            distributionProperty = new DistributionProperty(DistributionSpec.createGatherDistributionSpec());
        } else {
            distributionProperty = new DistributionProperty(DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(partitionColumnRefSet, HashDistributionDesc.SourceType.SHUFFLE_AGG)));
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
            LogicalOperator child = (LogicalOperator) context.getChildOperator(i);
            // If child has limit, we need to gather data to one instance
            if (child.hasLimit()) {
                childProperty.add(createLimitGatherProperty(child.getLimit()));
            } else {
                childProperty.add(PhysicalPropertySet.EMPTY);
            }
        }

        // Use Any to forbidden enforce some property, will add shuffle in FragmentBuilder
        requiredProperties.add(childProperty);
    }

    @Override
    public Void visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        // limit node in Memo means that the limit cannot be merged into other nodes.
        requiredProperties.add(Lists.newArrayList(createLimitGatherProperty(node.getLimit())));
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

    private PhysicalPropertySet createLimitGatherProperty(long limit) {
        DistributionSpec distributionSpec = DistributionSpec.createGatherDistributionSpec(limit);
        DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
        return new PhysicalPropertySet(distributionProperty, SortProperty.EMPTY);
    }

}
