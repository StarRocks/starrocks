// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
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
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.getEqConj;

public class InputPropertyDeriver extends OperatorVisitor<Void, ExpressionContext> {
    private PhysicalPropertySet requirements;
    private List<List<PhysicalPropertySet>> inputProperties;
    private final TaskContext taskContext;
    private final OptimizerContext context;

    public InputPropertyDeriver(TaskContext taskContext) {
        this.taskContext = taskContext;
        this.context = taskContext.getOptimizerContext();
    }

    public List<List<PhysicalPropertySet>> getInputProps(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression) {
        this.requirements = requirements;

        inputProperties = Lists.newArrayList();
        groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        return inputProperties;
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        List<PhysicalPropertySet> inputProps = new ArrayList<>();
        for (int childIndex = 0; childIndex < context.arity(); ++childIndex) {
            inputProps.add(PhysicalPropertySet.EMPTY);
        }
        inputProperties.add(inputProps);
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        String hint = node.getJoinHint();

        // 1 For broadcast join
        PhysicalPropertySet rightBroadcastProperty =
                new PhysicalPropertySet(new DistributionProperty(DistributionSpec.createReplicatedDistributionSpec()));
        inputProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty));

        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);
        List<BinaryPredicateOperator> equalOnPredicate =
                getEqConj(leftChildColumns, rightChildColumns, Utils.extractConjuncts(node.getOnPredicate()));

        if (Utils.canOnlyDoBroadcast(node, equalOnPredicate, hint)) {
            return null;
        }

        if (node.getJoinType().isRightJoin() || node.getJoinType().isFullOuterJoin()
                || "SHUFFLE".equalsIgnoreCase(hint) || "BUCKET".equalsIgnoreCase(hint)) {
            inputProperties.clear();
        }

        // 2 For shuffle join
        List<Integer> leftOnPredicateColumns = new ArrayList<>();
        List<Integer> rightOnPredicateColumns = new ArrayList<>();
        JoinPredicateUtils.getJoinOnPredicatesColumns(equalOnPredicate, leftChildColumns, rightChildColumns,
                leftOnPredicateColumns, rightOnPredicateColumns);
        Preconditions.checkState(leftOnPredicateColumns.size() == rightOnPredicateColumns.size());

        HashDistributionSpec leftDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(leftOnPredicateColumns, HashDistributionDesc.SourceType.SHUFFLE_JOIN));
        HashDistributionSpec rightDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(rightOnPredicateColumns, HashDistributionDesc.SourceType.SHUFFLE_JOIN));

        PhysicalPropertySet leftInputProperty = createPropertySetByDistribution(leftDistribution);
        PhysicalPropertySet rightInputProperty = createPropertySetByDistribution(rightDistribution);

        inputProperties.add(Lists.newArrayList(leftInputProperty, rightInputProperty));
        return null;
    }

    private Optional<HashDistributionDesc> getRequiredLocalDesc() {
        if (!requirements.getDistributionProperty().isShuffle()) {
            return Optional.empty();
        }

        HashDistributionDesc requireDistributionDesc =
                ((HashDistributionSpec) requirements.getDistributionProperty().getSpec()).getHashDistributionDesc();
        if (!HashDistributionDesc.SourceType.SHUFFLE_LOCAL.equals(requireDistributionDesc.getSourceType())) {
            return Optional.empty();
        }

        return Optional.of(requireDistributionDesc);
    }

    @Override
    public Void visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
        // If scan tablet sum leas than 1, do one phase local aggregate is enough
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == 0
                && context.getRootProperty().isExecuteInOneTablet()
                && node.getType().isGlobal() && !node.isSplit()) {
            inputProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
            return null;
        }

        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (node.getType().isGlobal() && !node.isSplit())) {
            inputProperties.add(Lists.newArrayList(createLimitGatherProperty(child.getLimit())));
            return null;
        }

        if (!node.getType().isLocal()) {
            List<Integer> columns = node.getPartitionByColumns().stream().map(ColumnRefOperator::getId).collect(
                    Collectors.toList());

            // None grouping columns
            if (columns.isEmpty()) {
                DistributionProperty distributionProperty =
                        new DistributionProperty(DistributionSpec.createGatherDistributionSpec());
                inputProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty)));
                return null;
            }

            // shuffle aggregation
            DistributionSpec distributionSpec = DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(columns, HashDistributionDesc.SourceType.SHUFFLE_AGG));
            DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
            inputProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty)));
            return null;
        }

        inputProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        return null;
    }

    @Override
    public Void visitPhysicalTopN(PhysicalTopNOperator topN, ExpressionContext context) {
        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (topN.getSortPhase().isFinal() && !topN.isSplit())) {
            PhysicalPropertySet inputProperty = createLimitGatherProperty(child.getLimit());
            inputProperties.add(Lists.newArrayList(inputProperty));
        } else {
            inputProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        }

        return null;
    }

    @Override
    public Void visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        DistributionSpec gather = DistributionSpec.createGatherDistributionSpec();
        DistributionProperty inputProperty = new DistributionProperty(gather);

        inputProperties.add(Lists.newArrayList(new PhysicalPropertySet(inputProperty)));
        return null;
    }

    @Override
    public Void visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        List<Integer> partitionColumnRefSet = new ArrayList<>();

        node.getPartitionExpressions().forEach(e -> {
            partitionColumnRefSet
                    .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList()));
        });

        SortProperty sortProperty = new SortProperty(new OrderSpec(node.getEnforceOrderBy()));

        if (partitionColumnRefSet.isEmpty()) {
            DistributionProperty distributionProperty =
                    new DistributionProperty(DistributionSpec.createGatherDistributionSpec());
            inputProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty, sortProperty)));
        } else {
            DistributionProperty distributionProperty = new DistributionProperty(DistributionSpec
                    .createHashDistributionSpec(
                            new HashDistributionDesc(partitionColumnRefSet,
                                    HashDistributionDesc.SourceType.SHUFFLE_AGG)));
            inputProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty, sortProperty)));
        }

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
        if (getRequiredLocalDesc().isPresent()) {
            // Set operator can't support local distribute
            return;
        }
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
        inputProperties.add(childProperty);
    }

    @Override
    public Void visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        inputProperties.add(Lists
                .newArrayList(createLimitGatherProperty(node.getLimit()), createLimitGatherProperty(node.getLimit())));
        return null;
    }

    @Override
    public Void visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        inputProperties.add(Lists.newArrayList(requirements));
        return null;
    }

    @Override
    public Void visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        inputProperties.add(Lists.newArrayList(requirements));
        return null;
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
