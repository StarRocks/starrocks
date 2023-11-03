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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.AnyDistributionSpec;
import com.starrocks.sql.optimizer.base.CTEProperty;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.starrocks.sql.optimizer.base.HashDistributionDesc.SourceType.SHUFFLE_AGG;
import static com.starrocks.sql.optimizer.base.HashDistributionDesc.SourceType.SHUFFLE_JOIN;

// The output property of the node is calculated according to the attributes of the child node and itself.
// Currently join node enforces a valid property for the child node that cannot meet the requirements.
public class OutputPropertyDeriver extends PropertyDeriverBase<PhysicalPropertySet, ExpressionContext> {

    private static final Logger LOG = LogManager.getLogger(OutputPropertyDeriver.class);

    private final GroupExpression groupExpression;

    private final PhysicalPropertySet requirements;
    // children output property
    private final List<PhysicalPropertySet> childrenOutputProperties;

    public OutputPropertyDeriver(GroupExpression groupExpression, PhysicalPropertySet requirements,
                                 List<PhysicalPropertySet> childrenOutputProperties) {
        this.groupExpression = groupExpression;
        this.requirements = requirements;
        // children best group expression
        this.childrenOutputProperties = childrenOutputProperties;
    }

    public PhysicalPropertySet getOutputProperty() {
        return groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
    }

    @NotNull
    private PhysicalPropertySet mergeCTEProperty(PhysicalPropertySet output) {
        // set cte property
        CTEProperty outputCte = new CTEProperty();
        outputCte.merge(output.getCteProperty());
        for (PhysicalPropertySet childrenOutputProperty : childrenOutputProperties) {
            outputCte.merge(childrenOutputProperty.getCteProperty());
        }
        output = output.copy();
        output.setCteProperty(outputCte);
        return output;
    }

    @Override
    public PhysicalPropertySet visitOperator(Operator node, ExpressionContext context) {
        return mergeCTEProperty(PhysicalPropertySet.EMPTY);
    }

    private PhysicalPropertySet computeColocateJoinOutputProperty(JoinOperator joinType,
                                                                  HashDistributionSpec leftScanDistributionSpec,
                                                                  HashDistributionSpec rightScanDistributionSpec) {

        HashDistributionSpec dominatedOutputSpec;

        if (joinType.isRightJoin()) {
            dominatedOutputSpec = rightScanDistributionSpec;
        } else if (joinType.isFullOuterJoin()) {
            dominatedOutputSpec = leftScanDistributionSpec.getNullRelaxSpec(leftScanDistributionSpec.getPropertyInfo());
        } else {
            dominatedOutputSpec = leftScanDistributionSpec;
        }

        DistributionSpec.PropertyInfo leftInfo = leftScanDistributionSpec.getPropertyInfo();
        DistributionSpec.PropertyInfo rightInfo = rightScanDistributionSpec.getPropertyInfo();


        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
        long leftTableId = leftInfo.tableId;
        long rightTableId = rightInfo.tableId;

        if (leftTableId == rightTableId && !colocateIndex.isSameGroup(leftTableId, rightTableId)) {
            return createPropertySetByDistribution(dominatedOutputSpec);
        } else {
            Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleDesc();
            if (!requiredShuffleDesc.isPresent()) {
                return createPropertySetByDistribution(dominatedOutputSpec);
            }

            return createPropertySetByDistribution(
                    new HashDistributionSpec(
                            new HashDistributionDesc(dominatedOutputSpec.getShuffleColumns(),
                                    HashDistributionDesc.SourceType.LOCAL),
                            dominatedOutputSpec.getPropertyInfo()));
        }
    }

    private PhysicalPropertySet computeBucketJoinDistributionPropertyInfo(JoinOperator joinType,
                                                                          HashDistributionSpec hashDistributionSpec,
                                                                          PhysicalPropertySet physicalPropertySet) {
        if (!joinType.isFullOuterJoin() && !joinType.isRightOuterJoin()) {
            return physicalPropertySet;
        }
        HashDistributionSpec newSpec = hashDistributionSpec.getNullRelaxSpec(hashDistributionSpec.getPropertyInfo());
        return new PhysicalPropertySet(new DistributionProperty(newSpec),
                physicalPropertySet.getSortProperty(), physicalPropertySet.getCteProperty());
    }

    private PhysicalPropertySet computeHashJoinDistributionPropertyInfo(PhysicalJoinOperator node,
                                                                        PhysicalPropertySet physicalPropertySet,
                                                                        List<DistributionCol> leftOnPredicateColumns,
                                                                        List<DistributionCol> rightOnPredicateColumns) {
        // only update propertyInfo in HashDistributionSpec
        if (!physicalPropertySet.getDistributionProperty().isShuffle()) {
            return physicalPropertySet;
        }
        DistributionSpec.PropertyInfo propertyInfo =
                physicalPropertySet.getDistributionProperty().getSpec().getPropertyInfo();

        JoinOperator joinOperator = node.getJoinType();
        if (joinOperator.isInnerJoin()) {
            for (int i = 0; i < leftOnPredicateColumns.size(); i++) {
                DistributionCol leftCol = leftOnPredicateColumns.get(i);
                DistributionCol rightCol = rightOnPredicateColumns.get(i);
                propertyInfo.unionDistributionCols(leftCol, rightCol);
            }
        } else if (joinOperator.isLeftOuterJoin() || joinOperator.isRightOuterJoin()) {
            for (int i = 0; i < leftOnPredicateColumns.size(); i++) {
                DistributionCol leftCol = leftOnPredicateColumns.get(i);
                DistributionCol rightCol = rightOnPredicateColumns.get(i);
                propertyInfo.unionNullRelaxCols(leftCol, rightCol);
            }
        } else if (joinOperator.isFullOuterJoin()) {
            propertyInfo.clearNullStrictDisjointSet();
            for (int i = 0; i < leftOnPredicateColumns.size(); i++) {
                DistributionCol leftCol = leftOnPredicateColumns.get(i);
                DistributionCol rightCol = rightOnPredicateColumns.get(i);
                propertyInfo.unionNullRelaxCols(leftCol, rightCol);
            }
        }

        return physicalPropertySet;
    }

    @Override
    public PhysicalPropertySet visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        return mergeCTEProperty(visitPhysicalJoin(node, context));
    }

    @Override
    public PhysicalPropertySet visitPhysicalMergeJoin(PhysicalMergeJoinOperator node, ExpressionContext context) {
        return mergeCTEProperty(visitPhysicalJoin(node, context));
    }

    @Override
    public PhysicalPropertySet visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator node, ExpressionContext context) {
        return mergeCTEProperty(childrenOutputProperties.get(0));
    }

    private PhysicalPropertySet visitPhysicalJoin(PhysicalJoinOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 2);
        PhysicalPropertySet leftChildOutputProperty = childrenOutputProperties.get(0);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);

        // 1. Distribution is broadcast
        if (rightChildOutputProperty.getDistributionProperty().isBroadcast()) {
            return computeHashJoinDistributionPropertyInfo(node, leftChildOutputProperty,
                    Collections.emptyList(), Collections.emptyList());
        }
        // 2. Distribution is shuffle
        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);
        JoinHelper joinHelper = JoinHelper.of(node, leftChildColumns, rightChildColumns);

        List<DistributionCol> leftOnPredicateColumns = joinHelper.getLeftCols();
        List<DistributionCol> rightOnPredicateColumns = joinHelper.getRightCols();

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
                        computeColocateJoinOutputProperty(node.getJoinType(), leftDistributionSpec, rightDistributionSpec),
                        leftOnPredicateColumns, rightOnPredicateColumns);

            } else if (leftDistributionDesc.isLocal() && rightDistributionDesc.isBucketJoin()) {
                // bucket join
                PhysicalPropertySet outputProperty = computeBucketJoinDistributionPropertyInfo(node.getJoinType(),
                        leftDistributionSpec, leftChildOutputProperty);
                return computeHashJoinDistributionPropertyInfo(node, outputProperty,
                        leftOnPredicateColumns, rightOnPredicateColumns);

            } else if ((leftDistributionDesc.isShuffle() || leftDistributionDesc.isShuffleEnforce()) &&
                    (rightDistributionDesc.isShuffle()) || rightDistributionDesc.isShuffleEnforce()) {
                // shuffle join
                PhysicalPropertySet outputProperty = computeShuffleJoinOutputProperty(node.getJoinType(),
                        leftDistributionDesc.getDistributionCols(), rightDistributionDesc.getDistributionCols());
                return computeHashJoinDistributionPropertyInfo(node, outputProperty,
                        leftOnPredicateColumns, rightOnPredicateColumns);

            } else {
                LOG.error("Children output property distribution error.left child property: {}, " +
                                "right child property: {}, join node: {}",
                        leftChildDistributionProperty, rightChildDistributionProperty, node);
                throw new IllegalStateException("Children output property distribution error.");
            }
        } else {
            LOG.error("Children output property distribution error.left child property: {}, " +
                            "right child property: {}, join node: {}",
                    leftChildDistributionProperty, rightChildDistributionProperty, node);
            throw new IllegalStateException("Children output property distribution error.");
        }
    }

    private PhysicalPropertySet computeShuffleJoinOutputProperty(JoinOperator joinType,
                                                                 List<DistributionCol> leftShuffleColumns,
                                                                 List<DistributionCol> rightShuffleColumns) {
        Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleDesc();
        if (!requiredShuffleDesc.isPresent()) {
            return PhysicalPropertySet.EMPTY;
        }

        // Get required properties for children.
        List<PhysicalPropertySet> requiredProperties =
                computeShuffleJoinRequiredProperties(requirements, leftShuffleColumns, rightShuffleColumns);
        checkState(requiredProperties.size() == 2);

        List<DistributionCol> dominatedOutputColumns;

        if (joinType.isRightJoin()) {
            dominatedOutputColumns = ((HashDistributionSpec) requiredProperties.get(1).getDistributionProperty().getSpec())
                    .getShuffleColumns();
        } else if (joinType.isFullOuterJoin()) {
            dominatedOutputColumns = ((HashDistributionSpec) requiredProperties.get(0).getDistributionProperty().getSpec())
                    .getShuffleColumns();
            dominatedOutputColumns = dominatedOutputColumns.stream().map(e -> e.getNullRelaxCol()).collect(Collectors.toList());
        } else {
            dominatedOutputColumns = ((HashDistributionSpec) requiredProperties.get(0).getDistributionProperty().getSpec())
                    .getShuffleColumns();
        }
        HashDistributionSpec outputShuffleDistribution = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(dominatedOutputColumns, SHUFFLE_JOIN));

        return createPropertySetByDistribution(outputShuffleDistribution);
    }

    private Optional<HashDistributionDesc> getRequiredShuffleDesc() {
        if (!requirements.getDistributionProperty().isShuffle()) {
            return Optional.empty();
        }

        HashDistributionDesc requireDistributionDesc =
                ((HashDistributionSpec) requirements.getDistributionProperty().getSpec()).getHashDistributionDesc();
        HashDistributionDesc.SourceType requiredType = requireDistributionDesc.getSourceType();

        if (SHUFFLE_JOIN == requiredType || SHUFFLE_AGG == requiredType) {
            return Optional.of(requireDistributionDesc);
        }

        return Optional.empty();
    }

    @Override
    public PhysicalPropertySet visitPhysicalProject(PhysicalProjectOperator node, ExpressionContext context) {
        checkState(this.childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalHashAggregate(PhysicalHashAggregateOperator node,
                                                          ExpressionContext context) {
        checkState(this.childrenOutputProperties.size() == 1);
        // The child node meets the distribution attribute requirements for hash Aggregate nodes.
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalRepeat(PhysicalRepeatOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 1);
        PhysicalPropertySet childPropertySet = childrenOutputProperties.get(0);
        List<ColumnRefOperator> subRefs = Lists.newArrayList(node.getRepeatColumnRef().get(0));
        node.getRepeatColumnRef().forEach(subRefs::retainAll);
        Set<ColumnRefOperator> allGroupingRefs = Sets.newHashSet();

        node.getRepeatColumnRef().forEach(allGroupingRefs::addAll);
        subRefs.forEach(allGroupingRefs::remove);

        DistributionProperty childDistribution = childPropertySet.getDistributionProperty();
        // update null distribution info to null relax for allGroupingRefs
        if (!allGroupingRefs.isEmpty() && childDistribution.isShuffle()) {
            HashDistributionSpec distributionSpec = (HashDistributionSpec) childDistribution.getSpec();
            DistributionSpec.PropertyInfo newPropertyInfo = childDistribution.getSpec().getPropertyInfo().copy();
            newPropertyInfo.clearNullStrictDisjointSet();
            HashDistributionSpec newDistributionSpec = distributionSpec.getNullRelaxSpec(newPropertyInfo);
            DistributionProperty newDistributionProperty = childPropertySet.getDistributionProperty()
                    .copyWithSpec(newDistributionSpec);
            return new PhysicalPropertySet(newDistributionProperty, childPropertySet.getSortProperty(),
                    childPropertySet.getCteProperty());
        }
        return childPropertySet;
    }

    @Override
    public PhysicalPropertySet visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
        DistributionSpec olapDistributionSpec = node.getDistributionSpec();

        DistributionSpec.PropertyInfo physicalPropertyInfo = new DistributionSpec.PropertyInfo();

        physicalPropertyInfo.tableId = node.getTable().getId();
        physicalPropertyInfo.partitionIds = node.getSelectedPartitionId();

        if (olapDistributionSpec instanceof HashDistributionSpec) {
            return createPropertySetByDistribution(new HashDistributionSpec(
                    new HashDistributionDesc(((HashDistributionSpec) olapDistributionSpec).getShuffleColumns(),
                            HashDistributionDesc.SourceType.LOCAL), physicalPropertyInfo));
        } else {
            return createPropertySetByDistribution(olapDistributionSpec);
        }
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
        } else if (topN.getPartitionByColumns() == null) {
            outputProperty = PhysicalPropertySet.EMPTY;
        } else {
            List<Integer> partitionColumnRefSet = topN.getPartitionByColumns().stream()
                    .flatMap(c -> Arrays.stream(c.getUsedColumns().getColumnIds()).boxed())
                    .collect(Collectors.toList());
            if (partitionColumnRefSet.isEmpty()) {
                outputProperty = PhysicalPropertySet.EMPTY;
            } else {
                outputProperty = new PhysicalPropertySet(childrenOutputProperties.get(0).getDistributionProperty());
            }
        }
        return mergeCTEProperty(outputProperty);
    }

    @Override
    public PhysicalPropertySet visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 1);
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
        return new PhysicalPropertySet(distributionProperty, sortProperty,
                childrenOutputProperties.get(0).getCteProperty());
    }

    @Override
    public PhysicalPropertySet visitPhysicalFilter(PhysicalFilterOperator node, ExpressionContext context) {
        checkState(this.childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalTableFunction(PhysicalTableFunctionOperator node,
                                                          ExpressionContext context) {
        checkState(this.childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        DistributionSpec gather = DistributionSpec.createGatherDistributionSpec();
        DistributionProperty distributionProperty = new DistributionProperty(gather);
        return new PhysicalPropertySet(distributionProperty, SortProperty.EMPTY,
                childrenOutputProperties.get(0).getCteProperty());

    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 2);
        PhysicalPropertySet output = childrenOutputProperties.get(1).copy();
        CTEProperty cteProperty = childrenOutputProperties.get(1).getCteProperty().removeCTE(node.getCteId());
        cteProperty.merge(childrenOutputProperties.get(0).getCteProperty());
        output.setCteProperty(cteProperty);
        return output;
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, ExpressionContext context) {
        return new PhysicalPropertySet(DistributionProperty.EMPTY, SortProperty.EMPTY,
                new CTEProperty(node.getCteId()));
    }

    @Override
    public PhysicalPropertySet visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, ExpressionContext context) {
        if (SystemTable.isBeSchemaTable(node.getTable().getName())) {
            return createPropertySetByDistribution(new AnyDistributionSpec());
        } else {
            return createGatherPropertySet();
        }
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
