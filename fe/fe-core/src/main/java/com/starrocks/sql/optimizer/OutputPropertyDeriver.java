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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.connector.BucketProperty;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.base.CTEProperty;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.EmptyDistributionProperty;
import com.starrocks.sql.optimizer.base.EmptySortProperty;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionDescBP;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.starrocks.sql.optimizer.base.HashDistributionDesc.SourceType.LOCAL;
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
        PhysicalPropertySet result = groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        updatePropertyWithProjection(groupExpression.getOp().getProjection(), result);
        return result;
    }

    @NotNull
    private PhysicalPropertySet mergeCTEProperty(PhysicalPropertySet output) {
        // set cte property
        Set<Integer> cteIds = Sets.newHashSet();
        for (PhysicalPropertySet childrenOutputProperty : childrenOutputProperties) {
            cteIds.addAll(childrenOutputProperty.getCteProperty().getCteIds());
        }
        output = output.copy();
        output.setCteProperty(CTEProperty.createProperty(cteIds));
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
            dominatedOutputSpec = leftScanDistributionSpec.getNullRelaxSpec(leftScanDistributionSpec.getEquivDesc());
        } else {
            dominatedOutputSpec = leftScanDistributionSpec;
        }

        EquivalentDescriptor leftDesc = leftScanDistributionSpec.getEquivDesc();
        EquivalentDescriptor rightDesc = rightScanDistributionSpec.getEquivDesc();

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long leftTableId = leftDesc.getTableId();
        long rightTableId = rightDesc.getTableId();

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
                            dominatedOutputSpec.getEquivDesc()));
        }
    }

    private PhysicalPropertySet computeBucketJoinDistributionProperty(JoinOperator joinType,
                                                                      HashDistributionSpec hashDistributionSpec,
                                                                      PhysicalPropertySet physicalPropertySet) {
        if (!joinType.isFullOuterJoin() && !joinType.isRightOuterJoin()) {
            return physicalPropertySet;
        }
        HashDistributionSpec newSpec = hashDistributionSpec.getNullRelaxSpec(hashDistributionSpec.getEquivDesc());
        return new PhysicalPropertySet(DistributionProperty.createProperty(newSpec),
                physicalPropertySet.getSortProperty(), physicalPropertySet.getCteProperty());
    }

    private PhysicalPropertySet updateEquivalentDescriptor(PhysicalJoinOperator node,
                                                           PhysicalPropertySet physicalPropertySet,
                                                           List<DistributionCol> leftOnPredicateColumns,
                                                           List<DistributionCol> rightOnPredicateColumns) {
        // only HashDistributionSpec need update equivalentDescriptor
        if (!physicalPropertySet.getDistributionProperty().isShuffle()) {
            return physicalPropertySet;
        }

        HashDistributionSpec distributionSpec = (HashDistributionSpec) physicalPropertySet.getDistributionProperty().getSpec();

        EquivalentDescriptor equivDesc = distributionSpec.getEquivDesc();

        JoinOperator joinOperator = node.getJoinType();
        if (joinOperator.isAnyInnerJoin()) {
            for (int i = 0; i < leftOnPredicateColumns.size(); i++) {
                DistributionCol leftCol = leftOnPredicateColumns.get(i);
                DistributionCol rightCol = rightOnPredicateColumns.get(i);
                equivDesc.unionDistributionCols(leftCol, rightCol);
            }
        } else if (joinOperator.isAnyLeftOuterJoin() || joinOperator.isRightOuterJoin()) {
            for (int i = 0; i < leftOnPredicateColumns.size(); i++) {
                DistributionCol leftCol = leftOnPredicateColumns.get(i);
                DistributionCol rightCol = rightOnPredicateColumns.get(i);
                equivDesc.unionNullRelaxCols(leftCol, rightCol);
            }
        } else if (joinOperator.isFullOuterJoin()) {
            equivDesc.clearNullStrictUnionFind();
            for (int i = 0; i < leftOnPredicateColumns.size(); i++) {
                DistributionCol leftCol = leftOnPredicateColumns.get(i);
                DistributionCol rightCol = rightOnPredicateColumns.get(i);
                equivDesc.unionNullRelaxCols(leftCol, rightCol);
            }
        }

        return physicalPropertySet;
    }

    @Override
    public PhysicalPropertySet visitPhysicalUnion(PhysicalUnionOperator node, ExpressionContext context) {
        return processPhysicalSetOperation(node, context);
    }

    @Override
    public PhysicalPropertySet visitPhysicalExcept(PhysicalExceptOperator node, ExpressionContext context) {
        return processPhysicalSetOperation(node, context);
    }

    @Override
    public PhysicalPropertySet visitPhysicalIntersect(PhysicalIntersectOperator node, ExpressionContext context) {
        return processPhysicalSetOperation(node, context);
    }

    private PhysicalPropertySet processPhysicalSetOperation(PhysicalSetOperation node, ExpressionContext context) {
        DistributionSpec inputDistSpec = childrenOutputProperties.get(0).getDistributionProperty().getSpec();
        if (inputDistSpec.getType().equals(DistributionSpec.DistributionType.ROUND_ROBIN)) {
            return visitOperator(node, context);
        }

        if (!(inputDistSpec instanceof HashDistributionSpec)) {
            return visitOperator(node, context);
        }

        HashDistributionSpec inputHashDistSpec = (HashDistributionSpec) inputDistSpec;
        HashDistributionDesc inputHashDistDesc = inputHashDistSpec.getHashDistributionDesc();

        if (inputHashDistDesc.isLocal()) {
            List<DistributionCol> inputColumns = node.getChildOutputColumns().get(0)
                    .stream()
                    .map(col -> new DistributionCol(col.getId(), true))
                    .collect(Collectors.toList());

            List<DistributionCol> outputColumns = node.getOutputColumnRefOp()
                    .stream()
                    .map(col -> new DistributionCol(col.getId(), true))
                    .collect(Collectors.toList());

            List<List<DistributionCol>> outputShuffleColumns = Lists.newArrayList();
            for (int i = 0; i < inputHashDistDesc.getDistributionCols().size(); ++i) {
                DistributionCol inputCol = inputHashDistDesc.getDistributionCols().get(i);
                List<DistributionCol> outputShuffleCols = IntStream.range(0, inputColumns.size())
                        .filter(k -> inputHashDistSpec.getEquivDesc().isConnected(inputCol, inputColumns.get(k)))
                        .mapToObj(outputColumns::get)
                        .collect(Collectors.toList());
                outputShuffleColumns.add(outputShuffleCols);
            }
            Preconditions.checkArgument(outputShuffleColumns.stream().allMatch(cols -> cols.size() > 0));
            List<Integer> columnIds = outputShuffleColumns.stream()
                    .map(cols -> cols.get(0).getColId())
                    .collect(Collectors.toList());

            HashDistributionDesc hashDistDesc = new HashDistributionDesc(columnIds, LOCAL);
            EquivalentDescriptor equivalentDescriptor = new EquivalentDescriptor(
                    inputHashDistSpec.getEquivDesc().getTableId(), inputHashDistSpec.getEquivDesc().getPartitionIds());

            List<DistributionCol> shuffleColumns = outputShuffleColumns.stream()
                    .map(cols -> cols.get(0))
                    .collect(Collectors.toList());

            equivalentDescriptor.initDistributionUnionFind(shuffleColumns);
            outputShuffleColumns.forEach(columns -> columns.stream().skip(1)
                    .forEach(col -> equivalentDescriptor.unionDistributionCols(columns.get(0), col)));

            HashDistributionSpec hashDistSpec = new HashDistributionSpec(hashDistDesc, equivalentDescriptor);
            PhysicalPropertySet propertySet =
                    new PhysicalPropertySet(DistributionProperty.createProperty(hashDistSpec));
            return mergeCTEProperty(propertySet);
        } else {
            List<Integer> columnIds = node.getOutputColumnRefOp().stream()
                    .map(ColumnRefOperator::getId)
                    .collect(Collectors.toList());
            HashDistributionDesc hashDistDesc = new HashDistributionDesc(columnIds, SHUFFLE_JOIN);
            HashDistributionSpec hashDistSpec = DistributionSpec.createHashDistributionSpec(hashDistDesc);
            PhysicalPropertySet propertySet =
                    new PhysicalPropertySet(DistributionProperty.createProperty(hashDistSpec));
            return mergeCTEProperty(propertySet);
        }
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

    private PhysicalPropertySet resetSortProperty(PhysicalPropertySet propertySet) {
        final SessionVariable sv = ConnectContext.get().getSessionVariable();
        // Spill partition join and partition join operations break ordering.
        // When these optimizations are enabled, we need to clear the property.
        boolean needResetSortProperty = sv.isEnableSpill() || sv.enablePartitionHashJoin();
        if (needResetSortProperty) {
            propertySet =
                    new PhysicalPropertySet(propertySet.getDistributionProperty(), EmptySortProperty.INSTANCE,
                            propertySet.getCteProperty());
        }
        return propertySet;
    }

    private PhysicalPropertySet visitPhysicalJoin(PhysicalJoinOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 2);
        PhysicalPropertySet leftChildOutputProperty = childrenOutputProperties.get(0);
        PhysicalPropertySet rightChildOutputProperty = childrenOutputProperties.get(1);

        // 1. Distribution is broadcast
        if (rightChildOutputProperty.getDistributionProperty().isBroadcast()) {
            return resetSortProperty(leftChildOutputProperty);
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
                PhysicalPropertySet outputProperty = computeColocateJoinOutputProperty(node.getJoinType(),
                        leftDistributionSpec, rightDistributionSpec);
                return updateEquivalentDescriptor(node, outputProperty,
                        leftOnPredicateColumns, rightOnPredicateColumns);

            } else if (leftDistributionDesc.isLocal() && rightDistributionDesc.isBucketJoin()) {
                // bucket join
                PhysicalPropertySet outputProperty = computeBucketJoinDistributionProperty(node.getJoinType(),
                        leftDistributionSpec, leftChildOutputProperty);
                outputProperty = resetSortProperty(outputProperty);
                return updateEquivalentDescriptor(node, outputProperty,
                        leftOnPredicateColumns, rightOnPredicateColumns);

            } else if ((leftDistributionDesc.isShuffle() || leftDistributionDesc.isShuffleEnforce()) &&
                    (rightDistributionDesc.isShuffle()) || rightDistributionDesc.isShuffleEnforce()) {
                // shuffle join
                PhysicalPropertySet outputProperty = computeShuffleJoinOutputProperty(node.getJoinType(),
                        leftDistributionDesc.getDistributionCols(), rightDistributionDesc.getDistributionCols());
                return updateEquivalentDescriptor(node, outputProperty,
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

        // calculate the Intersection of RepeatColumnRef
        List<ColumnRefOperator> subRefs = Lists.newArrayList(node.getRepeatColumnRef().get(0));
        node.getRepeatColumnRef().forEach(subRefs::retainAll);

        DistributionProperty childDistribution = childPropertySet.getDistributionProperty();
        // only if the Intersection of RepeatColumnRef is the superset of the childrenOutputProperties
        // we can use childrenOutputProperties as RepeatNode's output property
        // such as RepeatColumnRef is (cola,colb),(cola), and childrenOutputProperties is hash(cola)
        // since cola won't be inserted with null value, it's safe to use childrenOutputProperties
        // if RepeatColumnRef is (cola,colb),(cola), and childrenOutputProperties is hash(cola,colb)
        // since cola will be inserted with null value, it's unsafe to use hash(cola,colb)
        DistributionProperty outputDistribution = EmptyDistributionProperty.INSTANCE;
        if (childDistribution.isShuffle()) {
            boolean canFollowChild = true;
            HashDistributionSpec childDistributionSpec = (HashDistributionSpec) childDistribution.getSpec();
            Set<Integer> commonRefs = subRefs.stream().map(ColumnRefOperator::getId).collect(Collectors.toSet());

            for (DistributionCol col : childDistributionSpec.getHashDistributionDesc().getDistributionCols()) {
                if (!commonRefs.contains(col.getColId())) {
                    canFollowChild = false;
                    break;
                }
            }

            if (canFollowChild) {
                outputDistribution = childDistribution;
            }
        }

        return new PhysicalPropertySet(outputDistribution, childPropertySet.getSortProperty(),
                childPropertySet.getCteProperty());
    }

    @Override
    public PhysicalPropertySet visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
        DistributionSpec olapDistributionSpec = node.getDistributionSpec();

        if (olapDistributionSpec instanceof HashDistributionSpec) {
            EquivalentDescriptor equivDesc = new EquivalentDescriptor(node.getTable().getId(), node.getSelectedPartitionId());
            return createPropertySetByDistribution(new HashDistributionSpec(
                    new HashDistributionDesc(((HashDistributionSpec) olapDistributionSpec).getShuffleColumns(),
                            HashDistributionDesc.SourceType.LOCAL), equivDesc));
        } else if (olapDistributionSpec.getType() == DistributionSpec.DistributionType.ANY) {
            return PhysicalPropertySet.EMPTY;
        } else {
            return createPropertySetByDistribution(olapDistributionSpec);
        }
    }

    private Optional<HashDistributionDesc> computeLakeHashDistributionDesc(HashDistributionDesc require,
                                                                           List<BucketProperty> bucketProperties,
                                                                           Map<ColumnRefOperator, Column> map) {
        ColumnRefSet requireColumnRefSet = ColumnRefSet.createByIds(
                require.getDistributionCols().stream().map(DistributionCol::getColId).toList());

        List<Integer> bucketColumnIds = new ArrayList<>();
        Map<Integer, Integer> id2Index = new HashMap<>();
        for (int i = 0; i < bucketProperties.size(); i++) {
            Column column = bucketProperties.get(i).getColumn();
            for (Map.Entry<ColumnRefOperator, Column> entry : map.entrySet()) {
                if (entry.getKey().getName().equals(column.getName())) {
                    bucketColumnIds.add(entry.getKey().getId());
                    id2Index.put(entry.getKey().getId(), i);
                    break;
                }
            }
        }
        ColumnRefSet bucketColumnRefSet = ColumnRefSet.createByIds(bucketColumnIds);
        requireColumnRefSet.intersect(bucketColumnRefSet);
        if (requireColumnRefSet.isEmpty()) {
            return Optional.empty();
        } else {
            // respect the order of column shuffle
            List<BucketProperty> usedBP = require.getDistributionCols().stream()
                    .map(DistributionCol::getColId).filter(requireColumnRefSet::contains)
                    .map(id2Index::get).map(bucketProperties::get).toList();
            return Optional.of(new HashDistributionDescBP(
                    requireColumnRefSet.getStream().toList(), LOCAL, usedBP));
        }
    }

    @Override
    public PhysicalPropertySet visitPhysicalIcebergScan(PhysicalIcebergScanOperator node, ExpressionContext context) {
        // according bucket properties to compute distribution that meet requirement
        DistributionSpec distributionSpec = requirements.getDistributionProperty().getSpec();
        if (ConnectContext.get().getSessionVariable().isEnableBucketAwareExecutionOnLake() &&
                distributionSpec instanceof HashDistributionSpec hashDistribution) {
            IcebergTable table = (IcebergTable) node.getTable();
            if (table.hasBucketProperties()) {
                List<BucketProperty> properties = table.getBucketProperties();
                Optional<HashDistributionDesc> hashDistributionDesc = computeLakeHashDistributionDesc(
                        hashDistribution.getHashDistributionDesc(), properties, node.getColRefToColumnMetaMap());
                if (hashDistributionDesc.isPresent()) {
                    HashDistributionDesc nullStrictDesc = hashDistributionDesc.get().getNullStrictDesc();
                    return createPropertySetByDistribution(new HashDistributionSpec(nullStrictDesc));
                }
            }
            LOG.debug("table name: " + node.getTable().getName() + ", requirement distribution type: " +
                    distributionSpec.toString());
        }
        return mergeCTEProperty(PhysicalPropertySet.EMPTY);
    }

    @Override
    public PhysicalPropertySet visitPhysicalTopN(PhysicalTopNOperator topN, ExpressionContext context) {
        PhysicalPropertySet outputProperty;
        if (topN.getSortPhase().isFinal()) {
            if (topN.isSplit()) {
                DistributionSpec distributionSpec = DistributionSpec.createGatherDistributionSpec();
                DistributionProperty distributionProperty = DistributionProperty.createProperty(distributionSpec);
                SortProperty sortProperty = SortProperty.createProperty(topN.getOrderSpec().getOrderDescs());
                outputProperty = new PhysicalPropertySet(distributionProperty, sortProperty);
            } else {
                outputProperty = new PhysicalPropertySet(SortProperty.createProperty(topN.getOrderSpec().getOrderDescs()));
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
                Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().toList()));

        SortProperty sortProperty = SortProperty.createProperty(node.getEnforceOrderBy(), partitionColumnRefSet);

        DistributionProperty distributionProperty;
        if (partitionColumnRefSet.isEmpty()) {
            distributionProperty = DistributionProperty.createProperty(DistributionSpec.createGatherDistributionSpec());
        } else {
            // Use child's distribution
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
        DistributionProperty distributionProperty = DistributionProperty.createProperty(gather);
        return new PhysicalPropertySet(distributionProperty, EmptySortProperty.INSTANCE,
                childrenOutputProperties.get(0).getCteProperty());

    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 2);
        PhysicalPropertySet output = childrenOutputProperties.get(1).copy();
        Set<Integer> cteIds = Sets.newHashSet(childrenOutputProperties.get(1).getCteProperty().getCteIds());
        cteIds.remove(node.getCteId());
        cteIds.addAll(childrenOutputProperties.get(0).getCteProperty().getCteIds());
        output.setCteProperty(CTEProperty.createProperty(cteIds));
        return output;
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
        checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, ExpressionContext context) {
        return new PhysicalPropertySet(EmptyDistributionProperty.INSTANCE, EmptySortProperty.INSTANCE,
                new CTEProperty(node.getCteId()));
    }

    @Override
    public PhysicalPropertySet visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalPropertySet visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, ExpressionContext context) {
        if (SystemTable.isBeSchemaTable(node.getTable().getName())) {
            return PhysicalPropertySet.EMPTY;
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

    @Override
    public PhysicalPropertySet visitPhysicalRawValues(PhysicalRawValuesOperator node, ExpressionContext context) {
        return createGatherPropertySet();
    }

    private void updatePropertyWithProjection(Projection projection, PhysicalPropertySet oldProperty) {
        if (projection == null) {
            return;
        }

        if (!(oldProperty.getDistributionProperty().getSpec() instanceof HashDistributionSpec)) {
            return;
        }

        HashDistributionSpec distributionSpec =
                (HashDistributionSpec) oldProperty.getDistributionProperty().getSpec();
        final Map<Integer, DistributionCol> idToDistributionCol = Maps.newHashMap();
        distributionSpec.getShuffleColumns().forEach(e -> idToDistributionCol.put(e.getColId(), e));

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
            ColumnRefSet usedCols = entry.getValue().getUsedColumns();

            Optional<Boolean> nullStrictInfo = remainDistributionFunc(entry.getValue(), idToDistributionCol);
            if (nullStrictInfo.isPresent()) {
                if (nullStrictInfo.get()) {
                    distributionSpec.getEquivDesc().unionDistributionCols(
                            new DistributionCol(entry.getKey().getId(), true),
                            idToDistributionCol.get(usedCols.getFirstId())
                    );
                } else {
                    distributionSpec.getEquivDesc().unionNullRelaxCols(
                            new DistributionCol(entry.getKey().getId(), false),
                            idToDistributionCol.get(usedCols.getFirstId()).getNullRelaxCol()
                    );
                }

            }
        }
    }

    private Optional<Boolean> remainDistributionFunc(ScalarOperator scalarOperator,
                                                     Map<Integer, DistributionCol> idToDistributionCol) {
        DistributionCol col;
        if (scalarOperator.isColumnRef()
                && (col = idToDistributionCol.get(scalarOperator.getUsedColumns().getFirstId())) != null) {
            return Optional.of(col.isNullStrict());
        }

        // todo support if(col = x, col, null)

        return Optional.empty();
    }
}
