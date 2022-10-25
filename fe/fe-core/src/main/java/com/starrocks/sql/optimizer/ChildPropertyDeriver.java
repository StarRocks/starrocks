// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.GatherDistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.OutputInputProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
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
import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.isColumnToColumnBinaryPredicate;

/**
 * Get output and child input property for one physical operator
 * <p>
 * During query optimization, the optimizer looks for a physical plan that
 * satisfies a number of requirements. These requirements may be specified
 * on the query level such as an ORDER BY clause that specifies a required
 * sort order to be satisfied by query results.
 * Alternatively, the requirements may be triggered when
 * optimizing a plan subtree. For example, a Hash Join operator may
 * require its children to have hash distributions aligned with the
 * columns mentioned in join condition.
 * <p>
 * In either case, each operator receives requirements from the parent and
 * combines these requirements with local requirements to generate new
 * requirements (which may be empty) from each of its children.
 */
public class ChildPropertyDeriver extends OperatorVisitor<Void, ExpressionContext> {
    private PhysicalPropertySet requirements;
    private List<OutputInputProperty> outputInputProps;
    private final TaskContext taskContext;
    private final OptimizerContext context;

    public ChildPropertyDeriver(TaskContext taskContext) {
        this.taskContext = taskContext;
        this.context = taskContext.getOptimizerContext();
    }

    public List<OutputInputProperty> getOutputInputProps(
            PhysicalPropertySet requirements,
            GroupExpression groupExpression) {
        this.requirements = requirements;

        outputInputProps = Lists.newArrayList();
        groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        return outputInputProps;
    }

    private PhysicalPropertySet distributeRequirements() {
        return new PhysicalPropertySet(requirements.getDistributionProperty());
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        String hint = node.getJoinHint();

        // 1 For broadcast join
        PhysicalPropertySet rightBroadcastProperty =
                new PhysicalPropertySet(new DistributionProperty(DistributionSpec.createReplicatedDistributionSpec()));
        outputInputProps.add(OutputInputProperty.emptyOutputOf(PhysicalPropertySet.EMPTY, rightBroadcastProperty));

        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);
        List<BinaryPredicateOperator> equalOnPredicate =
                getEqConj(leftChildColumns, rightChildColumns, Utils.extractConjuncts(node.getOnPredicate()));

        if (Utils.canOnlyDoBroadcast(node, equalOnPredicate, hint)) {
            tryReplicatedCrossJoin(context);
            return visitJoinRequirements(node, context, false);
        }

        if (node.getJoinType().isRightJoin() || node.getJoinType().isFullOuterJoin()
                || "SHUFFLE".equalsIgnoreCase(hint)) {
            outputInputProps.clear();
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

        doHashShuffle(leftDistribution, rightDistribution);

        // Respect use join hint
        if ("SHUFFLE".equalsIgnoreCase(hint)) {
            return visitJoinRequirements(node, context, false);
        }

        // Colocate join and bucket shuffle join only support column to column binary predicate
        if (equalOnPredicate.stream().anyMatch(p -> !isColumnToColumnBinaryPredicate(p))) {
            tryReplicatedHashJoin(context, node.getJoinType(), leftDistribution);
            return visitJoinRequirements(node, context, false);
        }

        boolean canDoColocatedJoin = false;
        // 3 For colocate join
        if (!"BUCKET".equalsIgnoreCase(hint)) {
            canDoColocatedJoin = tryColocate(leftDistribution, rightDistribution);
            if (!canDoColocatedJoin) {
                tryReplicatedHashJoin(context, node.getJoinType(), leftDistribution);
            }
        }

        // 4 For bucket shuffle join
        tryBucketShuffle(node, leftDistribution, rightDistribution);

        // 5 resolve requirements
        return visitJoinRequirements(node, context, canDoColocatedJoin);
    }

    private void doHashShuffle(HashDistributionSpec leftDistribution, HashDistributionSpec rightDistribution) {
        // shuffle
        PhysicalPropertySet leftInputProperty = createPropertySetByDistribution(leftDistribution);
        PhysicalPropertySet rightInputProperty = createPropertySetByDistribution(rightDistribution);

        Optional<HashDistributionDesc> requiredShuffleDesc = getRequiredShuffleJoinDesc();
        if (!requiredShuffleDesc.isPresent()) {
            outputInputProps.add(OutputInputProperty.emptyOutputOf(leftInputProperty, rightInputProperty));
            return;
        }

        // try shuffle_hash_bucket
        HashDistributionDesc requiredDesc = requiredShuffleDesc.get();

        List<Integer> leftColumns = leftDistribution.getShuffleColumns();
        List<Integer> rightColumns = rightDistribution.getShuffleColumns();
        List<Integer> requiredColumns = requiredDesc.getColumns();

        Preconditions.checkState(leftColumns.size() == rightColumns.size());
        // Hash shuffle columns must keep same
        boolean checkLeft = leftColumns.containsAll(requiredColumns) && leftColumns.size() == requiredColumns.size();
        boolean checkRight = rightColumns.containsAll(requiredColumns) && rightColumns.size() == requiredColumns.size();

        // @Todo: Modify PlanFragmentBuilder to support complex query
        // Different joins maybe different on-clause predicate order, so the order of shuffle key is different,
        // and unfortunately PlanFragmentBuilder doesn't support adjust the order of join shuffle key,
        // so we must check the shuffle order strict
        if (checkLeft || checkRight) {
            for (int i = 0; i < requiredColumns.size(); i++) {
                checkLeft &= requiredColumns.get(i).equals(leftColumns.get(i));
                checkRight &= requiredColumns.get(i).equals(rightColumns.get(i));
            }
        }

        if (checkLeft || checkRight) {
            // Adjust hash shuffle columns orders follow requirement
            List<Integer> requiredLeft = Lists.newArrayList();
            List<Integer> requiredRight = Lists.newArrayList();

            for (Integer cid : requiredColumns) {
                int idx = checkLeft ? leftColumns.indexOf(cid) : rightColumns.indexOf(cid);
                requiredLeft.add(leftColumns.get(idx));
                requiredRight.add(rightColumns.get(idx));
            }

            PhysicalPropertySet leftShuffleProperty = createPropertySetByDistribution(
                    DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(requiredLeft,
                            HashDistributionDesc.SourceType.SHUFFLE_JOIN)));

            PhysicalPropertySet rightShuffleProperty = createPropertySetByDistribution(
                    DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(requiredRight,
                            HashDistributionDesc.SourceType.SHUFFLE_JOIN)));

            outputInputProps
                    .add(OutputInputProperty.of(distributeRequirements(), leftShuffleProperty, rightShuffleProperty));
            return;
        }

        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, leftInputProperty, rightInputProperty));
    }

    /*
     * Colocate will required children support local properties by topdown
     * All shuffle columns(predicate columns) must come from one table
     * Can't support case such as (The predicate columns combinations is N*N):
     *          JOIN(s1.A = s3.A AND s2.A = s4.A)
     *             /            \
     *          JOIN           JOIN
     *         /    \         /    \
     *        s1    s2       s3     s4
     *
     * support case:
     *          JOIN(s1.A = s3.A AND s1.B = s3.B)
     *             /            \
     *          JOIN           JOIN
     *         /    \         /    \
     *        s1    s2       s3     s4
     *
     * */
    private boolean tryColocate(HashDistributionSpec leftShuffleDistribution,
                                HashDistributionSpec rightShuffleDistribution) {
        if (ConnectContext.get().getSessionVariable().isDisableColocateJoin()) {
            return false;
        }

        Optional<LogicalOlapScanOperator> leftTable = findLogicalOlapScanOperator(leftShuffleDistribution);
        if (!leftTable.isPresent()) {
            return false;
        }

        LogicalOlapScanOperator left = leftTable.get();

        Optional<LogicalOlapScanOperator> rightTable = findLogicalOlapScanOperator(rightShuffleDistribution);
        if (!rightTable.isPresent()) {
            return false;
        }

        LogicalOlapScanOperator right = rightTable.get();
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();

        // join self
        if (left.getTable().getId() == right.getTable().getId() &&
                !colocateIndex.isSameGroup(left.getTable().getId(), right.getTable().getId())) {
            if (!left.getSelectedPartitionId().equals(right.getSelectedPartitionId())
                    || left.getSelectedPartitionId().size() > 1) {
                return false;
            }

            PhysicalPropertySet rightLocalProperty = createPropertySetByDistribution(
                    createLocalByByHashColumns(rightShuffleDistribution.getShuffleColumns()));
            PhysicalPropertySet leftLocalProperty = createPropertySetByDistribution(
                    createLocalByByHashColumns(leftShuffleDistribution.getShuffleColumns()));
            outputInputProps.add(OutputInputProperty.emptyOutputOf(leftLocalProperty, rightLocalProperty));
        } else {
            // colocate group
            if (!colocateIndex.isSameGroup(left.getTable().getId(), right.getTable().getId())) {
                return false;
            }

            ColocateTableIndex.GroupId groupId = colocateIndex.getGroup(left.getTable().getId());
            if (colocateIndex.isGroupUnstable(groupId)) {
                return false;
            }

            HashDistributionSpec leftScanDistribution = left.getDistributionSpec();
            HashDistributionSpec rightScanDistribution = right.getDistributionSpec();

            Preconditions.checkState(leftScanDistribution.getShuffleColumns().size() ==
                    rightScanDistribution.getShuffleColumns().size());

            if (!leftShuffleDistribution.getShuffleColumns().containsAll(leftScanDistribution.getShuffleColumns())) {
                return false;
            }

            if (!rightShuffleDistribution.getShuffleColumns().containsAll(rightScanDistribution.getShuffleColumns())) {
                return false;
            }

            // check orders of predicate columns is right
            // check predicate columns is satisfy bucket hash columns
            for (int i = 0; i < leftScanDistribution.getShuffleColumns().size(); i++) {
                int leftScanColumnId = leftScanDistribution.getShuffleColumns().get(i);
                int leftIndex = leftShuffleDistribution.getShuffleColumns().indexOf(leftScanColumnId);

                int rightScanColumnId = rightScanDistribution.getShuffleColumns().get(i);
                int rightIndex = rightShuffleDistribution.getShuffleColumns().indexOf(rightScanColumnId);

                if (leftIndex != rightIndex) {
                    return false;
                }
            }

            PhysicalPropertySet rightLocalProperty = createPropertySetByDistribution(
                    createLocalByByHashColumns(rightScanDistribution.getShuffleColumns()));
            PhysicalPropertySet leftLocalProperty = createPropertySetByDistribution(
                    createLocalByByHashColumns(leftScanDistribution.getShuffleColumns()));

            outputInputProps.add(OutputInputProperty.emptyOutputOf(leftLocalProperty, rightLocalProperty));
        }
        return true;
    }

    private void tryReplicatedCrossJoin(ExpressionContext context) {
        if (!ConnectContext.get().getSessionVariable().isEnableReplicationJoin()) {
            return;
        }

        // For simplicity, only support replicated join when left child has one olap scan
        GroupExpression leftChild = context.getChildGroupExpression(0);
        List<LogicalOlapScanOperator> leftScanLists = Lists.newArrayList();
        Utils.extractOlapScanOperator(leftChild, leftScanLists);
        if (leftScanLists.size() != 1) {
            return;
        }

        GroupExpression rightChild = context.getChildGroupExpression(1);
        List<LogicalScanOperator> rightScanLists = Lists.newArrayList();
        Utils.extractScanOperator(rightChild, rightScanLists);
        if (rightScanLists.size() != 1) {
            return;
        }

        LogicalScanOperator rightScan = rightScanLists.get(0);
        if (rightScan instanceof LogicalOlapScanOperator &&
                ((LogicalOlapScanOperator) rightScan).canDoReplicatedJoin()) {
            generatePropertiesForReplicated(leftScanLists.get(0), (LogicalOlapScanOperator) rightScan);
        }
    }

    private void generatePropertiesForReplicated(LogicalOlapScanOperator leftScan,
                                                 LogicalOlapScanOperator rightScan) {
        HashDistributionSpec leftScanDistribution = leftScan.getDistributionSpec();
        HashDistributionSpec rightScanDistribution = rightScan.getDistributionSpec();
        PhysicalPropertySet leftLocalProperty = createPropertySetByDistribution(
                createLocalByByHashColumns(leftScanDistribution.getShuffleColumns()));
        PhysicalPropertySet rightLocalProperty = createPropertySetByDistribution(
                createLocalByByHashColumns(rightScanDistribution.getShuffleColumns()));
        // For query schedule, we need left is hash local
        outputInputProps.add(OutputInputProperty.emptyOutputOf(leftLocalProperty, rightLocalProperty));
    }

    private void tryReplicatedHashJoin(ExpressionContext context,
                                       JoinOperator joinType,
                                       HashDistributionSpec leftShuffleDistribution) {
        if (!ConnectContext.get().getSessionVariable().isEnableReplicationJoin()) {
            return;
        }

        // Right join or full outer join couldn't do replicated join
        if (joinType.isRightJoin() || joinType.isFullOuterJoin()) {
            return;
        }

        Optional<LogicalOlapScanOperator> leftTable = findLogicalOlapScanOperator(leftShuffleDistribution);
        if (!leftTable.isPresent()) {
            return;
        }

        GroupExpression rightChild = context.getChildGroupExpression(1);
        List<LogicalScanOperator> scanLists = Lists.newArrayList();
        Utils.extractScanOperator(rightChild, scanLists);

        if (scanLists.size() != 1) {
            return;
        }

        LogicalScanOperator right = scanLists.get(0);
        if (right instanceof LogicalOlapScanOperator && ((LogicalOlapScanOperator) right).canDoReplicatedJoin()) {
            generatePropertiesForReplicated(leftTable.get(), (LogicalOlapScanOperator) right);
        }
    }

    /*
     * Bucket-shuffle will required left-children support local properties by topdown
     * All shuffle columns(predicate columns) must come from one table
     * Can't support case such as (The predicate columns combinations is N*N):
     *          JOIN(s1.A = s3.A AND s2.A = s4.A)
     *             /            \
     *          JOIN           JOIN
     *         /    \         /    \
     *        s1    s2       s3     s4
     *
     * support case:
     *          JOIN(s1.A = s3.A AND s1.B = s3.B)
     *             /            \
     *          JOIN           JOIN
     *         /    \         /    \
     *        s1    s2       s3     s4
     *
     * */
    private void tryBucketShuffle(PhysicalHashJoinOperator node, HashDistributionSpec leftShuffleDistribution,
                                  HashDistributionSpec rightShuffleDistribution) {
        JoinOperator nodeJoinType = node.getJoinType();
        if (nodeJoinType.isCrossJoin()) {
            return;
        }

        Optional<LogicalOlapScanOperator> leftTable = findLogicalOlapScanOperator(leftShuffleDistribution);

        if (!leftTable.isPresent()) {
            return;
        }

        LogicalOlapScanOperator left = leftTable.get();

        // Could only do bucket shuffle when partition size is 1, less 1 will case coordinator throw bugs
        if (left.getSelectedPartitionId().size() != 1) {
            return;
        }

        HashDistributionSpec leftScanDistribution = left.getDistributionSpec();

        // shuffle column check
        if (!leftShuffleDistribution.getShuffleColumns().containsAll(leftScanDistribution.getShuffleColumns())) {
            return;
        }

        // right table shuffle columns
        List<Integer> rightBucketShuffleColumns = Lists.newArrayList();

        for (int leftScanColumn : leftScanDistribution.getShuffleColumns()) {
            int index = leftShuffleDistribution.getShuffleColumns().indexOf(leftScanColumn);
            rightBucketShuffleColumns.add(rightShuffleDistribution.getShuffleColumns().get(index));
        }

        // left table local columns
        List<Integer> leftLocalColumns = Lists.newArrayList(leftScanDistribution.getShuffleColumns());

        PhysicalPropertySet rightBucketShuffleProperty = createPropertySetByDistribution(
                DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(rightBucketShuffleColumns,
                        HashDistributionDesc.SourceType.BUCKET_JOIN)));
        PhysicalPropertySet leftLocalProperty =
                createPropertySetByDistribution(createLocalByByHashColumns(leftLocalColumns));

        outputInputProps.add(OutputInputProperty.emptyOutputOf(leftLocalProperty, rightBucketShuffleProperty));
    }

    private Optional<LogicalOlapScanOperator> findLogicalOlapScanOperator(HashDistributionSpec distributionSpec) {
        /*
         * All shuffle columns must come from one table
         * */
        List<ColumnRefOperator> shuffleColumns =
                distributionSpec.getShuffleColumns().stream().map(d -> context.getColumnRefFactory().getColumnRef(d))
                        .collect(Collectors.toList());

        for (LogicalOlapScanOperator scanOperator : taskContext.getAllScanOperators()) {
            if (scanOperator.getOutputColumns().containsAll(shuffleColumns)) {
                return Optional.of(scanOperator);
            }
        }
        return Optional.empty();
    }

    private HashDistributionSpec createLocalByByHashColumns(List<Integer> hashColumns) {
        HashDistributionDesc hashDesc = new HashDistributionDesc(hashColumns, HashDistributionDesc.SourceType.LOCAL);
        return DistributionSpec.createHashDistributionSpec(hashDesc);
    }

    private Optional<HashDistributionDesc> getRequiredLocalDesc() {
        if (!requirements.getDistributionProperty().isShuffle()) {
            return Optional.empty();
        }

        HashDistributionDesc requireDistributionDesc =
                ((HashDistributionSpec) requirements.getDistributionProperty().getSpec()).getHashDistributionDesc();
        if (!HashDistributionDesc.SourceType.LOCAL.equals(requireDistributionDesc.getSourceType())) {
            return Optional.empty();
        }

        return Optional.of(requireDistributionDesc);
    }

    private Optional<GatherDistributionSpec> getRequiredGatherDesc() {
        if (!requirements.getDistributionProperty().isGather()) {
            return Optional.empty();
        }

        GatherDistributionSpec requireDistributionDesc =
                ((GatherDistributionSpec) requirements.getDistributionProperty().getSpec());

        return Optional.of(requireDistributionDesc);
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

    private Void tryGatherForBroadcastJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        List<OutputInputProperty> result = Lists.newArrayList();
        if (context.getChildLogicalProperty(0).isGatherToOneInstance()) {
            for (OutputInputProperty outputInputProp : outputInputProps) {
                PhysicalPropertySet left = outputInputProp.getInputProperty(0);
                PhysicalPropertySet right = outputInputProp.getInputProperty(1);
                if (left.getDistributionProperty().isAny() && right.getDistributionProperty().isBroadcast()) {
                    result.add(OutputInputProperty.of(distributeRequirements(), distributeRequirements(), right));
                } else {
                    result.add(outputInputProp);
                }
            }
            outputInputProps = result;
            return visitOperator(node, context);
        }
        return visitOperator(node, context);
    }

    private Void visitJoinRequirements(PhysicalHashJoinOperator node, ExpressionContext context,
                                       boolean canDoColocate) {
        //require property is gather
        Optional<GatherDistributionSpec> requiredGatherDistribution = getRequiredGatherDesc();
        if (requiredGatherDistribution.isPresent()) {
            return tryGatherForBroadcastJoin(node, context);
        }

        Optional<HashDistributionDesc> required = getRequiredLocalDesc();
        if (!required.isPresent()) {
            return visitOperator(node, context);
        }
        //require property is local
        HashDistributionDesc requireDistributionDesc = required.get();
        ColumnRefSet requiredLocalColumns = new ColumnRefSet();
        requireDistributionDesc.getColumns().forEach(requiredLocalColumns::union);

        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);

        boolean requiredLocalColumnsFromLeft = leftChildColumns.containsAll(requiredLocalColumns);
        boolean requiredLocalColumnsFromRight = rightChildColumns.containsAll(requiredLocalColumns);
        boolean isLeftOrFullJoin = node.getJoinType().isLeftOuterJoin() || node.getJoinType().isFullOuterJoin();
        boolean isRightOrFullJoin = node.getJoinType().isRightOuterJoin() || node.getJoinType().isFullOuterJoin();

        // 1. Not support local shuffle column appear on both sides at the same time
        // 2. Left outer join will cause right table produce NULL in different node, also right outer join
        if ((requiredLocalColumnsFromLeft == requiredLocalColumnsFromRight) ||
                (requiredLocalColumnsFromLeft && isRightOrFullJoin) ||
                (requiredLocalColumnsFromRight && isLeftOrFullJoin)) {
            outputInputProps.clear();
            return visitOperator(node, context);
        }

        List<OutputInputProperty> result = Lists.newArrayList();
        if (requiredLocalColumnsFromLeft) {
            for (OutputInputProperty outputInputProp : outputInputProps) {
                PhysicalPropertySet left = outputInputProp.getInputProperty(0);
                PhysicalPropertySet right = outputInputProp.getInputProperty(1);

                if (left.getDistributionProperty().isAny()) {
                    // Broadcast
                    result.add(OutputInputProperty.of(distributeRequirements(), distributeRequirements(), right));
                } else if (left.getDistributionProperty().isShuffle()) {
                    HashDistributionDesc desc =
                            ((HashDistributionSpec) left.getDistributionProperty().getSpec()).getHashDistributionDesc();

                    if (desc.getSourceType() == HashDistributionDesc.SourceType.LOCAL
                            && requireDistributionDesc.getColumns().containsAll(desc.getColumns())) {
                        // BucketShuffle or Colocate
                        // required local columns is sub-set
                        result.add(
                                OutputInputProperty.of(distributeRequirements(), outputInputProp.getInputProperties()));
                    }
                }
            }
        } else {
            // Could only requiredLocalColumnsFromRight for colocated join
            if (!canDoColocate) {
                // replicate join don't support LOCAL properties for right table
                outputInputProps.clear();
                return visitOperator(node, context);
            }

            for (OutputInputProperty outputInputProp : outputInputProps) {
                PhysicalPropertySet right = outputInputProp.getInputProperty(1);

                if (right.getDistributionProperty().isShuffle()) {
                    HashDistributionDesc desc =
                            ((HashDistributionSpec) right.getDistributionProperty().getSpec())
                                    .getHashDistributionDesc();

                    if (desc.getSourceType() == HashDistributionDesc.SourceType.LOCAL
                            && requireDistributionDesc.getColumns().containsAll(desc.getColumns())) {
                        // only Colocate
                        // required local columns is sub-set
                        result.add(OutputInputProperty.of(distributeRequirements(),
                                outputInputProp.getInputProperties()));
                    }
                }
            }
        }

        outputInputProps = result;
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalProject(PhysicalProjectOperator node, ExpressionContext context) {
        // Pass through the requirements to the child
        outputInputProps.add(OutputInputProperty.of(distributeRequirements(), distributeRequirements()));
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
        // If scan tablet sum leas than 1, do one phase local aggregate is enough
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == 0
                && context.getRootProperty().isExecuteInOneTablet()
                && node.getType().isGlobal() && !node.isSplit()) {
            outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
            return visitAggregateRequirements(node, context);
        }

        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (node.getType().isGlobal() && !node.isSplit())) {
            outputInputProps.add(OutputInputProperty
                    .of(PhysicalPropertySet.EMPTY, createLimitGatherProperty(child.getLimit())));
            return visitAggregateRequirements(node, context);
        }

        if (!node.getType().isLocal()) {
            List<Integer> columns = node.getPartitionByColumns().stream().map(ColumnRefOperator::getId).collect(
                    Collectors.toList());

            // None grouping columns
            if (columns.isEmpty()) {
                DistributionProperty distributionProperty =
                        new DistributionProperty(DistributionSpec.createGatherDistributionSpec());

                outputInputProps.add(OutputInputProperty.of(new PhysicalPropertySet(distributionProperty),
                        new PhysicalPropertySet(distributionProperty)));
                return visitAggregateRequirements(node, context);
            }

            // shuffle aggregation
            DistributionSpec distributionSpec = DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(columns, HashDistributionDesc.SourceType.SHUFFLE_AGG));
            DistributionProperty distributionProperty = new DistributionProperty(distributionSpec);
            outputInputProps.add(OutputInputProperty
                    .of(new PhysicalPropertySet(distributionProperty), new PhysicalPropertySet(distributionProperty)));

            // local aggregation
            DistributionSpec localSpec = DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(columns, HashDistributionDesc.SourceType.LOCAL));
            DistributionProperty localProperty = new DistributionProperty(localSpec);
            outputInputProps.add(OutputInputProperty
                    .of(new PhysicalPropertySet(localProperty), (new PhysicalPropertySet(localProperty))));

            return visitAggregateRequirements(node, context);
        }

        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
        return visitAggregateRequirements(node, context);
    }

    private Void visitAggregateRequirements(PhysicalHashAggregateOperator node, ExpressionContext context) {
        Optional<HashDistributionDesc> required = getRequiredLocalDesc();

        if (!required.isPresent()) {
            return visitOperator(node, context);
        }

        HashDistributionDesc requireDistributionDesc = required.get();

        ColumnRefSet requiredLocalColumns = new ColumnRefSet();
        requireDistributionDesc.getColumns().forEach(requiredLocalColumns::union);
        List<OutputInputProperty> result = Lists.newArrayList();

        for (OutputInputProperty outputInputProp : outputInputProps) {
            PhysicalPropertySet input = outputInputProp.getInputProperty(0);

            if (input.getDistributionProperty().isAny()) {
                result.add(OutputInputProperty.of(distributeRequirements(), distributeRequirements()));
            } else if (input.getDistributionProperty().isShuffle()) {
                HashDistributionDesc outputDesc =
                        ((HashDistributionSpec) outputInputProp.getOutputProperty().getDistributionProperty().getSpec())
                                .getHashDistributionDesc();

                HashDistributionDesc inputDesc =
                        ((HashDistributionSpec) input.getDistributionProperty().getSpec()).getHashDistributionDesc();

                if (outputDesc.getSourceType() == HashDistributionDesc.SourceType.LOCAL
                        && inputDesc.getSourceType() == HashDistributionDesc.SourceType.LOCAL
                        && requireDistributionDesc.getColumns().containsAll(inputDesc.getColumns())) {
                    // BucketShuffle or Colocate
                    // required local columns is sub-set
                    result.add(OutputInputProperty.of(distributeRequirements(), outputInputProp.getInputProperties()));
                }
            }
        }

        outputInputProps = result;
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
        HashDistributionSpec hashDistributionSpec = node.getDistributionSpec();

        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        boolean satisfyLocalProperty;
        if (node.getSelectedPartitionId().size() <= 1 || (colocateIndex.isColocateTable(node.getTable().getId()) &&
                !colocateIndex.isGroupUnstable(colocateIndex.getGroup(node.getTable().getId())))) {
            outputInputProps.add(OutputInputProperty.of(createPropertySetByDistribution(hashDistributionSpec)));
            satisfyLocalProperty = true;
        } else {
            outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
            satisfyLocalProperty = false;
        }

        Optional<HashDistributionDesc> required = getRequiredLocalDesc();
        if (!required.isPresent()) {
            return visitOperator(node, context);
        }

        outputInputProps.clear();
        HashDistributionDesc requireDistributionDesc = required.get();
        if (requireDistributionDesc.getColumns().containsAll(hashDistributionSpec.getShuffleColumns()) &&
                satisfyLocalProperty) {
            outputInputProps.add(OutputInputProperty.of(distributeRequirements()));
        }

        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalTopN(PhysicalTopNOperator topN, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(topN, context);
        }

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
            outputProperty = new PhysicalPropertySet();
        }

        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (topN.getSortPhase().isFinal() && !topN.isSplit())) {
            PhysicalPropertySet inputProperty = createLimitGatherProperty(child.getLimit());
            outputInputProps.add(OutputInputProperty.of(outputProperty, Lists.newArrayList(inputProperty)));
        } else {
            outputInputProps.add(OutputInputProperty.of(outputProperty, Lists.newArrayList(PhysicalPropertySet.EMPTY)));
        }

        return visitOperator(topN, context);
    }

    @Override
    public Void visitPhysicalHiveScan(PhysicalHiveScanOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalIcebergScan(PhysicalIcebergScanOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            outputInputProps.add(OutputInputProperty.of(distributeRequirements()));
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalMysqlScan(PhysicalMysqlScanOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            outputInputProps.add(OutputInputProperty.of(distributeRequirements()));
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalMetaScan(PhysicalMetaScanOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalEsScan(PhysicalEsScanOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        DistributionSpec gather = DistributionSpec.createGatherDistributionSpec();
        DistributionProperty inputProperty = new DistributionProperty(gather);

        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, new PhysicalPropertySet(inputProperty)));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        List<Integer> partitionColumnRefSet = new ArrayList<>();

        node.getPartitionExpressions().forEach(e -> {
            partitionColumnRefSet
                    .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList()));
        });

        SortProperty sortProperty = new SortProperty(new OrderSpec(node.getEnforceOrderBy()));

        Optional<HashDistributionDesc> required = getRequiredLocalDesc();
        if (required.isPresent()) {
            if (!partitionColumnRefSet.isEmpty() && required.get().getColumns().containsAll(partitionColumnRefSet)) {
                // local
                DistributionProperty localProperty = new DistributionProperty(DistributionSpec
                        .createHashDistributionSpec(new HashDistributionDesc(partitionColumnRefSet,
                                HashDistributionDesc.SourceType.LOCAL)));
                outputInputProps.add(OutputInputProperty.of(new PhysicalPropertySet(localProperty, sortProperty),
                        new PhysicalPropertySet(localProperty, sortProperty)));
            }

            return visitOperator(node, context);
        }

        if (partitionColumnRefSet.isEmpty()) {
            DistributionProperty distributionProperty =
                    new DistributionProperty(DistributionSpec.createGatherDistributionSpec());
            outputInputProps.add(OutputInputProperty.of(new PhysicalPropertySet(distributionProperty, sortProperty),
                    new PhysicalPropertySet(distributionProperty, sortProperty)));
        } else {
            DistributionProperty distributionProperty = new DistributionProperty(DistributionSpec
                    .createHashDistributionSpec(
                            new HashDistributionDesc(partitionColumnRefSet,
                                    HashDistributionDesc.SourceType.SHUFFLE_AGG)));
            outputInputProps.add(OutputInputProperty.of(new PhysicalPropertySet(distributionProperty, sortProperty),
                    new PhysicalPropertySet(distributionProperty, sortProperty)));

            // local
            DistributionProperty localProperty = new DistributionProperty(DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(partitionColumnRefSet, HashDistributionDesc.SourceType.LOCAL)));
            outputInputProps.add(OutputInputProperty.of(new PhysicalPropertySet(localProperty, sortProperty),
                    new PhysicalPropertySet(localProperty, sortProperty)));
        }

        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalUnion(PhysicalUnionOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalExcept(PhysicalExceptOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalIntersect(PhysicalIntersectOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return visitOperator(node, context);
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
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, childProperty));
    }

    @Override
    public Void visitPhysicalValues(PhysicalValuesOperator node, ExpressionContext context) {
        // Pass through the requirements to the child
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        // Can not support required property directly, this will generate plan fragment like:
        //            join(colocate)
        //            /            \
        //         Es/Hive     Union/Empty
        // This PlanFragment would use ColocatedBackendSelector which is not been allowed.
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalRepeat(PhysicalRepeatOperator node, ExpressionContext context) {
        if (!node.hasLimit() && context.getChildOperator(0).hasLimit() ||
                (node.hasLimit() && context.getChildOperator(0).hasLimit() &&
                        node.getLimit() != context.getChildOperator(0).getLimit())) {
            PhysicalPropertySet gather = createLimitGatherProperty(context.getChildOperator(0).getLimit());
            outputInputProps.add(OutputInputProperty.of(gather, gather));
            return visitOperator(node, context);
        }

        // Pass through the requirements to the child
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalFilter(PhysicalFilterOperator node, ExpressionContext context) {
        if (!node.hasLimit() && context.getChildOperator(0).hasLimit() ||
                (node.hasLimit() && context.getChildOperator(0).hasLimit() &&
                        node.getLimit() != context.getChildOperator(0).getLimit())) {
            PhysicalPropertySet gather = createLimitGatherProperty(context.getChildOperator(0).getLimit());
            outputInputProps.add(OutputInputProperty.of(gather, gather));
            return visitOperator(node, context);
        }

        // Pass through the requirements to the child
        if (getRequiredLocalDesc().isPresent()) {
            outputInputProps.add(OutputInputProperty.of(distributeRequirements(), distributeRequirements()));
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalTableFunction(PhysicalTableFunctionOperator node, ExpressionContext context) {
        if (!node.hasLimit() && context.getChildOperator(0).hasLimit() ||
                (node.hasLimit() && context.getChildOperator(0).hasLimit() &&
                        node.getLimit() != context.getChildOperator(0).getLimit())) {
            PhysicalPropertySet gather = createLimitGatherProperty(context.getChildOperator(0).getLimit());
            outputInputProps.add(OutputInputProperty.of(gather, gather));
            return visitOperator(node, context);
        }

        // Pass through the requirements to the child
        if (getRequiredLocalDesc().isPresent()) {
            outputInputProps.add(OutputInputProperty.of(distributeRequirements(), distributeRequirements()));
            return visitOperator(node, context);
        }
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        if (getRequiredLocalDesc().isPresent()) {
            return visitOperator(node, context);
        }

        outputInputProps.add(OutputInputProperty
                .of(createLimitGatherProperty(node.getLimit()), createLimitGatherProperty(node.getLimit())));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        outputInputProps.add(OutputInputProperty.of(requirements, PhysicalPropertySet.EMPTY, requirements));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY, PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, ExpressionContext context) {
        outputInputProps.add(OutputInputProperty.of(PhysicalPropertySet.EMPTY));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        outputInputProps.add(OutputInputProperty.of(requirements, requirements));
        return visitOperator(node, context);
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
