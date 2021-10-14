// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * For a join b on cast (a.id_date as datetime) = b.id_datetime
 * This rule will add project for join left child.
 * In project, we will add column mapping: new column -> cast (a.id_date as datetime)
 * Because when shuffle, we must shuffle by cast (a.id_date as datetime), not a.id_date.
 */
public class AddProjectForJoinOnBinaryPredicatesRule implements PhysicalOperatorTreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        ColumnRefFactory factory = taskContext.getOptimizerContext().getColumnRefFactory();
        for (int i = 0; i < root.arity(); ++i) {
            root.setChild(i, this.rewrite(root.inputAt(i), taskContext));
        }

        if (!(root.getOp() instanceof PhysicalHashJoinOperator)) {
            return root;
        }

        if (!(root.inputAt(0).getOp() instanceof PhysicalDistributionOperator &&
                ((PhysicalDistributionOperator) root.inputAt(0).getOp()).getDistributionSpec().getType() ==
                        DistributionSpec.DistributionType.SHUFFLE) ||
                !(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator &&
                        ((PhysicalDistributionOperator) root.inputAt(1).getOp()).getDistributionSpec().getType() ==
                                DistributionSpec.DistributionType.SHUFFLE)) {
            return root;
        }

        PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) root.getOp();
        List<ScalarOperator> predicateLists = Utils.extractConjuncts(joinOperator.getJoinPredicate());
        for (ScalarOperator predicate : predicateLists) {
            if (!(predicate instanceof BinaryPredicateOperator)) {
                return root;
            }
            BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
            if (!binary.getBinaryType().isEquivalence()) {
                return root;
            }
            ScalarOperator leftChild = binary.getChild(0);
            ScalarOperator rightChild = binary.getChild(1);

            if (leftChild.isVariable() && rightChild.isVariable()) {
                if (!JoinPredicateUtils
                        .isEqualBinaryPredicate(root.inputAt(0).getOutputColumns(), root.inputAt(1).getOutputColumns(),
                                binary)) {
                    continue;
                }

                if (!(leftChild.isColumnRef())) {
                    processOneChild(root, binary, 0, factory);
                }

                if (!(rightChild.isColumnRef())) {
                    processOneChild(root, binary, 1, factory);
                }
            }
        }
        return root;
    }

    private void processOneChild(OptExpression join,
                                 BinaryPredicateOperator binary,
                                 int childIndex,
                                 ColumnRefFactory factory) {
        OptExpression leftDistribution = join.inputAt(0);
        OptExpression rightDistribution = join.inputAt(1);
        ColumnRefSet outputColumn = leftDistribution.inputAt(0).getLogicalProperty().getOutputColumns();
        if (outputColumn.contains(binary.getChild(childIndex).getUsedColumns())) {
            OptExpression project = addProjectOperator(
                    join,
                    binary,
                    childIndex,
                    leftDistribution,
                    leftDistribution.inputAt(0),
                    factory);
            leftDistribution.setChild(0, project);
        } else {
            OptExpression project = addProjectOperator(
                    join,
                    binary,
                    childIndex,
                    rightDistribution,
                    rightDistribution.inputAt(0),
                    factory);
            rightDistribution.setChild(0, project);
        }
    }

    private OptExpression addProjectOperator(OptExpression joinExpr,
                                             BinaryPredicateOperator binary,
                                             int childIndex,
                                             OptExpression distributionExpr,
                                             OptExpression childExpr,
                                             ColumnRefFactory factory) {
        // 1 Generate new column for function call
        ScalarOperator childCall = binary.getChild(childIndex);
        ColumnRefOperator column = factory.create(childCall, childCall.getType(), childCall.isNullable());
        binary.setChild(childIndex, column);

        // 2 Build new project expr
        OptExpression resultProject;
        if (childExpr.getOp() instanceof PhysicalProjectOperator) {
            // TODO(kks): Should merge these two project exprs
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
            ColumnRefSet newOutputColumns = new ColumnRefSet();
            // Put new column firstly
            newColumnRefMap.put(column, childCall);
            newOutputColumns.union(column);

            // Put old project column if join operator or join parent operator use it
            PhysicalProjectOperator projectOperator = (PhysicalProjectOperator) childExpr.getOp();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
                if (isUsedByJoinOrJoinParent(joinExpr, kv.getKey().getId())) {
                    newColumnRefMap.put(kv.getKey(), kv.getKey());
                    newOutputColumns.union(kv.getKey());
                }
            }

            // Build new project expr
            PhysicalProjectOperator newProjectOperator = new PhysicalProjectOperator(newColumnRefMap, Maps.newHashMap());
            OptExpression projectExpr = OptExpression.create(newProjectOperator, childExpr);
            projectExpr.setLogicalProperty(new LogicalProperty(newOutputColumns));
            resultProject = projectExpr;
        } else {
            // Add new column to output columns
            ColumnRefSet childOutputColumns = childExpr.getLogicalProperty().getOutputColumns();
            ColumnRefSet projectOutputs = (ColumnRefSet) childOutputColumns.clone();
            projectOutputs.union(column.getId());

            // Build project column map
            Map<ColumnRefOperator, ScalarOperator> maps = Maps.newHashMap();
            maps.put(column, childCall);
            for (int id : childOutputColumns.getColumnIds()) {
                if (isUsedByJoinOrJoinParent(joinExpr, id)) {
                    ColumnRefOperator columnRef = factory.getColumnRef(id);
                    maps.put(columnRef, columnRef);
                }
            }

            // Build new project expr
            PhysicalProjectOperator projectOperator = new PhysicalProjectOperator(maps, Maps.newHashMap());
            OptExpression projectExpr = OptExpression.create(projectOperator, childExpr);
            projectExpr.setLogicalProperty(new LogicalProperty(projectOutputs));
            resultProject = projectExpr;
        }

        // 3 Rewrite distributionExpr shuffle columns
        reWriteDistributionShuffleColumns((PhysicalDistributionOperator) distributionExpr.getOp(), childCall, column);

        // 4 Generate a new LogicalProperty for distributionExpr to avoid affecting the output of other nodes
        LogicalProperty distributionLogicalProperty =
                new LogicalProperty((ColumnRefSet) distributionExpr.getLogicalProperty().getOutputColumns().clone());
        distributionLogicalProperty.getOutputColumns().union(column.getId());
        distributionExpr.setLogicalProperty(distributionLogicalProperty);
        resultProject.setStatistics(childExpr.getStatistics());
        return resultProject;
    }

    private boolean isUsedByJoinOrJoinParent(OptExpression join, int columnId) {
        // 1 Join output columns
        ColumnRefSet usedColumns = (ColumnRefSet) join.getLogicalProperty().getOutputColumns().clone();
        PhysicalHashJoinOperator joinOp = (PhysicalHashJoinOperator) join.getOp();
        // 2 Join on predicates used columns
        usedColumns.union(joinOp.getJoinPredicate().getUsedColumns());
        // 3 Join predicates used columns
        if (joinOp.getPredicate() != null) {
            usedColumns.union(joinOp.getJoinPredicate().getUsedColumns());
        }
        return usedColumns.contains(columnId);
    }

    private void reWriteDistributionShuffleColumns(PhysicalDistributionOperator distribution,
                                                   ScalarOperator oldCall,
                                                   ColumnRefOperator newColumn) {
        HashDistributionDesc desc =
                ((HashDistributionSpec) distribution.getDistributionSpec()).getHashDistributionDesc();

        List<Integer> shuffledColumns = new ArrayList<>(desc.getColumns());
        shuffledColumns.remove(Integer.valueOf(oldCall.getUsedColumns().getFirstId()));
        shuffledColumns.add(newColumn.getId());

        distribution.setDistributionSpec(
                new HashDistributionSpec(new HashDistributionDesc(shuffledColumns, desc.getSourceType())));
    }
}
