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
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * UKFKConstraintsCollector is used to collect unique key and foreign key constraints in bottom-up mode.
 * and the derived constraints will be attached to the corresponding OptExpression.
 * <p>
 * UKFKConstraints is plan-structure sensitive, fortunately, the original OptExpression will be replaced by a new one
 * if any transformation rules are applied, then these constraints will be removed automatically.
 */
public class UKFKConstraintsCollector extends OptExpressionVisitor<Void, Void> {

    public static void collectColumnConstraints(OptExpression root) {
        if (!ConnectContext.get().getSessionVariable().isEnableUKFKOpt()) {
            return;
        }
        UKFKConstraintsCollector collector = new UKFKConstraintsCollector();
        root.getOp().accept(collector, root, null);
    }

    @Override
    public Void visit(OptExpression optExpression, Void context) {
        visitChildren(optExpression, context);
        if (optExpression.getConstraints() != null) {
            return null;
        }
        optExpression.setConstraints(new UKFKConstraints());
        return null;
    }

    private void visitChildren(OptExpression optExpression, Void context) {
        for (OptExpression child : optExpression.getInputs()) {
            child.getOp().accept(this, child, context);
        }
    }

    private void inheritFromSingleChild(OptExpression optExpression, Void context) {
        visitChildren(optExpression, context);
        if (optExpression.getConstraints() != null) {
            return;
        }
        UKFKConstraints childConstraints = optExpression.inputAt(0).getConstraints();
        UKFKConstraints constraints = UKFKConstraints.inheritFrom(childConstraints,
                optExpression.getRowOutputInfo().getOutputColumnRefSet());

        optExpression.setConstraints(constraints);
    }

    @Override
    public Void visitLogicalTableScan(OptExpression optExpression, Void context) {
        visitChildren(optExpression, context);
        if (optExpression.getConstraints() != null) {
            return null;
        }
        if (!(optExpression.getOp() instanceof LogicalOlapScanOperator)) {
            optExpression.setConstraints(new UKFKConstraints());
            return null;
        }
        LogicalOlapScanOperator scanOperator = optExpression.getOp().cast();
        ColumnRefSet usedColumns = new ColumnRefSet();
        if (scanOperator.getPredicate() != null) {
            usedColumns.union(scanOperator.getPredicate().getUsedColumns());
        }
        OlapTable table = (OlapTable) scanOperator.getTable();
        Map<String, ColumnRefOperator> columnNameToColRefMap = scanOperator.getColumnNameToColRefMap();

        visitOlapTable(optExpression, table, columnNameToColRefMap, usedColumns);

        return null;
    }

    @Override
    public Void visitPhysicalOlapScan(OptExpression optExpression, Void context) {
        visitChildren(optExpression, context);
        if (optExpression.getConstraints() != null) {
            return null;
        }
        if (!(optExpression.getOp() instanceof PhysicalOlapScanOperator)) {
            optExpression.setConstraints(new UKFKConstraints());
            return null;
        }
        PhysicalOlapScanOperator scanOperator = optExpression.getOp().cast();
        ColumnRefSet usedColumns = scanOperator.getUsedColumns();
        OlapTable table = (OlapTable) scanOperator.getTable();
        Map<String, ColumnRefOperator> columnNameToColRefMap = scanOperator.getColRefToColumnMetaMap().entrySet()
                .stream().collect(Collectors.toMap(entry -> entry.getValue().getName(), Map.Entry::getKey));

        visitOlapTable(optExpression, table, columnNameToColRefMap, usedColumns);

        return null;
    }

    private void visitOlapTable(OptExpression optExpression, OlapTable table,
                                Map<String, ColumnRefOperator> columnNameToColRefMap, ColumnRefSet usedColumns) {
        ColumnRefSet outputColumns = optExpression.getRowOutputInfo().getOutputColumnRefSet();
        UKFKConstraints constraint = new UKFKConstraints();
        if (table.hasUniqueConstraints()) {
            List<UniqueConstraint> ukConstraints = table.getUniqueConstraints();
            for (UniqueConstraint ukConstraint : ukConstraints) {
                // For now, we only handle one column primary key or foreign key
                if (ukConstraint.getUniqueColumns().size() == 1) {
                    String ukColumn = ukConstraint.getUniqueColumns().get(0);

                    // Get non-uk original column ids
                    ColumnRefSet nonUkColumnRefs = new ColumnRefSet(table.getColumns().stream()
                            .map(Column::getName)
                            .filter(columnNameToColRefMap::containsKey)
                            .filter(name -> !Objects.equals(ukColumn, name))
                            .map(columnNameToColRefMap::get)
                            .collect(Collectors.toList()));

                    ColumnRefOperator columnRefOperator = columnNameToColRefMap.get(ukColumn);
                    if (columnRefOperator != null && outputColumns.contains(columnRefOperator)) {
                        constraint.addUniqueKey(columnRefOperator.getId(),
                                new UKFKConstraints.UniqueConstraintWrapper(ukConstraint,
                                        nonUkColumnRefs, usedColumns.isEmpty()));
                    }
                }
            }
        }
        if (table.hasForeignKeyConstraints()) {
            List<ForeignKeyConstraint> fkConstraints = table.getForeignKeyConstraints();
            for (ForeignKeyConstraint fkConstraint : fkConstraints) {
                if (fkConstraint.getColumnRefPairs().size() == 1) {
                    Pair<String, String> pair = fkConstraint.getColumnRefPairs().get(0);
                    ColumnRefOperator columnRefOperator = columnNameToColRefMap.get(pair.first);
                    if (columnRefOperator != null && outputColumns.contains(columnRefOperator)) {
                        constraint.addForeignKey(columnRefOperator.getId(), fkConstraint);
                    }
                }
            }
        }

        optExpression.setConstraints(constraint);
    }

    @Override
    public Void visitPhysicalDistribution(OptExpression optExpression, Void context) {
        inheritFromSingleChild(optExpression, context);
        return null;
    }

    @Override
    public Void visitPhysicalProject(OptExpression optExpression, Void context) {
        inheritFromSingleChild(optExpression, context);
        return null;
    }

    @Override
    public Void visitLogicalProject(OptExpression optExpression, Void context) {
        inheritFromSingleChild(optExpression, context);
        return null;
    }

    @Override
    public Void visitLogicalJoin(OptExpression optExpression, Void context) {
        LogicalJoinOperator joinOperator = optExpression.getOp().cast();
        visitJoinOperator(optExpression, context, joinOperator.getJoinType(), joinOperator.getOnPredicate());
        return null;
    }

    @Override
    public Void visitPhysicalJoin(OptExpression optExpression, Void context) {
        PhysicalJoinOperator joinOperator = optExpression.getOp().cast();
        visitJoinOperator(optExpression, context, joinOperator.getJoinType(), joinOperator.getOnPredicate());
        return null;
    }

    private void visitJoinOperator(OptExpression optExpression, Void context, JoinOperator joinType,
                                   ScalarOperator onPredicates) {
        visitChildren(optExpression, context);

        if (optExpression.getConstraints() != null) {
            return;
        }

        UKFKConstraints constraints = buildJoinColumnConstraint(optExpression.getOp(), joinType, onPredicates,
                optExpression.inputAt(0), optExpression.inputAt(1));

        optExpression.setConstraints(constraints);
    }

    public static UKFKConstraints buildJoinColumnConstraint(Operator operator, JoinOperator joinType,
                                                            ScalarOperator onPredicates,
                                                            OptExpression leftChild, OptExpression rightChild) {
        UKFKConstraints constraint = new UKFKConstraints();

        ColumnRefSet leftOutputColumns = leftChild.getRowOutputInfo().getOutputColumnRefSet();
        ColumnRefSet rightOutputColumns = rightChild.getRowOutputInfo().getOutputColumnRefSet();
        List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(leftOutputColumns,
                rightOutputColumns, Utils.extractConjuncts(onPredicates));

        UKFKConstraints leftConstraints = leftChild.getConstraints();
        UKFKConstraints rightConstraints = rightChild.getConstraints();
        UKFKConstraints.JoinProperty property =
                extractUKFKJoinOnPredicate(eqOnPredicates, leftConstraints, rightConstraints);

        ColumnRefSet outputColumns = operator.getRowOutputInfo(Lists.newArrayList(leftChild, rightChild))
                .getOutputColumnRefSet();

        if (property != null) {
            constraint.setJoinProperty(property);

            if ((joinType.isLeftSemiJoin() && property.isLeftUK) ||
                    (joinType.isRightSemiJoin() && !property.isLeftUK)) {
                // The unique property is preserved
                if (outputColumns.contains(property.ukColumnRef)) {
                    constraint.addUniqueKey(property.ukColumnRef.getId(), property.ukConstraint);
                }
            }
        }

        // All foreign properties can be preserved
        constraint.inheritForeignKey(leftConstraints, outputColumns);
        constraint.inheritForeignKey(rightConstraints, outputColumns);

        return constraint;
    }

    private static UKFKConstraints.JoinProperty extractUKFKJoinOnPredicate(
            List<BinaryPredicateOperator> eqOnPredicates,
            UKFKConstraints leftConstraints,
            UKFKConstraints rightConstraints) {
        for (BinaryPredicateOperator predicate : eqOnPredicates) {
            ScalarOperator child1 = predicate.getChild(0);
            ScalarOperator child2 = predicate.getChild(1);
            if (!(child1 instanceof ColumnRefOperator && child2 instanceof ColumnRefOperator)) {
                continue;
            }
            ColumnRefOperator colRef1 = (ColumnRefOperator) child1;
            ColumnRefOperator colRef2 = (ColumnRefOperator) child2;

            UKFKConstraints.UniqueConstraintWrapper ukConstraint =
                    leftConstraints.getUniqueConstraint(colRef1.getId());
            ForeignKeyConstraint fkConstraint = rightConstraints.getForeignKeyConstraint(colRef2.getId());
            if (isMatch(fkConstraint, ukConstraint)) {
                return new UKFKConstraints.JoinProperty(predicate, ukConstraint,
                        fkConstraint,
                        colRef1, colRef2, true);
            }

            ukConstraint = leftConstraints.getUniqueConstraint(colRef2.getId());
            fkConstraint = rightConstraints.getForeignKeyConstraint(colRef1.getId());
            if (isMatch(fkConstraint, ukConstraint)) {
                return new UKFKConstraints.JoinProperty(predicate, ukConstraint,
                        fkConstraint,
                        colRef2, colRef1, true);
            }

            ukConstraint = rightConstraints.getUniqueConstraint(colRef1.getId());
            fkConstraint = leftConstraints.getForeignKeyConstraint(colRef2.getId());
            if (isMatch(fkConstraint, ukConstraint)) {
                return new UKFKConstraints.JoinProperty(predicate, ukConstraint,
                        fkConstraint,
                        colRef1, colRef2, false);
            }

            ukConstraint = rightConstraints.getUniqueConstraint(colRef2.getId());
            fkConstraint = leftConstraints.getForeignKeyConstraint(colRef1.getId());
            if (isMatch(fkConstraint, ukConstraint)) {
                return new UKFKConstraints.JoinProperty(predicate, ukConstraint,
                        fkConstraint,
                        colRef2, colRef1, false);
            }
        }

        return null;
    }

    private static boolean isMatch(ForeignKeyConstraint fkConstraint,
                                   UKFKConstraints.UniqueConstraintWrapper ukConstraint) {
        if (fkConstraint == null || ukConstraint == null) {
            return false;
        }
        BaseTableInfo parentTableInfo = fkConstraint.getParentTableInfo();
        if (parentTableInfo == null) {
            return false;
        }
        if (!Objects.equals(parentTableInfo.getCatalogName(), ukConstraint.constraint.getCatalogName())) {
            return false;
        }
        if (!Objects.equals(parentTableInfo.getDbName(), ukConstraint.constraint.getDbName())) {
            return false;
        }
        if (!Objects.equals(parentTableInfo.getTableName(), ukConstraint.constraint.getTableName())) {
            return false;
        }
        return Objects.equals(fkConstraint.getColumnRefPairs().get(0).second,
                ukConstraint.constraint.getUniqueColumns().get(0));
    }
}
