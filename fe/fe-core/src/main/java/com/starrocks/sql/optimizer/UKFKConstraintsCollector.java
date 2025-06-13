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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
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
        collectColumnConstraintsForce(root);
    }

    public static void collectColumnConstraintsForce(OptExpression root) {
        UKFKConstraintsCollector collector = new UKFKConstraintsCollector();
        root.getOp().accept(collector, root, null);
    }

    @Override
    public Void visit(OptExpression optExpression, Void context) {
        if (!visitChildren(optExpression, context)) {
            return null;
        }
        optExpression.setConstraints(new UKFKConstraints());
        return null;
    }

    @Override
    public Void visitLogicalAggregate(OptExpression optExpression, Void context) {
        if (!visitChildren(optExpression, context)) {
            return null;
        }

        LogicalAggregationOperator aggOp = optExpression.getOp().cast();

        ColumnRefSet outputColumns = optExpression.getRowOutputInfo().getOutputColumnRefSet();
        UKFKConstraints childConstraints = optExpression.inputAt(0).getConstraints();

        UKFKConstraints constraints = new UKFKConstraints();
        constraints.inheritForeignKey(childConstraints, outputColumns);
        constraints.inheritRelaxedUniqueKey(childConstraints, outputColumns);

        if (!aggOp.isOnlyLocalAggregate()) {
            ColumnRefSet groupBys = new ColumnRefSet(aggOp.getGroupingKeys());
            if (!groupBys.isEmpty() && outputColumns.containsAll(groupBys)) {
                constraints.addAggUniqueKey(
                        new UKFKConstraints.UniqueConstraintWrapper(null, new ColumnRefSet(), false, groupBys));
            }
        }

        optExpression.setConstraints(constraints);
        return null;
    }

    /**
     * Visit each child.
     *
     * @return true，if the current node needs to recollect constraints.
     */
    private boolean visitChildren(OptExpression optExpression, Void context) {
        boolean childConstraintsChanged = false;
        for (OptExpression child : optExpression.getInputs()) {
            UKFKConstraints prevConstraints = child.getConstraints();
            child.getOp().accept(this, child, context);
            childConstraintsChanged |= !Objects.equals(prevConstraints, child.getConstraints());
        }
        return childConstraintsChanged || optExpression.getConstraints() == null;
    }

    private void inheritFromSingleChild(OptExpression optExpression, Void context) {
        if (!visitChildren(optExpression, context)) {
            return;
        }
        UKFKConstraints childConstraints = optExpression.inputAt(0).getConstraints();
        UKFKConstraints constraints = UKFKConstraints.inheritFrom(childConstraints,
                optExpression.getRowOutputInfo().getOutputColumnRefSet());

        optExpression.setConstraints(constraints);
    }

    @Override
    public Void visitLogicalTableScan(OptExpression optExpression, Void context) {
        if (!visitChildren(optExpression, context)) {
            return null;
        }
        LogicalScanOperator scanOperator = optExpression.getOp().cast();
        ColumnRefSet usedColumns = new ColumnRefSet();
        if (scanOperator.getPredicate() != null) {
            usedColumns.union(scanOperator.getPredicate().getUsedColumns());
        }
        Table table = scanOperator.getTable();
        Map<String, ColumnRefOperator> columnNameToColRefMap = scanOperator.getColumnNameToColRefMap();

        visitTable(optExpression, table, columnNameToColRefMap, usedColumns);

        return null;
    }

    @Override
    public Void visitPhysicalScan(OptExpression optExpression, Void context) {
        if (!visitChildren(optExpression, context)) {
            return null;
        }
        PhysicalScanOperator scanOperator = optExpression.getOp().cast();
        ColumnRefSet usedColumns = scanOperator.getUsedColumns();
        Table table = scanOperator.getTable();
        Map<String, ColumnRefOperator> columnNameToColRefMap = scanOperator.getColRefToColumnMetaMap().entrySet()
                .stream().collect(Collectors.toMap(entry -> entry.getValue().getName(), Map.Entry::getKey));

        visitTable(optExpression, table, columnNameToColRefMap, usedColumns);

        return null;
    }

    private void visitTable(OptExpression optExpression, Table table,
                            Map<String, ColumnRefOperator> columnNameToColRefMap, ColumnRefSet usedColumns) {
        ColumnRefSet outputColumns = optExpression.getRowOutputInfo().getOutputColumnRefSet();
        UKFKConstraints constraint = new UKFKConstraints();
        if (table.hasUniqueConstraints()) {
            List<UniqueConstraint> ukConstraints = table.getUniqueConstraints();
            for (UniqueConstraint ukConstraint : ukConstraints) {
                List<String> ukColNames = ukConstraint.getUniqueColumnNames(table);
                boolean containsAllUk = ukColNames.stream().allMatch(colName ->
                        columnNameToColRefMap.containsKey(colName) && outputColumns.contains(columnNameToColRefMap.get(colName)));
                if (!containsAllUk) {
                    continue;
                }

                ColumnRefSet ukColumnRefs = new ColumnRefSet();
                ukColNames.stream()
                        .map(columnNameToColRefMap::get)
                        .forEach(ukColumnRefs::union);
                ColumnRefSet nonUkColumnRefs = new ColumnRefSet();
                table.getColumns().stream()
                        .map(Column::getName)
                        .filter(columnNameToColRefMap::containsKey)
                        .filter(name -> !ukColNames.contains(name))
                        .map(columnNameToColRefMap::get)
                        .forEach(nonUkColumnRefs::union);

                UKFKConstraints.UniqueConstraintWrapper uk = new UKFKConstraints.UniqueConstraintWrapper(ukConstraint,
                        nonUkColumnRefs, usedColumns.isEmpty(), ukColumnRefs);
                constraint.addAggUniqueKey(uk);

                // For now, we only handle one column primary key or foreign key
                if (ukColNames.size() == 1) {
                    Preconditions.checkState(ukColumnRefs.size() == 1,
                            "the size of ukColumnRefs MUST be the same as that of ukColNames");
                    constraint.addUniqueKey(ukColumnRefs.getFirstId(), uk);
                }
            }
        }

        if (table.hasForeignKeyConstraints()) {
            Column firstKeyColumn = table.getPresentivateColumn();
            ColumnRefOperator firstKeyColumnRef = columnNameToColRefMap.get(firstKeyColumn.getName());
            List<ForeignKeyConstraint> fkConstraints = table.getForeignKeyConstraints();
            for (ForeignKeyConstraint fkConstraint : fkConstraints) {
                if (fkConstraint.getColumnNameRefPairs(table).size() == 1) {
                    Pair<String, String> pair = fkConstraint.getColumnNameRefPairs(table).get(0);
                    ColumnRefOperator fkColumnRef = columnNameToColRefMap.get(pair.first);
                    if (fkColumnRef != null && outputColumns.contains(fkColumnRef)) {
                        constraint.addForeignKey(fkColumnRef.getId(),
                                new UKFKConstraints.ForeignKeyConstraintWrapper(fkConstraint,
                                        Objects.equals(firstKeyColumnRef, fkColumnRef)));
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
        if (!visitChildren(optExpression, context)) {
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

            UKFKConstraints ukConstraints = property.isLeftUK ? leftConstraints : rightConstraints;
            UKFKConstraints fkConstraints = property.isLeftUK ? rightConstraints : leftConstraints;
            List<UKFKConstraints.UniqueConstraintWrapper> ukChildMultiUKs = ukConstraints.getAggUniqueKeys();
            List<UKFKConstraints.UniqueConstraintWrapper> fkChildMultiUKs = fkConstraints.getAggUniqueKeys();
            ColumnRefSet fkColumnRef = new ColumnRefSet(property.fkColumnRef.getId());

            // 1. Inherit unique key constraints.
            // If it is a left semi join on the UK child side, all rows of the UK child will be preserved.
            boolean inheritUKChildUK = (joinType.isLeftSemiJoin() && property.isLeftUK) ||
                    (joinType.isRightSemiJoin() && !property.isLeftUK);
            if (inheritUKChildUK) {
                // The unique property is preserved
                if (outputColumns.contains(property.ukColumnRef)) {
                    constraint.addUniqueKey(property.ukColumnRef.getId(), property.ukConstraint);
                }
                constraint.inheritAggUniqueKey(ukConstraints, outputColumns);
            }

            // 2. Inherit aggregate unique key constraints.
            // 2.1 from FK child side.
            // If it is not an outer join on the UK child side, the FK child side will not produce duplicate rows (NULL rows).
            boolean inheritFKChildAggUK = (property.isLeftUK && !joinType.isLeftOuterJoin() && !joinType.isFullOuterJoin()) ||
                    (!property.isLeftUK && !joinType.isRightOuterJoin() && !joinType.isFullOuterJoin());
            if (inheritFKChildAggUK) {
                constraint.inheritAggUniqueKey(fkConstraints, outputColumns);
            }

            // 2.2 form UK child side.
            // If it is not an outer join on the FK child side, the UK child side will not produce duplicate rows (NULL rows).
            //
            // Assumed that fk_table has a unique key (c11, c12) and a foreign key c11 referencing to c22 of uk_table,
            // uk_table has two unique keys c21 and c22.
            // Then, after INNER Join(fk_table.c11=uk_table.c21), (c12, c21), (c12, c22) are all unique.
            boolean inheritUKChildAggUK = (property.isLeftUK && !joinType.isRightOuterJoin() && !joinType.isFullOuterJoin()) ||
                    (!property.isLeftUK && !joinType.isLeftOuterJoin() && !joinType.isFullOuterJoin());
            if (inheritUKChildAggUK) {
                for (UKFKConstraints.UniqueConstraintWrapper fkChildMultiUK : fkChildMultiUKs) {
                    if (!fkChildMultiUK.ukColumnRefs.containsAll(fkColumnRef)) {
                        continue;
                    }

                    ColumnRefSet ukScopedColumnRefs = fkChildMultiUK.ukColumnRefs.clone();
                    ukScopedColumnRefs.except(fkColumnRef);
                    if (!outputColumns.containsAll(ukScopedColumnRefs)) {
                        continue;
                    }

                    ukChildMultiUKs.stream()
                            .filter(uk -> outputColumns.containsAll(uk.ukColumnRefs))
                            .forEach(uk -> {
                                ColumnRefSet newUKColumnRefs = uk.ukColumnRefs.clone();
                                newUKColumnRefs.union(ukScopedColumnRefs);
                                constraint.addAggUniqueKey(new UKFKConstraints.UniqueConstraintWrapper(null,
                                        uk.nonUKColumnRefs, false, newUKColumnRefs));
                            });
                }
            }
        }

        constraint.inheritRelaxedUniqueKey(leftConstraints, outputColumns);
        constraint.inheritRelaxedUniqueKey(rightConstraints, outputColumns);

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
            UKFKConstraints.ForeignKeyConstraintWrapper fkConstraint =
                    rightConstraints.getForeignKeyConstraint(colRef2.getId());
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

    private static boolean isMatch(UKFKConstraints.ForeignKeyConstraintWrapper fkConstraint,
                                   UKFKConstraints.UniqueConstraintWrapper ukConstraint) {
        if (fkConstraint == null || ukConstraint == null) {
            return false;
        }
        BaseTableInfo parentTableInfo = fkConstraint.constraint.getParentTableInfo();
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
        return Objects.equals(fkConstraint.constraint.getColumnRefPairs().get(0).second,
                ukConstraint.constraint.getUniqueColumns().get(0));
    }
}
