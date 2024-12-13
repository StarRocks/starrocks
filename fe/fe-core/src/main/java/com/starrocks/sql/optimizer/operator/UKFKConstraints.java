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
package com.starrocks.sql.optimizer.operator;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;

<<<<<<< HEAD
import java.util.Map;

public class UKFKConstraints {
    // ColumnRefOperator::id -> UniqueConstraint
    private final Map<Integer, UniqueConstraintWrapper> uniqueKeys = Maps.newHashMap();
    // The unique key of the data table needs to be collected when eliminating aggregation
    private Map<Integer, UniqueConstraintWrapper> tableUniqueKeys = Maps.newHashMap();
=======
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class UKFKConstraints {
    // ColumnRefOperator::id -> UniqueConstraint
    // When propagating constraints from the child node to the parent node, it needs to ensure that the parent node includes
    // all outputs of the child node.
    private final Map<Integer, UniqueConstraintWrapper> uniqueKeys = Maps.newHashMap();
    // ColumnRefOperator::id -> UniqueConstraint
    // ukColumnRefs no longer satisfies uniqueness, but the relationship between ukColumnRefs and nonUKColumnRefs still
    // satisfies, that is, the rows with the same ukColumnRefs must be the same nonUKColumnRefs.
    private final Map<Integer, UniqueConstraintWrapper> relaxedUniqueKeys = Maps.newHashMap();
    // aggUniqueKeys contains all unique keys, including multi-column unique keys, while uniqueKeys only contains single-column
    // unique keys. It is only used to eliminate aggregation and not to eliminate joins based on unique keys and foreign keys.
    // Therefore, when propagating constraints from the child node to the parent node, it is not necessary to ensure that the
    // parent node includes all outputs of the child node; only needs to ensure that the child node’s output has no duplicate rows.
    // TODO: unify aggUniqueKeys and uniqueKeys, and apply them to eliminate Join.
    private final List<UniqueConstraintWrapper> aggUniqueKeys = Lists.newArrayList();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    // ColumnRefOperator::id -> ForeignKeyConstraint
    private final Map<Integer, ForeignKeyConstraintWrapper> foreignKeys = Maps.newHashMap();
    private JoinProperty joinProperty;

    public void addUniqueKey(int id, UniqueConstraintWrapper uniqueKey) {
        uniqueKeys.put(id, uniqueKey);
<<<<<<< HEAD
    }

    public void addTableUniqueKey(int id, UniqueConstraintWrapper uniqueKey) {
        tableUniqueKeys.put(id, uniqueKey);
=======
        relaxedUniqueKeys.put(id, uniqueKey);
    }

    public void addAggUniqueKey(UniqueConstraintWrapper uniqueKey) {
        aggUniqueKeys.add(uniqueKey);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public void addForeignKey(int id, ForeignKeyConstraintWrapper foreignKey) {
        foreignKeys.put(id, foreignKey);
    }

    public UniqueConstraintWrapper getUniqueConstraint(Integer id) {
        return uniqueKeys.get(id);
    }

<<<<<<< HEAD
    public void setTableUniqueKeys(Map<Integer, UniqueConstraintWrapper> tableUniqueKeys) {
        this.tableUniqueKeys = tableUniqueKeys;
    }

    public Map<Integer, UniqueConstraintWrapper> getTableUniqueKeys() {
        return tableUniqueKeys;
=======
    public List<UniqueConstraintWrapper> getAggUniqueKeys() {
        return aggUniqueKeys;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public ForeignKeyConstraintWrapper getForeignKeyConstraint(Integer id) {
        return foreignKeys.get(id);
    }

<<<<<<< HEAD
=======
    public UniqueConstraintWrapper getRelaxedUniqueConstraint(Integer id) {
        return relaxedUniqueKeys.get(id);
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public JoinProperty getJoinProperty() {
        return joinProperty;
    }

<<<<<<< HEAD
    public void setJoinProperty(
            JoinProperty joinProperty) {
=======
    public void setJoinProperty(JoinProperty joinProperty) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        this.joinProperty = joinProperty;
    }

    public static UKFKConstraints inheritFrom(UKFKConstraints from, ColumnRefSet toOutputColumns) {
        UKFKConstraints clone = new UKFKConstraints();
        from.uniqueKeys.entrySet().stream()
                .filter(entry -> toOutputColumns.contains(entry.getKey()))
                .forEach(entry -> clone.uniqueKeys.put(entry.getKey(), entry.getValue()));
<<<<<<< HEAD
        from.foreignKeys.entrySet().stream()
                .filter(entry -> toOutputColumns.contains(entry.getKey()))
                .forEach(entry -> clone.foreignKeys.put(entry.getKey(), entry.getValue()));
        if (!(from.getTableUniqueKeys().isEmpty()) &&
                toOutputColumns.containsAll(Lists.newArrayList((from.getTableUniqueKeys().keySet())))) {
            clone.setTableUniqueKeys(from.getTableUniqueKeys());
        }
=======
        clone.inheritForeignKey(from, toOutputColumns);
        clone.inheritRelaxedUniqueKey(from, toOutputColumns);
        clone.inheritAggUniqueKey(from, toOutputColumns);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        return clone;
    }

    public void inheritForeignKey(UKFKConstraints other, ColumnRefSet outputColumns) {
        other.foreignKeys.entrySet().stream()
                .filter(entry -> outputColumns.contains(entry.getKey()))
                .forEach(entry -> foreignKeys.put(entry.getKey(), entry.getValue()));
    }

<<<<<<< HEAD
=======
    public void inheritRelaxedUniqueKey(UKFKConstraints other, ColumnRefSet outputColumns) {
        Stream.concat(other.uniqueKeys.entrySet().stream(), other.relaxedUniqueKeys.entrySet().stream())
                .filter(entry -> outputColumns.contains(entry.getKey()))
                .forEach(entry -> relaxedUniqueKeys.put(entry.getKey(), entry.getValue()));
    }

    public void inheritAggUniqueKey(UKFKConstraints other, ColumnRefSet outputColumns) {
        other.aggUniqueKeys.stream()
                .filter(uk -> outputColumns.containsAll(uk.ukColumnRefs))
                .forEach(aggUniqueKeys::add);
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public static final class UniqueConstraintWrapper {
        public final UniqueConstraint constraint;
        public final ColumnRefSet nonUKColumnRefs;
        public final boolean isIntact;

<<<<<<< HEAD
        public UniqueConstraintWrapper(UniqueConstraint constraint,
                                       ColumnRefSet nonUKColumnRefs, boolean isIntact) {
            this.constraint = constraint;
            this.nonUKColumnRefs = nonUKColumnRefs;
            this.isIntact = isIntact;
=======
        public final ColumnRefSet ukColumnRefs;

        public UniqueConstraintWrapper(UniqueConstraint constraint, ColumnRefSet nonUKColumnRefs, boolean isIntact,
                                       ColumnRefSet ukColumnRefs) {
            this.constraint = constraint;
            this.nonUKColumnRefs = nonUKColumnRefs;
            this.isIntact = isIntact;
            this.ukColumnRefs = ukColumnRefs;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

    public static final class ForeignKeyConstraintWrapper {
        public final ForeignKeyConstraint constraint;
        public final boolean isOrderByFK;

        public ForeignKeyConstraintWrapper(ForeignKeyConstraint constraint, boolean isOrderByFK) {
            this.constraint = constraint;
            this.isOrderByFK = isOrderByFK;
        }
    }

    public static final class JoinProperty {
        public final BinaryPredicateOperator predicate;
        public final UniqueConstraintWrapper ukConstraint;
        public final ForeignKeyConstraintWrapper fkConstraint;
        public final ColumnRefOperator ukColumnRef;
        public final ColumnRefOperator fkColumnRef;
        public final boolean isLeftUK;
        public Expr ukColumn;
        public Expr fkColumn;

        public JoinProperty(BinaryPredicateOperator predicate,
                            UniqueConstraintWrapper ukConstraint,
                            ForeignKeyConstraintWrapper fkConstraint,
                            ColumnRefOperator ukColumnRef,
                            ColumnRefOperator fkColumnRef, boolean isLeftUK) {
            this.predicate = predicate;
            this.ukConstraint = ukConstraint;
            this.fkConstraint = fkConstraint;
            this.ukColumnRef = ukColumnRef;
            this.fkColumnRef = fkColumnRef;
            this.isLeftUK = isLeftUK;
        }

        public void buildExpr(ExecPlan context) {
            ukColumn = ScalarOperatorToExpr.buildExecExpression(ukColumnRef,
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));
            fkColumn = ScalarOperatorToExpr.buildExecExpression(fkColumnRef,
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));
        }
    }
}
