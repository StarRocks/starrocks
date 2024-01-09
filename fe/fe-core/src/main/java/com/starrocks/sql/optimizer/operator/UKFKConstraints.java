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

import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;

import java.util.Map;

public class UKFKConstraints {
    // ColumnRefOperator::id -> UniqueConstraint
    private final Map<Integer, UniqueConstraintWrapper> uniqueKeys = Maps.newHashMap();
    // ColumnRefOperator::id -> ForeignKeyConstraint
    private final Map<Integer, ForeignKeyConstraint> foreignKeys = Maps.newHashMap();
    private JoinProperty joinProperty;

    public void addUniqueKey(int id, UniqueConstraintWrapper uniqueKey) {
        uniqueKeys.put(id, uniqueKey);
    }

    public void addForeignKey(int id, ForeignKeyConstraint foreignKey) {
        foreignKeys.put(id, foreignKey);
    }

    public UniqueConstraintWrapper getUniqueConstraint(Integer id) {
        return uniqueKeys.get(id);
    }

    public ForeignKeyConstraint getForeignKeyConstraint(Integer id) {
        return foreignKeys.get(id);
    }

    public JoinProperty getJoinProperty() {
        return joinProperty;
    }

    public void setJoinProperty(
            JoinProperty joinProperty) {
        this.joinProperty = joinProperty;
    }

    public static UKFKConstraints inheritFrom(UKFKConstraints from, ColumnRefSet toOutputColumns) {
        UKFKConstraints clone = new UKFKConstraints();
        from.uniqueKeys.entrySet().stream()
                .filter(entry -> toOutputColumns.contains(entry.getKey()))
                .forEach(entry -> clone.uniqueKeys.put(entry.getKey(), entry.getValue()));
        from.foreignKeys.entrySet().stream()
                .filter(entry -> toOutputColumns.contains(entry.getKey()))
                .forEach(entry -> clone.foreignKeys.put(entry.getKey(), entry.getValue()));
        return clone;
    }

    public void inheritForeignKey(UKFKConstraints other, ColumnRefSet outputColumns) {
        other.foreignKeys.entrySet().stream()
                .filter(entry -> outputColumns.contains(entry.getKey()))
                .forEach(entry -> foreignKeys.put(entry.getKey(), entry.getValue()));
    }

    public static final class UniqueConstraintWrapper {
        public final UniqueConstraint constraint;
        public final ColumnRefSet nonUKColumnRefs;
        public final boolean isIntact;

        public UniqueConstraintWrapper(UniqueConstraint constraint,
                                       ColumnRefSet nonUKColumnRefs, boolean isIntact) {
            this.constraint = constraint;
            this.nonUKColumnRefs = nonUKColumnRefs;
            this.isIntact = isIntact;
        }
    }

    public static final class JoinProperty {
        public final BinaryPredicateOperator predicate;
        public final UniqueConstraintWrapper ukConstraint;
        public final ForeignKeyConstraint fkConstraint;
        public final ColumnRefOperator ukColumnRef;
        public final ColumnRefOperator fkColumnRef;
        public final boolean isLeftUK;
        public boolean isOneMatchProbe;

        public JoinProperty(BinaryPredicateOperator predicate,
                            UniqueConstraintWrapper ukConstraint,
                            ForeignKeyConstraint fkConstraint,
                            ColumnRefOperator ukColumnRef,
                            ColumnRefOperator fkColumnRef, boolean isLeftUK) {
            this.predicate = predicate;
            this.ukConstraint = ukConstraint;
            this.fkConstraint = fkConstraint;
            this.ukColumnRef = ukColumnRef;
            this.fkColumnRef = fkColumnRef;
            this.isLeftUK = isLeftUK;
        }
    }
}
