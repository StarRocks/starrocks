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

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.List;
import java.util.Map;

public class PhysicalHiveScanOperator extends PhysicalScanOperator {
    private ScanOperatorPredicates predicates;
    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
    private Map<Integer, ScalarOperator> globalDictsExpr = Maps.newHashMap();

    public PhysicalHiveScanOperator(LogicalHiveScanOperator scanOperator) {
        super(OperatorType.PHYSICAL_HIVE_SCAN, scanOperator);
        this.predicates = scanOperator.getScanOperatorPredicates();
    }

    private PhysicalHiveScanOperator() {
        super(OperatorType.PHYSICAL_HIVE_SCAN);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ScanOperatorPredicates getScanOperatorPredicates() {
        return this.predicates;
    }

    @Override
    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
    }

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public Map<Integer, ScalarOperator> getGlobalDictsExpr() {
        return globalDictsExpr;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHiveScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalHiveScan(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        predicates.getNoEvalPartitionConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getPartitionConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getMinMaxConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getMinMaxColumnRefMap().keySet().forEach(refs::union);
        return refs;
    }

    public static class Builder
            extends PhysicalScanOperator.Builder<PhysicalHiveScanOperator, PhysicalScanOperator.Builder> {
        @Override
        protected PhysicalHiveScanOperator newInstance() {
            return new PhysicalHiveScanOperator();
        }

        @Override
        public PhysicalHiveScanOperator.Builder withOperator(PhysicalHiveScanOperator operator) {
            super.withOperator(operator);
            builder.predicates = operator.predicates;
            return this;
        }

        public PhysicalHiveScanOperator.Builder setScanPredicates(ScanOperatorPredicates predicates) {
            builder.predicates = predicates;
            return this;
        }

        public PhysicalHiveScanOperator.Builder setGlobalDicts(List<Pair<Integer, ColumnDict>> globalDicts) {
            builder.globalDicts = globalDicts;
            return this;
        }

        public PhysicalHiveScanOperator.Builder setGlobalDictsExpr(Map<Integer, ScalarOperator> globalDictsExpr) {
            builder.globalDictsExpr = globalDictsExpr;
            return this;
        }
    }

}
