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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class PhysicalScanOperator extends PhysicalOperator {
    protected final Table table;
    protected List<ColumnRefOperator> outputColumns;
    /**
     * ColumnRefMap is the map from column reference to starrocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected final ImmutableMap<ColumnRefOperator, Column> colRefToColumnMetaMap;

    public PhysicalScanOperator(OperatorType type, Table table,
                                Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                long limit,
                                ScalarOperator predicate,
                                Projection projection) {
        super(type);
        this.table = Objects.requireNonNull(table, "table is null");
        this.colRefToColumnMetaMap = ImmutableMap.copyOf(colRefToColumnMetaMap);
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;

        if (this.projection != null) {
            ColumnRefSet usedColumns = new ColumnRefSet();
            for (ScalarOperator scalarOperator : this.projection.getColumnRefMap().values()) {
                usedColumns.union(scalarOperator.getUsedColumns());
            }
            for (ScalarOperator scalarOperator : this.projection.getCommonSubOperatorMap().values()) {
                usedColumns.union(scalarOperator.getUsedColumns());
            }

            ImmutableList.Builder<ColumnRefOperator> outputBuilder = ImmutableList.builder();
            for (ColumnRefOperator columnRefOperator : colRefToColumnMetaMap.keySet()) {
                if (usedColumns.contains(columnRefOperator)) {
                    outputBuilder.add(columnRefOperator);
                }
            }
            outputColumns = outputBuilder.build();
        } else {
            outputColumns = ImmutableList.copyOf(colRefToColumnMetaMap.keySet());
        }
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return outputColumns;
    }

    public List<ColumnRefOperator> getRealOutputColumns() {
        if (projection != null) {
            return projection.getOutputColumns();
        } else {
            return outputColumns;
        }
    }

    public Map<ColumnRefOperator, Column> getColRefToColumnMetaMap() {
        return colRefToColumnMetaMap;
    }

    public Table getTable() {
        return table;
    }

    public ScanOperatorPredicates getScanOperatorPredicates() throws AnalysisException {
        throw new AnalysisException("Operation getScanOperatorPredicates() is not supported by this ScanOperator.");
    }

    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) throws AnalysisException {
        throw new AnalysisException("Operation setScanOperatorPredicates(...) is not supported by this ScanOperator.");
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        colRefToColumnMetaMap.keySet().forEach(set::union);
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalScanOperator that = (PhysicalScanOperator) o;
        return Objects.equals(table.getId(), that.table.getId()) &&
                Objects.equals(colRefToColumnMetaMap, that.getColRefToColumnMetaMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table.getId(), colRefToColumnMetaMap.keySet());
    }
}