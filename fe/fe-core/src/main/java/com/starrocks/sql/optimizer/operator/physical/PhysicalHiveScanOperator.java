// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PhysicalHiveScanOperator extends PhysicalScanOperator {
    private final Collection<Long> selectedPartitionIds;
    // id -> partition key
    private final Map<Long, PartitionKey> idToPartitionKey;

    // After partition pruner prune, conjuncts that are not evaled will be send to backend.
    private final List<ScalarOperator> noEvalPartitionConjuncts;
    // nonPartitionConjuncts contains non-partition filters, and will be sent to backend.
    private final List<ScalarOperator> nonPartitionConjuncts;
    // List of conjuncts for min/max values that are used to skip data when scanning Parquet/Orc files.
    private final List<ScalarOperator> minMaxConjuncts;
    // Map of columnRefOperator to column which column in minMaxConjuncts
    private final Map<ColumnRefOperator, Column> minMaxColumnRefMap;

    public PhysicalHiveScanOperator(Table table,
                                    List<ColumnRefOperator> outputColumns,
                                    Map<ColumnRefOperator, Column> columnRefMap,
                                    Collection<Long> selectedPartitionIds,
                                    Map<Long, PartitionKey> idToPartitionKey,
                                    List<ScalarOperator> noEvalPartitionConjuncts,
                                    List<ScalarOperator> nonPartitionConjuncts,
                                    List<ScalarOperator> minMaxConjuncts,
                                    Map<ColumnRefOperator, Column> minMaxColumnRefMap,
                                    long limit,
                                    ScalarOperator predicate) {
        super(OperatorType.PHYSICAL_HIVE_SCAN, table, outputColumns, columnRefMap, limit, predicate);
        this.selectedPartitionIds = selectedPartitionIds;
        this.idToPartitionKey = idToPartitionKey;
        this.noEvalPartitionConjuncts = noEvalPartitionConjuncts;
        this.nonPartitionConjuncts = nonPartitionConjuncts;
        this.minMaxConjuncts = minMaxConjuncts;
        this.minMaxColumnRefMap = minMaxColumnRefMap;
    }

    public Collection<Long> getSelectedPartitionIds() {
        return this.selectedPartitionIds;
    }

    public Map<Long, PartitionKey> getIdToPartitionKey() {
        return this.idToPartitionKey;
    }

    public List<ScalarOperator> getNoEvalPartitionConjuncts() {
        return noEvalPartitionConjuncts;
    }

    public List<ScalarOperator> getNonPartitionConjuncts() {
        return nonPartitionConjuncts;
    }

    public List<ScalarOperator> getMinMaxConjuncts() {
        return minMaxConjuncts;
    }

    public Map<ColumnRefOperator, Column> getMinMaxColumnRefMap() {
        return minMaxColumnRefMap;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        PhysicalHiveScanOperator that = (PhysicalHiveScanOperator) o;
        return Objects.equal(table, that.table) && Objects.equal(selectedPartitionIds, that.selectedPartitionIds) &&
                Objects.equal(idToPartitionKey, that.idToPartitionKey) &&
                Objects.equal(noEvalPartitionConjuncts, that.noEvalPartitionConjuncts) &&
                Objects.equal(nonPartitionConjuncts, that.nonPartitionConjuncts) &&
                Objects.equal(minMaxConjuncts, that.minMaxConjuncts) &&
                Objects.equal(minMaxColumnRefMap, that.minMaxColumnRefMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), table, selectedPartitionIds, idToPartitionKey,
                noEvalPartitionConjuncts, nonPartitionConjuncts, minMaxConjuncts, minMaxColumnRefMap);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        noEvalPartitionConjuncts.forEach(d -> refs.union(d.getUsedColumns()));
        nonPartitionConjuncts.forEach(d -> refs.union(d.getUsedColumns()));
        minMaxConjuncts.forEach(d -> refs.union(d.getUsedColumns()));
        minMaxColumnRefMap.keySet().forEach(refs::union);
        return refs;
    }
}
