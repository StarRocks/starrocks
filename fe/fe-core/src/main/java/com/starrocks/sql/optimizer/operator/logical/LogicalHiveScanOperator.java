// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogicalHiveScanOperator extends LogicalScanOperator {
    private final Table.TableType tableType;
    // id -> partition key
    private Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();

    // partitionConjuncts contains partition filters.
    private List<ScalarOperator> partitionConjuncts = Lists.newArrayList();
    // After partition pruner prune, conjuncts that are not evaled will be send to backend.
    private List<ScalarOperator> noEvalPartitionConjuncts = Lists.newArrayList();
    // nonPartitionConjuncts contains non-partition filters, and will be sent to backend.
    private List<ScalarOperator> nonPartitionConjuncts = Lists.newArrayList();
    // List of conjuncts for min/max values that are used to skip data when scanning Parquet/Orc files.
    private List<ScalarOperator> minMaxConjuncts = new ArrayList<>();
    // Map of columnRefOperator to column which column in minMaxConjuncts
    private Map<ColumnRefOperator, Column> minMaxColumnRefMap = Maps.newHashMap();
    private Set<String> partitionColumns = Sets.newHashSet();

    public LogicalHiveScanOperator(Table table,
                                   Table.TableType tableType,
                                   Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                   Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                   long limit,
                                   ScalarOperator predicate) {
        super(OperatorType.LOGICAL_HIVE_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate, null);

        Preconditions.checkState(table instanceof HiveTable);
        this.tableType = tableType;
        HiveTable hiveTable = (HiveTable) table;
        partitionColumns.addAll(hiveTable.getPartitionColumnNames());
    }

    private LogicalHiveScanOperator(LogicalHiveScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_HIVE_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());

        this.tableType = builder.tableType;
        this.idToPartitionKey = builder.idToPartitionKey;
        this.selectedPartitionIds = builder.selectedPartitionIds;
        this.partitionConjuncts = builder.partitionConjuncts;
        this.noEvalPartitionConjuncts = builder.noEvalPartitionConjuncts;
        this.nonPartitionConjuncts = builder.nonPartitionConjuncts;
        this.minMaxConjuncts = builder.minMaxConjuncts;
        this.minMaxColumnRefMap = builder.minMaxColumnRefMap;
        this.partitionColumns = builder.partitionColumns;
    }

    public Table.TableType getTableType() {
        return tableType;
    }

    public Set<String> getPartitionColumns() {
        return partitionColumns;
    }

    public Map<Long, PartitionKey> getIdToPartitionKey() {
        return idToPartitionKey;
    }

    public List<ScalarOperator> getPartitionConjuncts() {
        return partitionConjuncts;
    }

    public List<ScalarOperator> getNoEvalPartitionConjuncts() {
        return noEvalPartitionConjuncts;
    }

    public List<ScalarOperator> getNonPartitionConjuncts() {
        return nonPartitionConjuncts;
    }

    public Collection<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    public void setSelectedPartitionIds(Collection<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
    }

    public List<ScalarOperator> getMinMaxConjuncts() {
        return minMaxConjuncts;
    }

    public Map<ColumnRefOperator, Column> getMinMaxColumnRefMap() {
        return minMaxColumnRefMap;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalHiveScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalHiveScanOperator, LogicalHiveScanOperator.Builder> {
        private Table.TableType tableType;
        private Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
        private Collection<Long> selectedPartitionIds = Lists.newArrayList();
        private List<ScalarOperator> partitionConjuncts = Lists.newArrayList();
        private List<ScalarOperator> noEvalPartitionConjuncts = Lists.newArrayList();
        private List<ScalarOperator> nonPartitionConjuncts = Lists.newArrayList();
        private List<ScalarOperator> minMaxConjuncts = new ArrayList<>();
        private Map<ColumnRefOperator, Column> minMaxColumnRefMap = Maps.newHashMap();
        private Set<String> partitionColumns = Sets.newHashSet();

        @Override
        public LogicalHiveScanOperator build() {
            return new LogicalHiveScanOperator(this);
        }

        @Override
        public LogicalHiveScanOperator.Builder withOperator(LogicalHiveScanOperator scanOperator) {
            super.withOperator(scanOperator);

            this.tableType = scanOperator.tableType;
            this.idToPartitionKey = scanOperator.idToPartitionKey;
            this.selectedPartitionIds = scanOperator.selectedPartitionIds;
            this.partitionConjuncts = scanOperator.partitionConjuncts;
            this.noEvalPartitionConjuncts = scanOperator.noEvalPartitionConjuncts;
            this.nonPartitionConjuncts = scanOperator.nonPartitionConjuncts;
            this.minMaxConjuncts = scanOperator.minMaxConjuncts;
            this.minMaxColumnRefMap = scanOperator.minMaxColumnRefMap;
            this.partitionColumns = scanOperator.partitionColumns;
            return this;
        }
    }
}
