// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
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
    private final Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();

    // partitionConjuncts contains partition filters.
    private final List<ScalarOperator> partitionConjuncts = Lists.newArrayList();
    // After partition pruner prune, conjuncts that are not evaled will be send to backend.
    private final List<ScalarOperator> noEvalPartitionConjuncts = Lists.newArrayList();
    // nonPartitionConjuncts contains non-partition filters, and will be sent to backend.
    private final List<ScalarOperator> nonPartitionConjuncts = Lists.newArrayList();
    // List of conjuncts for min/max values that are used to skip data when scanning Parquet/Orc files.
    private final List<ScalarOperator> minMaxConjuncts = new ArrayList<>();
    // Map of columnRefOperator to column which column in minMaxConjuncts
    private final Map<ColumnRefOperator, Column> minMaxColumnRefMap = Maps.newHashMap();
    private final ImmutableMap<Column, Integer> columnToIds;
    private final Set<String> partitionColumns = Sets.newHashSet();

    public LogicalHiveScanOperator(Table table,
                                   Table.TableType tableType,
                                   List<ColumnRefOperator> outputColumns,
                                   Map<ColumnRefOperator, Column> columnRefMap,
                                   ImmutableMap<Column, Integer> columnToIds) {
        super(OperatorType.LOGICAL_HIVE_SCAN, table, outputColumns, columnRefMap);
        this.columnToIds = columnToIds;
        this.tableType = tableType;

        HiveTable hiveTable = (HiveTable) table;
        partitionColumns.addAll(hiveTable.getPartitionColumnNames());
    }

    public Map<Column, Integer> getColumnToIds() {
        return columnToIds;
    }

    public Table.TableType getTableType() {
        return tableType;
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

    @Override
    public void tryExtendOutputColumns(Set<ColumnRefOperator> newOutputColumns) {
        if (outputColumns.size() == 0) {
            return;
        }
        // make sure there is at least one materialized column in new output columns.
        if (newOutputColumns.stream().anyMatch(x -> !partitionColumns.contains(x.getName()))) {
            return;
        }

        // if not, we have to choose one materialized column from scan operator output columns
        // with the minimal cost.
        int smallestIndex = -1;
        int smallestColumnLength = Integer.MAX_VALUE;
        for (int index = 0; index < outputColumns.size(); ++index) {
            ColumnRefOperator columnRefOperator = outputColumns.get(index);
            if (partitionColumns.contains(columnRefOperator.getName())) {
                continue;
            }
            // at least this is a materialized column.
            if (smallestIndex == -1) {
                smallestIndex = index;
            }
            Type columnType = outputColumns.get(index).getType();
            // better this materialized column is a scalar type to save cost.
            if (columnType.isScalarType()) {
                int columnLength = columnType.getSlotSize();
                if (columnLength < smallestColumnLength) {
                    smallestIndex = index;
                    smallestColumnLength = columnLength;
                }
            }
        }
        Preconditions.checkArgument(smallestIndex != -1);
        newOutputColumns.add(outputColumns.get(smallestIndex));
    }
}
