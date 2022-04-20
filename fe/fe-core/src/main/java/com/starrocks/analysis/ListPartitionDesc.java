// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.*;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;

import java.util.*;
import java.util.stream.Collectors;

/**
 *  entrance describe for list partition
 */
public class ListPartitionDesc extends PartitionDesc {

    // describe for statement like `PARTITION p1 VALUES IN ("beijing","chongqing")`
    private final List<SingleListPartitionDesc> singleListPartitionDescs;
    // describe for statement like `PARTITION p1 VALUES IN (("2022-04-01", "beijing"))`
    private final List<MultiListPartitionDesc> multiListPartitionDescs;

    private final List<String> partitionColNames;

    public ListPartitionDesc(List<String> partitionColNames,
                             List<PartitionDesc> partitionDescs) {
        super.type = PartitionType.LIST;
        this.partitionColNames = partitionColNames;
        this.singleListPartitionDescs = Lists.newArrayList();
        this.multiListPartitionDescs = Lists.newArrayList();
        if (partitionDescs != null) {
            for (PartitionDesc partitionDesc : partitionDescs) {
                if (partitionDesc instanceof SingleListPartitionDesc) {
                    singleListPartitionDescs.add((SingleListPartitionDesc) partitionDesc);
                } else if (partitionDesc instanceof MultiListPartitionDesc) {
                    multiListPartitionDescs.add((MultiListPartitionDesc) partitionDesc);
                }
            }
        }
    }

    public List<String> findAllParitionName() {
        List<String> partitionNames = new ArrayList<>();
        this.singleListPartitionDescs.forEach(desc -> partitionNames.add(desc.getPartitionName()));
        this.multiListPartitionDescs.forEach(desc -> partitionNames.add(desc.getPartitionName()));
        return partitionNames;
    }

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> tableProperties) throws AnalysisException {
        // analyze partition columns
        this.analyzePartitionColumns(columnDefs);
        // analyze single list property
        this.analyzeSingleListPartition(tableProperties);
        // analyze multi list partition
        this.analyzeMultiListPartition(tableProperties);
    }

    private void analyzePartitionColumns(List<ColumnDef> columnDefs) throws AnalysisException {
        if (partitionColNames == null || partitionColNames.isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }
        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String partitionCol : partitionColNames) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }
            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equals(partitionCol)) {
                    if (!columnDef.isKey() && columnDef.getAggregateType() != AggregateType.NONE) {
                        throw new AnalysisException("The partition column could not be aggregated column");
                    }
                    if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()) {
                        throw new AnalysisException(String.format("Invalid partition column '%s': %s",
                                columnDef.getName(), "invalid data type " + columnDef.getType()));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new AnalysisException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }
    }

    private void analyzeMultiListPartition(Map<String, String> tableProperties) throws AnalysisException {
        Set<String> multiListParttionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (this.multiListPartitionDescs.size() != 0) {
            for (MultiListPartitionDesc desc : this.multiListPartitionDescs) {
                if (!multiListParttionName.add(desc.getPartitionName())) {
                    throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
                }
                desc.analyze(partitionColNames.size(), tableProperties);
            }
        }
    }

    private void analyzeSingleListPartition(Map<String, String> copiedProperties) throws AnalysisException {
        Set<String> singListParttionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SingleListPartitionDesc desc : this.singleListPartitionDescs) {
            if (!singListParttionName.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            desc.analyze(this.partitionColNames.size(), copiedProperties);
        }
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        List<Column> partitionColumns = this.toPartitionColumns(columns);
        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(super.type, partitionColumns);
        this.singleListPartitionDescs.forEach(desc -> {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            listPartitionInfo.setDataProperty(partitionId, desc.getPartitionDataProperty());
            listPartitionInfo.setIsInMemory(partitionId, desc.isInMemory());
            listPartitionInfo.setTabletType(partitionId, desc.getTabletType());
            listPartitionInfo.setReplicationNum(partitionId, desc.getReplicationNum());
            listPartitionInfo.setValues(partitionId, desc.getValues());
        });
        this.multiListPartitionDescs.forEach(desc -> {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            listPartitionInfo.setDataProperty(partitionId, desc.getPartitionDataProperty());
            listPartitionInfo.setIsInMemory(partitionId, desc.isInMemory());
            listPartitionInfo.setTabletType(partitionId, desc.getTabletType());
            listPartitionInfo.setReplicationNum(partitionId, desc.getReplicationNum());
            listPartitionInfo.setMultiValues(partitionId, desc.getMultiValues());
        });
        return listPartitionInfo;
    }

    /**
     * check and getPartitionColumns
     *
     * @param columns columns from table
     * @return
     * @throws DdlException
     */
    private List<Column> toPartitionColumns(List<Column> columns) throws DdlException {
        List<Column> partitionColumns = Lists.newArrayList();
        for (String colName : partitionColNames) {
            boolean find = false;
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    if (!column.isKey() && column.getAggregationType() != AggregateType.NONE) {
                        throw new DdlException("The partition column could not be aggregated column");
                    }
                    if (column.getType().isFloatingPointType() || column.getType().isComplexType()) {
                        throw new DdlException(String.format("Invalid partition column '%s': %s",
                                column.getName(), "invalid data type " + column.getType()));
                    }
                    partitionColumns.add(column);
                    find = true;
                    break;
                }
            }
            if (!find) {
                throw new DdlException("Partition column[" + colName + "] does not found");
            }
        }
        return partitionColumns;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY LIST(");
        sb.append(this.partitionColNames.stream()
                .map(item -> "`" + item + "`")
                .collect(Collectors.joining(",")));
        sb.append(")(\n");
        if (!multiListPartitionDescs.isEmpty()) {
            String multiList = multiListPartitionDescs.stream()
                    .map(item -> "  " + item.toSql())
                    .collect(Collectors.joining(",\n"));
            sb.append(multiList);
        }
        if (!singleListPartitionDescs.isEmpty()) {
            String sinleList = singleListPartitionDescs.stream()
                    .map(item -> "  " + item.toSql())
                    .collect(Collectors.joining(",\n"));
            sb.append(sinleList);
        }
        sb.append("\n)");
        return sb.toString();
    }
}
