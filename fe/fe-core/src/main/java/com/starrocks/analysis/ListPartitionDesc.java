// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * entrance describe for list partition
 */
public class ListPartitionDesc extends PartitionDesc {

    // describe for statement like `PARTITION p1 VALUES IN ("beijing","chongqing")`
    private final List<SingleItemListPartitionDesc> singleListPartitionDescs;
    // describe for statement like `PARTITION p1 VALUES IN (("2022-04-01", "beijing"))`
    private final List<MultiItemListPartitionDesc> multiListPartitionDescs;

    private final List<String> partitionColNames;

    public ListPartitionDesc(List<String> partitionColNames,
                             List<PartitionDesc> partitionDescs) {
        super.type = PartitionType.LIST;
        this.partitionColNames = partitionColNames;
        this.singleListPartitionDescs = Lists.newArrayList();
        this.multiListPartitionDescs = Lists.newArrayList();
        if (partitionDescs != null) {
            for (PartitionDesc partitionDesc : partitionDescs) {
                if (partitionDesc instanceof SingleItemListPartitionDesc) {
                    this.singleListPartitionDescs.add((SingleItemListPartitionDesc) partitionDesc);
                } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
                    this.multiListPartitionDescs.add((MultiItemListPartitionDesc) partitionDesc);
                }
            }
        }
    }

    public List<String> findAllPartitionName() {
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
        if (this.partitionColNames == null || this.partitionColNames.isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }
        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String partitionCol : this.partitionColNames) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }
            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equals(partitionCol)) {
                    if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()) {
                        throw new AnalysisException(String.format("Invalid partition column '%s': %s",
                                columnDef.getName(), "invalid data type " + columnDef.getType()));
                    }
                    if (!columnDef.isKey() && columnDef.getAggregateType() != AggregateType.NONE) {
                        throw new AnalysisException("The partition column could not be aggregated column");
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
        Set<String> multiListPartitionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<List<String>> allMultiValues = Lists.newArrayList();
        for (MultiItemListPartitionDesc desc : this.multiListPartitionDescs) {
            if (!multiListPartitionName.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            desc.analyze(this.partitionColNames.size(), tableProperties);
            allMultiValues.addAll(desc.getMultiValues());
        }
        this.analyzeDuplicateValues(this.partitionColNames.size(), allMultiValues);
    }

    private void analyzeSingleListPartition(Map<String, String> copiedProperties) throws AnalysisException {
        List<String> allValues = Lists.newArrayList();
        Set<String> singListPartitionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SingleItemListPartitionDesc desc : this.singleListPartitionDescs) {
            if (!singListPartitionName.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            desc.analyze(this.partitionColNames.size(), copiedProperties);
            allValues.addAll(desc.getValues());
        }
        this.analyzeDuplicateValues(allValues);
    }

    /**
     * Check if duplicate values are found
     * If the value of the member in the same position is equals, it is considered a duplicate
     * @param partitionColSize the partition column size
     * @param allMultiValues values from multi list partition
     * @throws AnalysisException
     */
    private void analyzeDuplicateValues(int partitionColSize, List<List<String>> allMultiValues) throws AnalysisException {
        List<List<String>> tempMultiValues = new ArrayList<>(allMultiValues.size());
        for (List<String> values : allMultiValues) {
            for (List<String> tempValues : tempMultiValues){
                int duplicatedSize = 0;
                for (int i = 0; i < values.size(); i++) {
                    if(values.get(i).equals(tempValues.get(i))){
                        duplicatedSize++;
                    }
                }
                if (duplicatedSize == partitionColSize) {
                    throw new AnalysisException("Duplicate values " +
                            "(" + String.join(",", values) + ") not allow");
                }
            }
            tempMultiValues.add(values);
        }
    }

    /**
     * Check if duplicate values are found
     * Use hashSet to check duplicate value
     * @param allValues values from single list partition
     * @throws AnalysisException
     */
    private void analyzeDuplicateValues(List<String> allValues) throws AnalysisException {
        Set<String> hashSet = new HashSet<>();
        for (String value : allValues) {
            if (!hashSet.add(value)) {
                throw new AnalysisException("Duplicated value" + value);
            }
        }
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId, boolean isTemp) {
        List<Column> partitionColumns = this.findPartitionColumns(columns);
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

    private List<Column> findPartitionColumns(List<Column> columns) {
        List<Column> partitionColumns = Lists.newArrayList();
        for (String colName : this.partitionColNames) {
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    partitionColumns.add(column);
                }
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
        if (!this.multiListPartitionDescs.isEmpty()) {
            String multiList = this.multiListPartitionDescs.stream()
                    .map(item -> "  " + item.toSql())
                    .collect(Collectors.joining(",\n"));
            sb.append(multiList);
        }
        if (!this.singleListPartitionDescs.isEmpty()) {
            String sinleList = this.singleListPartitionDescs.stream()
                    .map(item -> "  " + item.toSql())
                    .collect(Collectors.joining(",\n"));
            sb.append(sinleList);
        }
        sb.append("\n)");
        return sb.toString();
    }
}
