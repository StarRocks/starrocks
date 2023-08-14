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


package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    // for automatic partition table is ture. otherwise is false
    protected boolean isAutoPartitionTable = false;

    public ListPartitionDesc(List<String> partitionColNames,
                             List<PartitionDesc> partitionDescs) {
        this(partitionColNames, partitionDescs, NodePosition.ZERO);
    }

    public ListPartitionDesc(List<String> partitionColNames,
                             List<PartitionDesc> partitionDescs, NodePosition pos) {
        super(pos);
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

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    public List<String> findAllPartitionNames() {
        List<String> partitionNames = new ArrayList<>();
        this.singleListPartitionDescs.forEach(desc -> partitionNames.add(desc.getPartitionName()));
        this.multiListPartitionDescs.forEach(desc -> partitionNames.add(desc.getPartitionName()));
        return partitionNames;
    }

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> tableProperties) throws AnalysisException {
        // analyze partition columns
        List<ColumnDef> columnDefList = this.analyzePartitionColumns(columnDefs);
        for (ColumnDef columnDef : columnDefList) {
            if (columnDef.isAllowNull()) {
                throw new AnalysisException("The list partition column does not support allow null currently, column:["
                        + columnDef.getName() + "] should be set to not null.");
            }
        }
        // analyze single list property
        this.analyzeSingleListPartition(tableProperties, columnDefList);
        // analyze multi list partition
        this.analyzeMultiListPartition(tableProperties, columnDefList);
    }

    public List<ColumnDef> analyzePartitionColumns(List<ColumnDef> columnDefs) throws AnalysisException {
        if (this.partitionColNames == null || this.partitionColNames.isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }
        List<ColumnDef> partitionColumns = new ArrayList<>(this.partitionColNames.size());
        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String partitionCol : this.partitionColNames) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }
            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equals(partitionCol)) {
                    if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()
                            || columnDef.getType().isDecimalOfAnyVersion()) {
                        throw new AnalysisException(String.format("Invalid partition column '%s': %s",
                                columnDef.getName(), "invalid data type " + columnDef.getType()));
                    }
                    if (!columnDef.isKey() && columnDef.getAggregateType() != AggregateType.NONE) {
                        throw new AnalysisException("The partition column could not be aggregated column"
                                + " and unique table's partition column must be key column");
                    }
                    found = true;
                    partitionColumns.add(columnDef);
                    break;
                }
            }
            if (!found) {
                throw new AnalysisException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }
        return partitionColumns;
    }

    public void analyzeExternalPartitionColumns(List<ColumnDef> columnDefs, String engineName) {
        if (this.partitionColNames == null || this.partitionColNames.isEmpty()) {
            throw new SemanticException("No partition columns.");
        }
        List<ColumnDef> partitionColumns = new ArrayList<>(this.partitionColNames.size());
        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String partitionCol : this.partitionColNames) {
            if (!partColNames.add(partitionCol)) {
                throw new SemanticException("Duplicated partition column " + partitionCol);
            }
            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equals(partitionCol)) {
                    if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()
                            || columnDef.getType().isDecimalOfAnyVersion()) {
                        throw new SemanticException(String.format("Invalid partition column '%s': %s",
                                columnDef.getName(), "invalid data type " + columnDef.getType()));
                    }
                    found = true;
                    partitionColumns.add(columnDef);
                    break;
                }
            }
            if (!found) {
                throw new SemanticException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }

        if (engineName.equalsIgnoreCase("iceberg")) {
            checkIcebergPartitionColPos(columnDefs);
        } else if (engineName.equalsIgnoreCase("hive")) {
            checkHivePartitionColPos(columnDefs);
        }
    }

    public void checkIcebergPartitionColPos(List<ColumnDef> columnDefs) {
        for (int i = 0; i < columnDefs.size() - partitionColNames.size(); i++) {
            String colName = columnDefs.get(i).getName();
            if (partitionColNames.contains(colName)) {
                throw new SemanticException("Partition columns must be at the end of column defs");
            }
        }
    }

    public void checkHivePartitionColPos(List<ColumnDef> columnDefs) {
        List<String> allColNames = columnDefs.stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());

        if (allColNames.size() == partitionColNames.size()) {
            throw new SemanticException("Table contains only partition columns");
        }

        if (!allColNames.subList(allColNames.size() - partitionColNames.size(), allColNames.size()).equals(partitionColNames)) {
            throw new SemanticException("Partition columns must be the last columns in the table and " +
                    "in the same order as partition by clause: %s", partitionColNames);
        }
    }

    private void analyzeMultiListPartition(Map<String, String> tableProperties,
                                           List<ColumnDef> columnDefList) throws AnalysisException {
        Set<String> multiListPartitionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<List<LiteralExpr>> allMultiLiteralExprValues = Lists.newArrayList();
        for (MultiItemListPartitionDesc desc : this.multiListPartitionDescs) {
            if (!multiListPartitionName.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            desc.analyze(columnDefList, tableProperties);
            allMultiLiteralExprValues.addAll(desc.getMultiLiteralExprValues());
        }
        this.analyzeDuplicateValues(this.partitionColNames.size(), allMultiLiteralExprValues);
    }

    private void analyzeSingleListPartition(Map<String, String> tableProperties, List<ColumnDef> columnDefList)
            throws AnalysisException {
        List<LiteralExpr> allLiteralExprValues = Lists.newArrayList();
        Set<String> singListPartitionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SingleItemListPartitionDesc desc : this.singleListPartitionDescs) {
            if (!singListPartitionName.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            desc.analyze(columnDefList, tableProperties);
            allLiteralExprValues.addAll(desc.getLiteralExprValues());
        }
        this.analyzeDuplicateValues(allLiteralExprValues);
    }

    /**
     * Check if duplicate values are found
     * If the value of the member in the same position is equals, it is considered a duplicate
     *
     * @param partitionColSize          the partition column size
     * @param allMultiLiteralExprValues values from multi list partition
     * @throws AnalysisException
     */
    private void analyzeDuplicateValues(int partitionColSize, List<List<LiteralExpr>> allMultiLiteralExprValues)
            throws AnalysisException {
        for (int i = 0; i < allMultiLiteralExprValues.size(); i++) {
            List<LiteralExpr> literalExprValues1 = allMultiLiteralExprValues.get(i);
            for (int j = i + 1; j < allMultiLiteralExprValues.size(); j++) {
                List<LiteralExpr> literalExprValues2 = allMultiLiteralExprValues.get(j);
                int duplicatedSize = 0;
                for (int k = 0; k < literalExprValues1.size(); k++) {
                    String value = literalExprValues1.get(k).getStringValue();
                    String tmpValue = literalExprValues2.get(k).getStringValue();
                    if (value.equals(tmpValue)) {
                        duplicatedSize++;
                    }
                }
                if (duplicatedSize == partitionColSize) {
                    List<String> msg = literalExprValues1.stream()
                            .map(value -> ("\"" + value.getStringValue() + "\""))
                            .collect(Collectors.toList());
                    throw new AnalysisException("Duplicate values " +
                            "(" + String.join(",", msg) + ") not allow");
                }
            }
        }
    }

    /**
     * Check if duplicate values are found
     * Use hashSet to check duplicate value
     *
     * @param allLiteralExprValues values from single list partition
     * @throws AnalysisException
     */
    private void analyzeDuplicateValues(List<LiteralExpr> allLiteralExprValues) throws AnalysisException {
        Set<String> hashSet = new HashSet<>();
        for (LiteralExpr value : allLiteralExprValues) {
            if (!hashSet.add(value.getStringValue())) {
                throw new AnalysisException("Duplicated value " + value.getStringValue());
            }
        }
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        try {
            List<Column> partitionColumns = this.findPartitionColumns(columns);
            ListPartitionInfo listPartitionInfo = new ListPartitionInfo(super.type, partitionColumns);
            for (SingleItemListPartitionDesc desc : this.singleListPartitionDescs) {
                long partitionId = partitionNameToId.get(desc.getPartitionName());
                listPartitionInfo.setDataProperty(partitionId, desc.getPartitionDataProperty());
                listPartitionInfo.setIsInMemory(partitionId, desc.isInMemory());
                listPartitionInfo.setTabletType(partitionId, desc.getTabletType());
                listPartitionInfo.setReplicationNum(partitionId, desc.getReplicationNum());
                listPartitionInfo.setValues(partitionId, desc.getValues());
                listPartitionInfo.setLiteralExprValues(partitionId, desc.getValues());
                listPartitionInfo.setIdToIsTempPartition(partitionId, isTemp);
                listPartitionInfo.setStorageCacheInfo(partitionId, desc.getDataCacheInfo());
            }
            for (MultiItemListPartitionDesc desc : this.multiListPartitionDescs) {
                long partitionId = partitionNameToId.get(desc.getPartitionName());
                listPartitionInfo.setDataProperty(partitionId, desc.getPartitionDataProperty());
                listPartitionInfo.setIsInMemory(partitionId, desc.isInMemory());
                listPartitionInfo.setTabletType(partitionId, desc.getTabletType());
                listPartitionInfo.setReplicationNum(partitionId, desc.getReplicationNum());
                listPartitionInfo.setMultiValues(partitionId, desc.getMultiValues());
                listPartitionInfo.setMultiLiteralExprValues(partitionId, desc.getMultiValues());
                listPartitionInfo.setIdToIsTempPartition(partitionId, isTemp);
                listPartitionInfo.setStorageCacheInfo(partitionId, desc.getDataCacheInfo());
            }
            listPartitionInfo.setAutomaticPartition(isAutoPartitionTable);
            return listPartitionInfo;
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private List<Column> findPartitionColumns(List<Column> columns) {
        List<Column> partitionColumns = Lists.newArrayList();
        for (String colName : this.partitionColNames) {
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    partitionColumns.add(column);
                    break;
                }
            }
        }
        return partitionColumns;
    }

    public boolean isAutoPartitionTable() {
        return isAutoPartitionTable;
    }

    public void setAutoPartitionTable(boolean autoPartitionTable) {
        isAutoPartitionTable = autoPartitionTable;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY LIST(");
        sb.append(this.partitionColNames.stream()
                .map(item -> "`" + item + "`")
                .collect(Collectors.joining(",")));
        sb.append(")(\n");
        if (!this.multiListPartitionDescs.isEmpty()) {
            String multiList = this.multiListPartitionDescs.stream()
                    .map(item -> "  " + item.toString())
                    .collect(Collectors.joining(",\n"));
            sb.append(multiList);
        }
        if (!this.singleListPartitionDescs.isEmpty()) {
            String sinleList = this.singleListPartitionDescs.stream()
                    .map(item -> "  " + item.toString())
                    .collect(Collectors.joining(",\n"));
            sb.append(sinleList);
        }
        sb.append("\n)");
        return sb.toString();
    }
}
