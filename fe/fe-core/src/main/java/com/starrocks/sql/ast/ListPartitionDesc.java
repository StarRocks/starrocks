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
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
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
    private List<String> partitionNames = Lists.newArrayList();

    // for automatic partition table is ture. otherwise is false
    protected boolean isAutoPartitionTable = false;

    private final List<ParseNode> multiDescList;

    private List<Expr> partitionExprs = null;

    // for multi expr partition
    public ListPartitionDesc(List<ParseNode> multiDescList, NodePosition pos) {
        super(pos);
        this.multiDescList = multiDescList;
        this.singleListPartitionDescs = Lists.newArrayList();
        this.multiListPartitionDescs = Lists.newArrayList();
        this.partitionColNames = Lists.newArrayList();
    }

    public ListPartitionDesc(List<String> partitionColNames,
                             List<PartitionDesc> partitionDescs) {
        this(partitionColNames, partitionDescs, NodePosition.ZERO);
    }

    public ListPartitionDesc(List<String> partitionColNames,
                             List<PartitionDesc> partitionDescs, NodePosition pos) {
        super(pos);
        this.partitionColNames = partitionColNames;
        this.singleListPartitionDescs = Lists.newArrayList();
        this.multiListPartitionDescs = Lists.newArrayList();
        this.multiDescList = null;
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

    public List<ParseNode> getMultiDescList() {
        return multiDescList;
    }

    public List<PartitionDesc> getPartitionDescs() {
        List<PartitionDesc> partitionDescs = Lists.newArrayList();
        if (singleListPartitionDescs != null) {
            partitionDescs.addAll(singleListPartitionDescs);
        }
        if (multiListPartitionDescs != null) {
            partitionDescs.addAll(multiListPartitionDescs);
        }
        return partitionDescs;
    }

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public List<String> findAllPartitionNames() {
        List<String> partitionNames = new ArrayList<>();
        this.singleListPartitionDescs.forEach(desc -> partitionNames.add(desc.getPartitionName()));
        this.multiListPartitionDescs.forEach(desc -> partitionNames.add(desc.getPartitionName()));
        return partitionNames;
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
                    if (engineName.equalsIgnoreCase(Table.TableType.ICEBERG.name())) {
                        if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()
                                || columnDef.getType().isDecimalV2()) {
                            throw new SemanticException(String.format("Invalid iceberg partition column '%s': %s",
                                    columnDef.getName(), "invalid data type " + columnDef.getType()));
                        }
                    } else if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()
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

        if (engineName.equalsIgnoreCase("hive")) {
            checkHivePartitionColPos(columnDefs);
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

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public void setPartitionExprs(List<Expr> partitionExprs) {
        this.partitionExprs = partitionExprs;
    }

    public boolean isAutoPartitionTable() {
        return isAutoPartitionTable;
    }

    public void setAutoPartitionTable(boolean autoPartitionTable) {
        isAutoPartitionTable = autoPartitionTable;
    }

    public List<SingleItemListPartitionDesc> getSingleListPartitionDescs() {
        return singleListPartitionDescs;
    }

    public List<MultiItemListPartitionDesc> getMultiListPartitionDescs() {
        return multiListPartitionDescs;
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
