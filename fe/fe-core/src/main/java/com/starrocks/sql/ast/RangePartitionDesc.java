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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.PartitionKeyDesc.PartitionRangeType;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;
import java.util.Set;

// to describe the key range partition's information in create table stmt
public class RangePartitionDesc extends PartitionDesc {

    private final List<String> partitionColNames;
    private final List<SingleRangePartitionDesc> singleRangePartitionDescs;
    private final List<MultiRangePartitionDesc> multiRangePartitionDescs;
    // for automatic partition table is ture. otherwise is false
    protected boolean isAutoPartitionTable = false;

    public RangePartitionDesc(List<String> partitionColNames, List<PartitionDesc> partitionDescs) {
        this(partitionColNames, partitionDescs, NodePosition.ZERO);
    }

    public RangePartitionDesc(List<String> partitionColNames, List<PartitionDesc> partitionDescs, NodePosition pos) {
        super(pos);
        type = PartitionType.RANGE;
        this.partitionColNames = partitionColNames;

        singleRangePartitionDescs = Lists.newArrayList();
        multiRangePartitionDescs = Lists.newArrayList();
        if (partitionDescs != null) {
            for (PartitionDesc partitionDesc : partitionDescs) {
                if (partitionDesc instanceof SingleRangePartitionDesc) {
                    singleRangePartitionDescs.add((SingleRangePartitionDesc) partitionDesc);
                } else if (partitionDesc instanceof MultiRangePartitionDesc) {
                    multiRangePartitionDescs.add((MultiRangePartitionDesc) partitionDesc);
                }
            }
        }
    }

    public List<SingleRangePartitionDesc> getSingleRangePartitionDescs() {
        return this.singleRangePartitionDescs;
    }

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        if (partitionColNames == null || partitionColNames.isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }

        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        ColumnDef firstPartitionColumn = null;
        for (String partitionCol : partitionColNames) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }

            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equalsIgnoreCase(partitionCol)) {
                    if (!columnDef.isKey() && columnDef.getAggregateType() != AggregateType.NONE) {
                        throw new AnalysisException("The partition column could not be aggregated column"
                                + " and unique table's partition column must be key column");
                    }
                    if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()) {
                        throw new AnalysisException(String.format("Invalid partition column '%s': %s",
                                columnDef.getName(), "invalid data type " + columnDef.getType()));
                    }
                    found = true;
                    firstPartitionColumn = columnDef;
                    break;
                }
            }

            if (!found) {
                throw new AnalysisException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }

        // use buildSinglePartitionDesc to build singleRangePartitionDescs
        if (multiRangePartitionDescs.size() != 0) {

            if (partitionColNames.size() > 1) {
                throw new AnalysisException("Batch build partition only support single range column.");
            }

            for (MultiRangePartitionDesc multiRangePartitionDesc : multiRangePartitionDescs) {
                TimestampArithmeticExpr.TimeUnit timeUnit = TimestampArithmeticExpr.TimeUnit
                        .fromName(multiRangePartitionDesc.getTimeUnit());
                if (timeUnit == TimestampArithmeticExpr.TimeUnit.HOUR && firstPartitionColumn.getType() != Type.DATETIME) {
                    throw new AnalysisException("Batch build partition for hour interval only supports " +
                            "partition column as DATETIME type");
                }
                PartitionConvertContext context = new PartitionConvertContext();
                context.setAutoPartitionTable(isAutoPartitionTable);
                context.setFirstPartitionColumnType(firstPartitionColumn.getType());
                context.setProperties(otherProperties);

                this.singleRangePartitionDescs.addAll(multiRangePartitionDesc.convertToSingle(context));
            }
        }

        Set<String> nameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        PartitionRangeType partitionType = PartitionRangeType.INVALID;
        for (SingleRangePartitionDesc desc : singleRangePartitionDescs) {
            if (!nameSet.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            // in create table stmt, we use given properties
            // copy one. because ProperAnalyzer will remove entry after analyze
            Map<String, String> givenProperties = null;
            if (otherProperties != null) {
                givenProperties = Maps.newHashMap(otherProperties);
            }
            // check partitionType
            if (partitionType == PartitionRangeType.INVALID) {
                partitionType = desc.getPartitionKeyDesc().getPartitionType();
            } else if (partitionType != desc.getPartitionKeyDesc().getPartitionType()) {
                throw new AnalysisException("You can only use one of these methods to create partitions");
            }
            desc.analyze(partitionColNames.size(), givenProperties);
        }
    }

    public void setAutoPartitionTable(boolean autoPartitionTable) {
        this.isAutoPartitionTable = autoPartitionTable;
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        List<Column> partitionColumns = Lists.newArrayList();

        // check and get partition column
        for (String colName : partitionColNames) {
            ExpressionPartitionDesc.findRangePartitionColumn(schema, partitionColumns, colName);
            for (Column column : partitionColumns) {
                try {
                    RangePartitionInfo.checkRangeColumnType(column);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
        }

        /*
         * validate key range
         * eg.
         * VALUE LESS THEN (10, 100, 1000)
         * VALUE LESS THEN (50, 500)
         * VALUE LESS THEN (80)
         *
         * key range is:
         * ( {MIN, MIN, MIN},     {10,  100, 1000} )
         * [ {10,  100, 1000},    {50,  500, MIN } )
         * [ {50,  500, MIN },    {80,  MIN, MIN } )
         */
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(partitionColumns);
        for (SingleRangePartitionDesc desc : singleRangePartitionDescs) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            rangePartitionInfo.handleNewSinglePartitionDesc(desc, partitionId, isTemp);
        }
        return rangePartitionInfo;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
        for (String column : partitionColNames) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column).append("`");
            idx++;
        }
        sb.append(")\n(\n");

        for (int i = 0; i < singleRangePartitionDescs.size(); i++) {
            if (i != 0) {
                sb.append(",\n");
            }
            sb.append(singleRangePartitionDescs.get(i).toString());
        }
        sb.append("\n)");
        return sb.toString();
    }
}
