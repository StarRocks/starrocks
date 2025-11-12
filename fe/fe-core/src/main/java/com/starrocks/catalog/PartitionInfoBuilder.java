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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SinglePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.common.MetaUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Builder class to centralize the creation of PartitionInfo from various PartitionDesc types.
 */
public class PartitionInfoBuilder {

    /**
     * Build PartitionInfo from a PartitionDesc
     */
    public static PartitionInfo build(PartitionDesc partitionDesc, List<Column> schema,
                                      Map<String, Long> partitionNameToId, boolean isTemp) throws DdlException {
        if (partitionDesc instanceof RangePartitionDesc) {
            return buildRangePartitionInfo((RangePartitionDesc) partitionDesc, schema, partitionNameToId, isTemp);
        } else if (partitionDesc instanceof ListPartitionDesc) {
            return buildListPartitionInfo((ListPartitionDesc) partitionDesc, schema, partitionNameToId, isTemp);
        } else if (partitionDesc instanceof ExpressionPartitionDesc) {
            return buildExpressionPartitionInfo((ExpressionPartitionDesc) partitionDesc, schema, partitionNameToId, isTemp);
        } else {
            throw new DdlException("Unsupported partition type: " + partitionDesc.getClass().getSimpleName());
        }
    }

    /**
     * Build RangePartitionInfo from RangePartitionDesc
     */
    private static PartitionInfo buildRangePartitionInfo(RangePartitionDesc rangePartitionDesc,
                                                         List<Column> schema,
                                                         Map<String, Long> partitionNameToId,
                                                         boolean isTemp) throws DdlException {
        List<Column> partitionColumns = Lists.newArrayList();

        // check and get partition column
        for (String colName : rangePartitionDesc.getPartitionColNames()) {
            findRangePartitionColumn(schema, partitionColumns, colName);
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
        for (SingleRangePartitionDesc desc : rangePartitionDesc.getSingleRangePartitionDescs()) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            rangePartitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(schema), desc, partitionId, isTemp);
        }
        return rangePartitionInfo;
    }

    /**
     * Build ListPartitionInfo from ListPartitionDesc
     */
    private static PartitionInfo buildListPartitionInfo(ListPartitionDesc listPartitionDesc,
                                                        List<Column> schema,
                                                        Map<String, Long> partitionNameToId,
                                                        boolean isTemp) throws DdlException {
        try {
            List<Column> partitionColumns = findPartitionColumns(listPartitionDesc.getPartitionColNames(), schema);
            Map<ColumnId, Column> idToColumn = MetaUtils.buildIdToColumn(schema);
            ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);

            // Handle SingleItemListPartitionDesc
            for (SingleItemListPartitionDesc desc : listPartitionDesc.getSingleListPartitionDescs()) {
                long partitionId = partitionNameToId.get(desc.getPartitionName());
                setPartitionProperties(listPartitionInfo, desc, partitionId, isTemp);
                listPartitionInfo.setValues(partitionId, desc.getValues());
                listPartitionInfo.setLiteralExprValues(idToColumn, partitionId, desc.getValues());
            }

            // Handle MultiItemListPartitionDesc
            for (MultiItemListPartitionDesc desc : listPartitionDesc.getMultiListPartitionDescs()) {
                long partitionId = partitionNameToId.get(desc.getPartitionName());
                setPartitionProperties(listPartitionInfo, desc, partitionId, isTemp);
                listPartitionInfo.setMultiValues(partitionId, desc.getMultiValues());
                listPartitionInfo.setMultiLiteralExprValues(idToColumn, partitionId, desc.getMultiValues());
            }

            listPartitionInfo.setAutomaticPartition(listPartitionDesc.isAutoPartitionTable());
            return listPartitionInfo;
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    /**
     * Build ExpressionRangePartitionInfo from ExpressionPartitionDesc
     */
    private static PartitionInfo buildExpressionPartitionInfo(ExpressionPartitionDesc expressionPartitionDesc,
                                                              List<Column> schema,
                                                              Map<String, Long> partitionNameToId,
                                                              boolean isTemp) throws DdlException {
        // for materialized view express partition.
        if (expressionPartitionDesc.getRangePartitionDesc() == null) {
            return new ExpressionRangePartitionInfo(
                    Collections.singletonList(ColumnIdExpr.create(schema, expressionPartitionDesc.getExpr())),
                    schema,
                    PartitionType.RANGE);
        }

        RangePartitionDesc rangePartitionDesc = expressionPartitionDesc.getRangePartitionDesc();
        List<Column> partitionColumns = Lists.newArrayList();

        // check and get partition column
        for (String colName : rangePartitionDesc.getPartitionColNames()) {
            findRangePartitionColumn(schema, partitionColumns, colName);
        }

        // automatic partition / partition expr only support one partition column
        Column sourcePartitionColumn = partitionColumns.get(0);
        if (expressionPartitionDesc.getPartitionType() != null) {
            Column newTypePartitionColumn = new Column(sourcePartitionColumn);
            newTypePartitionColumn.setType(expressionPartitionDesc.getPartitionType());
            partitionColumns = Lists.newArrayList(newTypePartitionColumn);
        }

        for (Column column : partitionColumns) {
            try {
                RangePartitionInfo.checkExpressionRangeColumnType(column, expressionPartitionDesc.getExpr());
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // Recreate a partition column type bypass check
        RangePartitionInfo partitionInfo;
        if (rangePartitionDesc.isAutoPartitionTable()) {
            // for automatic partition table
            partitionInfo = new ExpressionRangePartitionInfo(
                    Collections.singletonList(ColumnIdExpr.create(schema, expressionPartitionDesc.getExpr())),
                    partitionColumns,
                    PartitionType.EXPR_RANGE);
        } else {
            // for partition by range expr
            ExpressionRangePartitionInfoV2 expressionRangePartitionInfoV2 =
                    new ExpressionRangePartitionInfoV2(
                            Collections.singletonList(ColumnIdExpr.create(schema, expressionPartitionDesc.getExpr())),
                            partitionColumns);
            expressionRangePartitionInfoV2.setSourcePartitionTypes(
                    Collections.singletonList(sourcePartitionColumn.getType()));
            partitionInfo = expressionRangePartitionInfoV2;
        }

        for (SingleRangePartitionDesc desc : rangePartitionDesc.getSingleRangePartitionDescs()) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(schema), desc, partitionId, isTemp);
        }

        return partitionInfo;
    }

    /**
     * Helper method to find partition columns from schema
     */
    private static List<Column> findPartitionColumns(List<String> partitionColNames, List<Column> schema) {
        List<Column> partitionColumns = Lists.newArrayList();
        for (String colName : partitionColNames) {
            for (Column column : schema) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    partitionColumns.add(column);
                    break;
                }
            }
        }
        return partitionColumns;
    }

    private static void findRangePartitionColumn(List<Column> schema, List<Column> partitionColumns, String colName)
            throws DdlException {
        boolean find = false;
        for (Column column : schema) {
            if (column.getName().equalsIgnoreCase(colName)) {
                if (!column.isKey() && column.getAggregationType() != AggregateType.NONE) {
                    throw new DdlException("The partition column could not be aggregated column"
                            + " and unique table's partition column must be key column");
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

    /**
     * Helper method to set common partition properties
     */
    private static void setPartitionProperties(ListPartitionInfo listPartitionInfo,
                                               SinglePartitionDesc desc,
                                               long partitionId,
                                               boolean isTemp) {
        listPartitionInfo.setDataProperty(partitionId, desc.getPartitionDataProperty());
        listPartitionInfo.setIsInMemory(partitionId, desc.isInMemory());
        listPartitionInfo.setTabletType(partitionId, desc.getTabletType());
        listPartitionInfo.setReplicationNum(partitionId, desc.getReplicationNum());
        listPartitionInfo.setIdToIsTempPartition(partitionId, isTemp);
        listPartitionInfo.setDataCacheInfo(partitionId, desc.getDataCacheInfo());
    }
}
