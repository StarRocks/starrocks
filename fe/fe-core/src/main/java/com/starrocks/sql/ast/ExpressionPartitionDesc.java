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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
<<<<<<< HEAD
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
=======
>>>>>>> branch-2.5-mrs
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExpressionPartitionDesc extends PartitionDesc {

    private Expr expr;
<<<<<<< HEAD
    // If this value is not null, the type of the partition is different from the type of the partition field.
    private Type partitionType = null;
    // range partition desc == null means this must be materialized view
    private RangePartitionDesc rangePartitionDesc = null;

=======
    private RangePartitionDesc rangePartitionDesc = null;
    // No entry created in 2.5, just for compatibility
>>>>>>> branch-2.5-mrs
    public ExpressionPartitionDesc(RangePartitionDesc rangePartitionDesc, Expr expr) {
        this.rangePartitionDesc = rangePartitionDesc;
        this.expr = expr;
    }

    public ExpressionPartitionDesc(Expr expr) {
        this.expr = expr;
    }

    public RangePartitionDesc getRangePartitionDesc() {
        return rangePartitionDesc;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        Preconditions.checkState(expr != null);
        this.expr = expr;
    }

    public SlotRef getSlotRef() {
        if (expr instanceof FunctionCallExpr) {
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                if (child instanceof SlotRef) {
                    return (SlotRef) child;
                }
            }
        }
        return ((SlotRef) expr);
    }

    public RangePartitionDesc getRangePartitionDesc() {
        return rangePartitionDesc;
    }

    public boolean isFunction() {
        return expr instanceof FunctionCallExpr;
    }

    @Override
<<<<<<< HEAD
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        boolean hasExprAnalyze = false;
        SlotRef slotRef;
        if (rangePartitionDesc != null) {
            rangePartitionDesc.analyze(columnDefs, otherProperties);
            // for automatic partition table
            if (rangePartitionDesc.isAutoPartitionTable) {
                rangePartitionDesc.setAutoPartitionTable(true);
                slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
            } else {
                // for partition by range expr table
                // The type of the partition field may be different from the type after the expression
                if (expr instanceof CastExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromCast(expr);
                    partitionType = ((CastExpr) expr).getTargetTypeDef().getType();
                } else if (expr instanceof FunctionCallExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
                } else {
                    throw new AnalysisException("Unsupported expr:" + expr.toSql());
                }
            }
        } else {
            // for materialized view
            slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
        }

        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                slotRef.setType(columnDef.getType());
                PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
                hasExprAnalyze = true;
            }
        }
        if (!hasExprAnalyze) {
            throw new AnalysisException("Partition expr without analyzed.");
        }
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        // for materialized view express partition.
        if (rangePartitionDesc == null) {
            return new ExpressionRangePartitionInfo(Collections.singletonList(expr), schema, PartitionType.RANGE);
        }
        List<Column> partitionColumns = Lists.newArrayList();
        // check and get partition column
        for (String colName : rangePartitionDesc.getPartitionColNames()) {
            findRangePartitionColumn(schema, partitionColumns, colName);
        }
        // automatic partition / partition expr only support one partition column
        Column sourcePartitionColumn = partitionColumns.get(0);
        if (partitionType != null) {
            Column newTypePartitionColumn = new Column(sourcePartitionColumn);
            newTypePartitionColumn.setType(partitionType);
            partitionColumns = Lists.newArrayList(newTypePartitionColumn);
        }
        for (Column column : partitionColumns) {
            try {
                RangePartitionInfo.checkRangeColumnType(column);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // Recreate a partition column type bypass check
        RangePartitionInfo partitionInfo;
        if (rangePartitionDesc.isAutoPartitionTable) {
            // for automatic partition table
            partitionInfo = new ExpressionRangePartitionInfo(Collections.singletonList(expr), partitionColumns,
                    PartitionType.EXPR_RANGE);
        } else {
            // for partition by range expr
            ExpressionRangePartitionInfoV2 expressionRangePartitionInfoV2 =
                    new ExpressionRangePartitionInfoV2(Collections.singletonList(expr), partitionColumns);
            expressionRangePartitionInfoV2.setSourcePartitionTypes(Collections.singletonList(sourcePartitionColumn.getType()));
            partitionInfo = expressionRangePartitionInfoV2;
        }

        for (SingleRangePartitionDesc desc : getRangePartitionDesc().getSingleRangePartitionDescs()) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            partitionInfo.handleNewSinglePartitionDesc(desc, partitionId, isTemp);
        }

        return partitionInfo;
    }

    static void findRangePartitionColumn(List<Column> schema, List<Column> partitionColumns, String colName)
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
=======
    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId,
                                         boolean isTemp, boolean isExprPartition)
            throws DdlException {
        // we will support other PartitionInto in the future
        PartitionType partitionType = PartitionType.RANGE;
        if (isExprPartition)  {
            partitionType = PartitionType.EXPR_RANGE;
        }
        return new ExpressionRangePartitionInfo(Arrays.asList(expr), columns, partitionType);
>>>>>>> branch-2.5-mrs
    }

}
