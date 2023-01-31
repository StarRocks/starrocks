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
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExpressionPartitionDesc extends PartitionDesc {

    private Expr expr;
    private RangePartitionDesc rangePartitionDesc = null;

    public ExpressionPartitionDesc(RangePartitionDesc rangePartitionDesc, Expr expr) {
        this.rangePartitionDesc = rangePartitionDesc;
        this.expr = expr;
    }

    public ExpressionPartitionDesc(Expr expr) {
        Preconditions.checkState(expr != null);
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

    public boolean isFunction() {
        return expr instanceof FunctionCallExpr;
    }

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        if (rangePartitionDesc != null) {
            rangePartitionDesc.analyze(columnDefs, otherProperties);
        }

        SlotRef slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);

        boolean hasExprAnalyze = false;
        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.getName().equalsIgnoreCase(slotRef.getColumnName()))  {
                PartitionExprAnalyzer.analyzePartitionExpr(this.expr, columnDef.getType());
                slotRef.setType(columnDef.getType());
                hasExprAnalyze = true;
            }
        }
        if (!hasExprAnalyze) {
            throw new AnalysisException("Partition expr without analyzed.");
        }
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId,
                                         boolean isTemp, boolean isExprPartition)
            throws DdlException {
        PartitionType partitionType = PartitionType.RANGE;
        if (isExprPartition)  {
            partitionType = PartitionType.EXPR_RANGE;
        }
        // we will support other PartitionInto in the future
        if (rangePartitionDesc == null) {
            // for materialized view express partition.
            return new ExpressionRangePartitionInfo(Collections.singletonList(expr), schema, partitionType);
        }
        List<Column> partitionColumns = Lists.newArrayList();

        // check and get partition column
        for (String colName : rangePartitionDesc.getPartitionColNames()) {
            findRangePartitionColumn(schema, partitionColumns, colName);
        }

        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                new ExpressionRangePartitionInfo(Collections.singletonList(expr), partitionColumns, partitionType);

        for (SingleRangePartitionDesc desc : getRangePartitionDesc().getSingleRangePartitionDescs()) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            expressionRangePartitionInfo.handleNewSinglePartitionDesc(desc, partitionId, isTemp);
        }

        return expressionRangePartitionInfo;
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

                try {
                    RangePartitionInfo.checkRangeColumnType(column);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
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

}
